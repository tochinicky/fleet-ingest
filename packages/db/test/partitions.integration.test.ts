/**
 * Partitioning `readings` against a real Postgres 17 (S8-2):
 * - migration 007 converts a table that already holds rows, keeps every row, and gives each day its partition;
 * - the new key is (device_id, epoch, seq, measured_at): a redelivery still conflicts, a reused key with another
 *   timestamp no longer does;
 * - maintenance creates the days ahead and drops the days retention no longer keeps, and is idempotent;
 * - the per-device covering index exists on every partition, so the gap reads stay index-only.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { applyMigrations, listPartitions, loadMigrations, maintainPartitions, MIGRATIONS_DIR, partitionName, startOfUtcDay } from "../src/index.ts";

let container: StartedPostgreSqlContainer;
let client: pg.Client;

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
  client = new pg.Client({ connectionString: container.getConnectionUri() });
  await client.connect();
  const all = loadMigrations(MIGRATIONS_DIR);
  await applyMigrations(client, all.filter((m) => m.version < 7));
  await client.query("INSERT INTO gateways (gateway_id, first_seen) VALUES ('gw-1', now())");
  await client.query("INSERT INTO devices (device_id, gateway_id, kind, first_seen) VALUES ('dev-1', 'gw-1', 'Battery', now())");
  await client.query(`
    INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind)
    SELECT 'dev-1', 1, s, timestamptz '2026-10-10 23:00:00Z' + s * interval '9 minutes', timestamptz '2026-10-10 23:00:01Z' + s * interval '9 minutes', 'Battery'
    FROM generate_series(1, 300) AS s`);
  await applyMigrations(client, all);
});

afterAll(async () => {
  await client?.end();
  await container?.stop();
});

describe("migration 007", () => {
  it("keeps every existing row and gives each of their days a partition", async () => {
    const { rows } = await client.query<{ n: number; days: number }>("SELECT count(*)::int AS n, count(DISTINCT tableoid)::int AS days FROM readings");
    expect(rows[0]).toEqual({ n: 300, days: 3 });
    const names = (await listPartitions(client)).map((p) => p.name);
    expect(names).toEqual(expect.arrayContaining(["readings_p20261010", "readings_p20261011", "readings_p20261012"]));
    const { rows: parent } = await client.query<{ kind: string }>("SELECT relkind AS kind FROM pg_class WHERE relname = 'readings'");
    expect(parent[0]?.kind).toBe("p");
  });

  it("the key is (device_id, epoch, seq, measured_at): a redelivery conflicts, a reused key with another timestamp doesn't", async () => {
    const insert = (measuredAt: string) => client.query(
      "INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind) VALUES ('dev-1', 1, 5, $1, now(), 'Battery') ON CONFLICT (device_id, epoch, seq, measured_at) DO NOTHING", [measuredAt]);
    expect((await insert("2026-10-10T23:45:00Z")).rowCount).toBe(0);
    expect((await insert("2026-10-10T23:45:01Z")).rowCount).toBe(1);
  });

  it("every partition has the per-device covering index, so per-device reads stay index-only", async () => {
    const { rows } = await client.query<{ n: number }>(`
      SELECT count(*)::int AS n FROM pg_inherits i JOIN pg_class p ON p.oid = i.inhparent JOIN pg_index x ON x.indrelid = i.inhrelid
      JOIN pg_class ix ON ix.oid = x.indexrelid
      WHERE p.relname = 'readings' AND pg_get_indexdef(ix.oid) LIKE '%(device_id, measured_at) INCLUDE (epoch, seq)%'`);
    expect(rows[0]?.n).toBe((await listPartitions(client)).length);
  });
});

describe("maintainPartitions", () => {
  it("creates the kept window, drops what's older, keeps the rows of kept days, and then does nothing", async () => {
    const now = new Date("2026-10-16T12:00:00Z");
    const first = await maintainPartitions(client, { retentionDays: 5, aheadDays: 3 }, now);
    expect(first.dropped).toEqual(expect.arrayContaining(["readings_p20261010"]));
    expect(first.dropped).not.toContain("readings_p20261011");
    const names = (await listPartitions(client)).map((p) => p.name).sort();
    expect(names[0]).toBe("readings_p20261011");
    expect(names).toContain(partitionName(new Date(startOfUtcDay(now).getTime() + 3 * 86_400_000)));
    const { rows } = await client.query<{ n: number }>("SELECT count(*)::int AS n FROM readings WHERE measured_at < '2026-10-11T00:00:00Z'");
    expect(rows[0]?.n).toBe(0);
    expect(await maintainPartitions(client, { retentionDays: 5, aheadDays: 3 }, now)).toEqual({ created: [], dropped: [] });
  });

  it("refuses a retention under the floor", async () => {
    await expect(maintainPartitions(client, { retentionDays: 3, aheadDays: 3 }, new Date())).rejects.toThrow(/at least 4/);
  });
});
