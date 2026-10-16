/**
 * Integration tests for the migration runner against a real Postgres 17 container.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "../src/index.ts";

let container: StartedPostgreSqlContainer;

/** Opens a new connection to the test database; the caller ends it. */
async function connect(): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: container.getConnectionUri() });
  await client.connect();
  return client;
}

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
});

afterAll(async () => {
  await container?.stop();
});

describe("applyMigrations", () => {
  it("applies the repo's migrations once, then is a no-op, even with two runners at once", async () => {
    const migrations = loadMigrations(MIGRATIONS_DIR);
    const [a, b] = [await connect(), await connect()];
    const results = await Promise.all([applyMigrations(a, migrations), applyMigrations(b, migrations)]);
    expect(results.flat()).toEqual(["001_initial_schema.sql", "002_status_received_at.sql", "003_offline_received_at.sql", "004_slo_columns.sql", "005_desired_mode.sql", "006_readings_fleet_reads.sql", "007_readings_partitioned.sql", "008_health_snapshot.sql", "009_gap_checkpoints.sql"]);
    expect(await applyMigrations(a, migrations)).toEqual([]);
    const { rows } = await a.query("SELECT to_regclass('public.readings') AS readings, to_regclass('public.dead_letters') AS dead_letters");
    expect(rows[0]).toEqual({ readings: "readings", dead_letters: "dead_letters" });
    await Promise.all([a.end(), b.end()]);
  });

  it("rolls a failing migration back completely and doesn't record it", async () => {
    const client = await connect();
    const migrations = [
      ...loadMigrations(MIGRATIONS_DIR),
      { version: 900, name: "900_broken.sql", sql: "CREATE TABLE half_done (id int); SELECT 1/0;" },
    ];
    await expect(applyMigrations(client, migrations)).rejects.toThrow(/division by zero/);
    const { rows } = await client.query("SELECT to_regclass('public.half_done') AS t, (SELECT count(*) FROM schema_migrations WHERE version = 900)::int AS recorded");
    expect(rows[0]).toEqual({ t: null, recorded: 0 });
    await client.end();
  });
});
