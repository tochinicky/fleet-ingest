/**
 * Fencing the health worker's snapshot (S8-3), against a real Postgres: a lock doesn't make a single writer on its
 * own. Here the old leader is paused mid-pass, its session is lost and a new leader takes the lock and writes; when
 * the old one resumes with its stale result, its write must change nothing, whichever connection it uses.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";
import type { FleetGaps } from "../src/healthJob.ts";
import { readHealthSnapshot, writeHealthSnapshot } from "../src/healthSnapshot.ts";
import { tryWorkerLock } from "../src/workerLock.ts";

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
const clients: pg.Client[] = [];

/** A connected client of its own, as each worker's lock connection is. */
async function session(): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: container.getConnectionUri() });
  client.on("error", () => undefined);
  await client.connect();
  clients.push(client);
  return client;
}

/** A complete gaps pass as of `asOf`, with `gaps` gaps. */
const pass = (asOf: string, gaps: number): FleetGaps => ({ complete: true, asOf, gaps, missingReadings: gaps, explainedByDroppedTelemetry: 0 });

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  const client = await pool.connect();
  await applyMigrations(client, loadMigrations(MIGRATIONS_DIR));
  client.release();
});

afterAll(async () => {
  await Promise.allSettled(clients.map((c) => c.end()));
  await pool?.end();
  await container?.stop();
});

describe("the health snapshot write is fenced by the worker lock", () => {
  it("a zombie leader that resumes after a takeover can't overwrite the new leader's snapshot", async () => {
    const oldLeader = await session();
    expect(await tryWorkerLock(oldLeader)).toBe(true);
    expect(await writeHealthSnapshot(oldLeader, pass("2026-10-16T10:00:00.000Z", 1), "old")).toBe("written");
    // The old leader is paused mid-pass (it has computed a result and not yet written it), and meanwhile loses its
    // session: Postgres releases the lock, and the standby takes it and writes a newer pass.
    const stale = pass("2026-10-16T10:01:00.000Z", 7);
    const killer = await session();
    await killer.query("SELECT pg_terminate_backend($1)", [(await oldLeader.query<{ pid: number }>("SELECT pg_backend_pid() AS pid")).rows[0]?.pid]);
    const newLeader = await session();
    expect(await tryWorkerLock(newLeader)).toBe(true);
    expect(await writeHealthSnapshot(newLeader, pass("2026-10-16T10:02:00.000Z", 2), "new")).toBe("written");

    // It resumes. On its own (dead) lock connection the write fails outright; through any other connection, a pool
    // say, it's refused by the lock check in the statement.
    await expect(writeHealthSnapshot(oldLeader, stale, "old")).rejects.toThrow();
    expect(await writeHealthSnapshot(pool, stale, "old")).toBe("fenced");
    const zombie = await session();
    expect(await writeHealthSnapshot(zombie, stale, "old")).toBe("fenced");

    const now = new Date("2026-10-16T10:02:30.000Z");
    expect(await readHealthSnapshot(pool, now)).toMatchObject({ asOf: "2026-10-16T10:02:00.000Z", gaps: 2, ageSeconds: 30 });
    expect((await pool.query("SELECT writer FROM health_snapshot")).rows[0]?.writer).toBe("new");
  });
});
