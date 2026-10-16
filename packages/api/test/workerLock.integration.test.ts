/**
 * The health worker's single-writer lock against a real Postgres (S8-3): one session holds it, a second waits on
 * standby and takes over when the first session ends, and a holder whose session is killed finds out within one check.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { holdWorkerLock, tryWorkerLock, waitForWorkerLock } from "../src/workerLock.ts";

let container: StartedPostgreSqlContainer;
const clients: pg.Client[] = [];

/** A connected client of its own, as each worker has; ended after the tests. */
async function session(): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: container.getConnectionUri() });
  client.on("error", () => undefined);
  await client.connect();
  clients.push(client);
  return client;
}

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
});

afterAll(async () => {
  await Promise.allSettled(clients.map((c) => c.end()));
  await container?.stop();
});

describe("the health worker lock", () => {
  it("is held by one session at a time, and passes to the standby when the holder's session ends", async () => {
    const first = await session();
    const second = await session();
    expect(await tryWorkerLock(first)).toBe(true);
    expect(await tryWorkerLock(second)).toBe(false);
    let standbyTries = 0;
    const takeover = waitForWorkerLock(second, 50, () => standbyTries++, () => false);
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(standbyTries).toBeGreaterThan(0);
    await first.end();
    expect(await takeover).toBe(true);
    expect(await tryWorkerLock(await session())).toBe(false);
  });

  it("tells the holder within one check when its session is killed, and not before", async () => {
    const holder = await session();
    const killer = await session();
    // The first test's standby still holds it; free it so this holder can take it.
    await killer.query("SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()");
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(await tryWorkerLock(holder)).toBe(true);
    const lost: unknown[] = [];
    const stop = holdWorkerLock(holder, 100, (reason) => lost.push(reason));
    try {
      await new Promise((resolve) => setTimeout(resolve, 350));
      expect(lost).toEqual([]);
      const { rows } = await holder.query<{ pid: number }>("SELECT pg_backend_pid() AS pid");
      await killer.query("SELECT pg_terminate_backend($1)", [rows[0]?.pid]);
      await new Promise((resolve) => setTimeout(resolve, 400));
      expect(lost).toHaveLength(1);
      expect(await tryWorkerLock(killer)).toBe(true);
    } finally {
      stop();
    }
  });
});
