/**
 * The health worker's single-writer lock (S8-3): a Postgres session advisory lock, held on a connection of its own for
 * as long as the worker runs its job. Two workers running the job at once would export every fleet counter twice, and
 * the SLO is a sum across scrape targets, so the second worker waits on standby instead. The lock is the session's:
 * if the connection ends, the lock is released, so a worker that loses its lock connection must stop at once
 * ({@link holdWorkerLock} reports that, and the composition root exits).
 */
import { setTimeout as sleep } from "node:timers/promises";
import type pg from "pg";

/** The advisory lock key: an arbitrary constant every health worker agrees on ("heal" in ASCII). */
export const HEALTH_WORKER_LOCK_KEY = 0x6865616c;

/** True if this session took the lock now (or already held it). Never waits. */
export async function tryWorkerLock(client: pg.ClientBase): Promise<boolean> {
  const { rows } = await client.query<{ locked: boolean }>("SELECT pg_try_advisory_lock($1) AS locked", [HEALTH_WORKER_LOCK_KEY]);
  return rows[0]?.locked === true;
}

/**
 * Waits until `client`'s session holds the lock, trying every `retryMs`; `onStandby` is called on each failed try.
 * Returns false if `stopped()` turned true first.
 */
export async function waitForWorkerLock(client: pg.ClientBase, retryMs: number, onStandby: () => void, stopped: () => boolean): Promise<boolean> {
  while (!stopped()) {
    if (await tryWorkerLock(client)) return true;
    onStandby();
    await sleep(retryMs);
  }
  return false;
}

/**
 * Keeps checking, every `everyMs`, that the lock connection still answers and still holds the lock; calls `onLost`
 * once if it doesn't. A connection that dies silently (a network cut) shows up here as a query timeout, not as an
 * `error` event, so this check is what bounds how long a worker can run without the lock. Returns a stop function.
 */
export function holdWorkerLock(client: pg.ClientBase, everyMs: number, onLost: (reason: unknown) => void): () => void {
  let stopped = false;
  const check = async (): Promise<void> => {
    try {
      const { rows } = await client.query<{ held: boolean }>(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND classid = 0 AND objid = $1 AND objsubid = 1 AND granted) AS held",
        [HEALTH_WORKER_LOCK_KEY]);
      if (!rows[0]?.held) throw new Error("the health worker lock is no longer held");
    } catch (error) {
      if (!stopped) {
        stopped = true;
        clearInterval(timer);
        onLost(error);
      }
    }
  };
  const timer = setInterval(() => void check(), everyMs);
  return () => {
    stopped = true;
    clearInterval(timer);
  };
}
