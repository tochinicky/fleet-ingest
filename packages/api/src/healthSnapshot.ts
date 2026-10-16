/**
 * The health worker's snapshot (S8-3): the fleet's open gaps as of its last gaps pass, in one row of
 * `health_snapshot`. The worker writes it; every API replica reads it for `/fleet`, so they all report the same
 * figures, and none of them keeps a gap window of its own.
 *
 * **Fenced:** the write succeeds only if the session running it holds the health worker lock *at that moment*, checked
 * in the same statement. A lock alone doesn't make a single writer: a leader that lost its session (a network cut,
 * a pause) can still be mid-pass while a new leader holds the lock, and its stale result would overwrite the new one.
 * With the check in the write, a zombie's write affects no row, whichever connection it goes through. The worker
 * writes on its lock connection, so the check holds for exactly as long as it is leader.
 */
import type pg from "pg";
import type { FleetGaps } from "./healthJob.ts";
import { HEALTH_WORKER_LOCK_KEY } from "./workerLock.ts";

/** The open gaps as `/fleet` reports them: the last gaps pass, with how old it is. */
export type FleetGapsReport = FleetGaps & { ageSeconds: number | null };

/** What a write did: replaced the row, refused because this session doesn't hold the lock, or skipped an incomplete pass. */
export type SnapshotWrite = "written" | "fenced" | "incomplete";

/**
 * Replaces the snapshot with `gaps`, written by `writer`, if `client`'s session holds the health worker lock. An
 * incomplete pass is never written.
 */
export async function writeHealthSnapshot(client: pg.ClientBase | pg.Pool, gaps: FleetGaps, writer: string): Promise<SnapshotWrite> {
  if (!gaps.complete) return "incomplete";
  const { rowCount } = await client.query(
    `INSERT INTO health_snapshot (id, as_of, open_gaps, writer, written_at)
     SELECT true, $1, $2, $3, now()
     WHERE EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()
                     AND classid = 0 AND objid = $4 AND objsubid = 1 AND granted)
     ON CONFLICT (id) DO UPDATE SET as_of = excluded.as_of, open_gaps = excluded.open_gaps, writer = excluded.writer, written_at = excluded.written_at`,
    [gaps.asOf, JSON.stringify(gaps), writer, HEALTH_WORKER_LOCK_KEY],
  );
  return rowCount === 1 ? "written" : "fenced";
}

/**
 * The snapshot as of `now`, or "not complete" if the worker hasn't finished a gaps pass yet. A stale snapshot is
 * still returned, with its age: the figures are what they are, and HealthJobStale and HealthWorkerNotRunning say why.
 */
export async function readHealthSnapshot(pool: pg.Pool, now: Date): Promise<FleetGapsReport> {
  const { rows } = await pool.query<{ open_gaps: FleetGaps; as_of: Date }>("SELECT open_gaps, as_of FROM health_snapshot WHERE id");
  const row = rows[0];
  if (!row) return { complete: false, asOf: null, ageSeconds: null };
  return { ...row.open_gaps, ageSeconds: Math.max(0, Math.round((now.getTime() - row.as_of.getTime()) / 1000)) };
}
