/**
 * The SQL side of the gap checkpoint (S8-4): a {@link GapSource} over `readings`, and `gap_checkpoints` itself. Each
 * query must give exactly what the in-memory model in `test/support/gapModel.ts` gives on the same rows; the
 * integration test checks that.
 *
 * Every query orders a series by (seq, measured_at), the order the model uses, so a key reused with another timestamp
 * (stored twice since S8-2) sorts the same way everywhere.
 */
import type pg from "pg";
import { listPartitions } from "@fleet-ingest/db";
import { explainCheckpointed } from "./core/gapCheckpoint.ts";
import type { GapCheckpoint, GapSource, RecomputeReason } from "./core/gapCheckpoint.ts";
import { CURSOR_OVERLAP_MS } from "./core/gapWindow.ts";
import type { Gap } from "./core/health.ts";

/**
 * The most stored readings a pass checks one by one before recomputing instead. Each check costs about 0.33 ms (a
 * resumed pass took 15 ms, and 55 ms with 120 new readings), so 2,000 cost about 0.7 s, under the 1.1 s of a
 * from-scratch recompute of 30 days of one gateway (lab VM, S8-4). A normal pass checks about 120.
 */
export const CHECK_ROWS_MAX = 2000;

const GAP_COLUMNS = `d.device_id AS "deviceId", g.epoch::int, (g.prev_seq + 1)::int AS "fromSeq", (g.seq - 1)::int AS "toSeq",
  (g.seq - g.prev_seq - 1)::int AS missing, g.prev_at AS before`;

/**
 * The gateway's gaps through SQL.
 * - `allGaps`: every pair of consecutive readings in each series, all partitions, no time bound.
 * - `gapsFrom(T)`: the gaps whose reading-before was measured at or after T. Per series, it takes the lowest seq
 *   measured at or after T and reads the series from there by seq, through the primary key: every such reading-before
 *   is in that suffix, and so is its successor, whatever the clock did. Reading by time instead would miss a pair
 *   whose second reading was measured before T, after a clock stepped back.
 * - `changedSince`: the two invalidating changes, over readings stored since the checkpoint was verified, less the
 *   cursor overlap (a transaction committing late carries an older `stored_at`, as in S7-11). Re-checking a row seen
 *   before can only cause a needless recompute, never miss one. Each row checked costs a predecessor lookup in every
 *   partition (about 0.33 ms on the lab VM), so past {@link CHECK_ROWS_MAX} rows (a gateway replaying its buffer) it
 *   answers `backlog` and the pass recomputes instead, which is bounded by the history (S8-4).
 */
export function sqlGapSource(pool: pg.Pool, gatewayId: string): GapSource {
  return {
    allGaps: async () => (await pool.query<Gap>(
      `SELECT ${GAP_COLUMNS}
       FROM devices d
       CROSS JOIN LATERAL (
         SELECT r.epoch, r.seq, lag(r.seq) OVER w AS prev_seq, lag(r.measured_at) OVER w AS prev_at
         FROM readings r WHERE r.device_id = d.device_id
         WINDOW w AS (PARTITION BY r.epoch ORDER BY r.seq, r.measured_at)
       ) g
       WHERE d.gateway_id = $1 AND g.seq - g.prev_seq > 1`, [gatewayId])).rows,
    gapsFrom: async (from) => (await pool.query<Gap>(
      `SELECT ${GAP_COLUMNS}
       FROM devices d
       CROSS JOIN LATERAL (
         SELECT r.epoch, min(r.seq) AS from_seq FROM readings r
         WHERE r.device_id = d.device_id AND r.measured_at >= $2 GROUP BY r.epoch
       ) s
       CROSS JOIN LATERAL (
         SELECT s.epoch, r.seq, lag(r.seq) OVER w AS prev_seq, lag(r.measured_at) OVER w AS prev_at
         FROM readings r WHERE r.device_id = d.device_id AND r.epoch = s.epoch AND r.seq >= s.from_seq
         WINDOW w AS (ORDER BY r.seq, r.measured_at)
       ) g
       WHERE d.gateway_id = $1 AND g.seq - g.prev_seq > 1 AND g.prev_at >= $2`, [gatewayId, from])).rows,
    changedSince: async (checkpoint) => {
      const since = [gatewayId, checkpoint.verifiedStoredAt, CURSOR_OVERLAP_MS / 1000];
      const { rows: counted } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM (
           SELECT 1 FROM readings n JOIN devices d USING (device_id)
           WHERE d.gateway_id = $1 AND n.stored_at > $2::timestamptz - make_interval(secs => $3) LIMIT $4) c`, [...since, CHECK_ROWS_MAX + 1]);
      if ((counted[0]?.n ?? 0) > CHECK_ROWS_MAX) return "backlog";
      const { rows } = await pool.query<{ measured_behind: boolean | null; gap_behind: boolean | null }>(
        `SELECT bool_or(n.measured_at < $3) AS measured_behind, bool_or(p.measured_at < $3) AS gap_behind
         FROM readings n JOIN devices d USING (device_id)
         LEFT JOIN LATERAL (
           SELECT o.measured_at FROM readings o
           WHERE o.device_id = n.device_id AND o.epoch = n.epoch AND (o.seq, o.measured_at) < (n.seq, n.measured_at)
           ORDER BY o.seq DESC, o.measured_at DESC LIMIT 1
         ) p ON true
         WHERE d.gateway_id = $1 AND n.stored_at > $2::timestamptz - make_interval(secs => $4)`,
        [gatewayId, checkpoint.verifiedStoredAt, checkpoint.explainedThrough, CURSOR_OVERLAP_MS / 1000]);
      if (rows[0]?.measured_behind) return "measured_behind";
      if (rows[0]?.gap_behind) return "gap_behind";
      return null;
    },
  };
}

/** The gateway's checkpoint, or null. */
export async function loadGapCheckpoint(pool: pg.Pool, gatewayId: string): Promise<GapCheckpoint | null> {
  const { rows } = await pool.query<{ explained_through: Date; budget_left: string; stopped: boolean; dropped_telemetry: string; retained_from: Date | null; verified_stored_at: Date }>(
    "SELECT explained_through, budget_left, stopped, dropped_telemetry, retained_from, verified_stored_at FROM gap_checkpoints WHERE gateway_id = $1", [gatewayId]);
  const row = rows[0];
  if (!row) return null;
  return {
    explainedThrough: row.explained_through,
    state: { budget: Number(row.budget_left), stopped: row.stopped },
    droppedTelemetry: Number(row.dropped_telemetry),
    retainedFrom: row.retained_from,
    verifiedStoredAt: row.verified_stored_at,
  };
}

/** Stores the gateway's checkpoint, replacing the previous one. */
export async function saveGapCheckpoint(pool: pg.Pool, gatewayId: string, checkpoint: GapCheckpoint): Promise<void> {
  await pool.query(
    `INSERT INTO gap_checkpoints (gateway_id, explained_through, budget_left, stopped, dropped_telemetry, retained_from, verified_stored_at, computed_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, now())
     ON CONFLICT (gateway_id) DO UPDATE SET explained_through = excluded.explained_through, budget_left = excluded.budget_left,
       stopped = excluded.stopped, dropped_telemetry = excluded.dropped_telemetry, retained_from = excluded.retained_from,
       verified_stored_at = excluded.verified_stored_at, computed_at = excluded.computed_at`,
    [gatewayId, checkpoint.explainedThrough, checkpoint.state.budget, checkpoint.state.stopped, checkpoint.droppedTelemetry, checkpoint.retainedFrom, checkpoint.verifiedStoredAt]);
}

/** What a gaps pass knows once for all gateways: the database's time at its start, and where stored history begins. */
export interface CheckpointPass {
  passStartedAt: Date;
  retainedFrom: Date | null;
}

/** How long a checkpoint may go unverified before it's recomputed instead of checked (see `staleBefore`). */
export const CHECKPOINT_STALE_MS = 15 * 60_000;

/**
 * A gateway's gaps whose `before` falls in [from, to], explained against its whole stored history through its
 * checkpoint, which this also advances to `from` and saves. Same result as {@link gapsInWindow} on the same history
 * (the integration test compares them), at the cost of the last hour instead of the whole history.
 */
export async function checkpointedGapsInWindow(pool: pg.Pool, gatewayId: string, droppedTelemetry: number, from: Date, to: Date, pass: CheckpointPass): Promise<{ gaps: Array<Gap & { explained: boolean }>; recomputed: RecomputeReason | null }> {
  const result = await explainCheckpointed(sqlGapSource(pool, gatewayId), await loadGapCheckpoint(pool, gatewayId),
    { droppedTelemetry, retainedFrom: pass.retainedFrom, passStartedAt: pass.passStartedAt, staleBefore: new Date(pass.passStartedAt.getTime() - CHECKPOINT_STALE_MS) }, from);
  await saveGapCheckpoint(pool, gatewayId, result.checkpoint);
  return { gaps: result.gaps.filter((g) => g.before <= to), recomputed: result.recomputed };
}

/** The start of the oldest kept partition of `readings`: where "all stored history" begins now. */
export async function oldestRetained(pool: pg.Pool): Promise<Date | null> {
  const client = await pool.connect();
  try {
    const partitions = await listPartitions(client);
    return partitions.length ? new Date(Math.min(...partitions.map((p) => p.from.getTime()))) : null;
  } finally {
    client.release();
  }
}
