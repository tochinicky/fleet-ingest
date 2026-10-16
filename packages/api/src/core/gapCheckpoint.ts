/**
 * Gap explanation with a per-gateway checkpoint (S7-11 option A, decided in S8-4). `droppedTelemetry` is a lifetime
 * counter, so explaining any window means explaining every gap the gateway ever had, oldest first ({@link explainGaps}).
 * Without a checkpoint that reads the gateway's whole history every gaps pass. With one, a pass reads only the gaps
 * from `explainedThrough` on, and resumes the explanation from the stored state.
 *
 * **Exact, not approximate.** The checkpoint is the explanation state after every gap whose `before` is earlier than
 * `explainedThrough` (T). It stays valid only while that set of gaps can't have changed; anything that could change it
 * makes the gateway recompute from scratch. What can change it, and how each is noticed:
 * - `droppedTelemetry` moved: a bigger counter can explain more, or stop later (compared with the stored value);
 * - retention dropped a partition, so "all stored history" lost its oldest gaps (the oldest partition moved);
 * - a reading was stored that was **measured before T** (`measured_behind`): it can start a gap before T;
 * - a reading was stored whose **predecessor** in its series, by (seq, measured_at), **was measured before T**
 *   (`gap_behind`). That covers every change to a folded gap: a gap is folded only if the reading before it was
 *   measured before T, so anything landing inside it, of the new readings the lowest, has that reading as its
 *   predecessor. It catches a replay filling a gap that straddles T even when the replay was measured after T, a key
 *   reused with another timestamp, a clock that stepped back, and a device back after a silence longer than the
 *   window. Both are judged on what's stored when the pass checks, not on arrival order, which `stored_at` (a
 *   transaction's start) can't give exactly.
 * Every other new reading makes its gap, if any, with `before` ≥ T: the pass reads those. S8-4 records why the rule
 * first considered (a fill measured before T) was not enough on its own. The oracle property test checks the result
 * equals explaining all history, for any history.
 *
 * Pure: the database is behind {@link GapSource}, so the same code runs on SQL and on the test's in-memory model.
 */
import { explainFrom, initialExplainState } from "./health.ts";
import type { ExplainState, Gap } from "./health.ts";

/** One gateway's checkpoint. */
export interface GapCheckpoint {
  /** T: every gap with `before` < T is folded into `state`. */
  explainedThrough: Date;
  /** The explanation state after those gaps. */
  state: ExplainState;
  /** The counter the state was computed with. */
  droppedTelemetry: number;
  /** The oldest kept partition's start when it was computed: the start of "all stored history". */
  retainedFrom: Date | null;
  /** The database time up to which stored readings were checked against it (rows stored later are checked next). */
  verifiedStoredAt: Date;
}

/** Why a pass recomputed from scratch instead of resuming. */
export type RecomputeReason = "no_checkpoint" | "dropped_changed" | "retention" | "measured_behind" | "gap_behind" | "window_behind" | "stale" | "backlog";

/** What a checkpointed pass needs from storage, for one gateway. */
export interface GapSource {
  /** Every gap in the gateway's stored history. */
  allGaps(): Promise<Gap[]>;
  /** The gaps whose `before` is at or after `from`, exactly as {@link allGaps} would have them. */
  gapsFrom(from: Date): Promise<Gap[]>;
  /**
   * Whether a reading stored since the checkpoint was verified invalidates it (see the module comment), and how; or
   * `backlog` when so many were stored that checking each would cost more than recomputing.
   */
  changedSince(checkpoint: GapCheckpoint): Promise<"measured_behind" | "gap_behind" | "backlog" | null>;
}

/** The facts a pass checks a checkpoint against. */
export interface CheckpointContext {
  droppedTelemetry: number;
  retainedFrom: Date | null;
  /** The database's time when this pass started, before it read anything: the next checkpoint's verification point. */
  passStartedAt: Date;
  /**
   * A checkpoint verified before this is recomputed rather than checked: checking it would read every reading the
   * gateway stored since, and a recompute is bounded by the history instead (a gateway whose gaps closed, and that
   * no pass looked at for a while).
   */
  staleBefore: Date;
}

/** What one pass produced: the explained gaps with `before` ≥ `windowFrom`, the next checkpoint, and why it recomputed. */
export interface CheckpointedGaps {
  gaps: Array<Gap & { explained: boolean }>;
  checkpoint: GapCheckpoint;
  recomputed: RecomputeReason | null;
}

const sameTime = (a: Date | null, b: Date | null): boolean => (a?.getTime() ?? null) === (b?.getTime() ?? null);

/** Why `checkpoint` can't be resumed by a pass whose window starts at `windowFrom`, or null if it can. */
async function invalidation(source: GapSource, checkpoint: GapCheckpoint | null, context: CheckpointContext, windowFrom: Date): Promise<RecomputeReason | null> {
  if (!checkpoint) return "no_checkpoint";
  if (checkpoint.droppedTelemetry !== context.droppedTelemetry) return "dropped_changed";
  if (!sameTime(checkpoint.retainedFrom, context.retainedFrom)) return "retention";
  if (windowFrom < checkpoint.explainedThrough) return "window_behind";
  if (checkpoint.verifiedStoredAt < context.staleBefore) return "stale";
  return source.changedSince(checkpoint);
}

/**
 * Explains a gateway's gaps from `windowFrom` on, resuming from `checkpoint` when it's still valid, and returns the
 * next checkpoint, at T = `windowFrom`. The result equals `explainGaps(allGaps, droppedTelemetry)` restricted to
 * gaps with `before` ≥ `windowFrom`, for any history (the oracle property test).
 */
export async function explainCheckpointed(source: GapSource, checkpoint: GapCheckpoint | null, context: CheckpointContext, windowFrom: Date): Promise<CheckpointedGaps> {
  const reason = await invalidation(source, checkpoint, context, windowFrom);
  const start = reason === null && checkpoint ? { state: checkpoint.state, gaps: await source.gapsFrom(checkpoint.explainedThrough) }
    : { state: initialExplainState(context.droppedTelemetry), gaps: await source.allGaps() };
  const { explained } = explainFrom(start.state, start.gaps);
  const folded = explainFrom(start.state, start.gaps.filter((g) => g.before < windowFrom)).state;
  return {
    gaps: explained.filter((g) => g.before >= windowFrom),
    checkpoint: { explainedThrough: windowFrom, state: folded, droppedTelemetry: context.droppedTelemetry, retainedFrom: context.retainedFrom, verifiedStoredAt: context.passStartedAt },
    recomputed: reason,
  };
}
