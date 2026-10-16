/**
 * An in-memory model of `readings` for one gateway, and the gap rules on it, for the checkpoint's oracle test (S8-4).
 * It defines what the SQL adapter must return: the integration test checks the SQL against these same functions.
 */
import fc from "fast-check";
import type { GapCheckpoint, GapSource } from "../../src/core/gapCheckpoint.ts";
import type { Gap } from "../../src/core/health.ts";

/** One stored reading. `storedAt` is the model's storage clock: later rows were stored later. */
export interface ModelRow {
  deviceId: string;
  epoch: number;
  seq: number;
  measuredAt: number;
  storedAt: number;
}

const seriesKey = (r: Pick<ModelRow, "deviceId" | "epoch">): string => `${r.deviceId}\u0000${r.epoch}`;

/** Order within a series: by seq, then by measured_at (a key reused with another timestamp sorts by its time). */
export function compareInSeries(a: Pick<ModelRow, "seq" | "measuredAt">, b: Pick<ModelRow, "seq" | "measuredAt">): number {
  return a.seq - b.seq || a.measuredAt - b.measuredAt;
}

/** Every gap among `rows`: consecutive readings of a series, in series order, more than one seq apart. */
export function seriesGaps(rows: ModelRow[]): Gap[] {
  const bySeries = new Map<string, ModelRow[]>();
  for (const r of rows) bySeries.set(seriesKey(r), [...(bySeries.get(seriesKey(r)) ?? []), r]);
  const out: Gap[] = [];
  for (const series of bySeries.values()) {
    const sorted = [...series].sort(compareInSeries);
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1] as ModelRow;
      const cur = sorted[i] as ModelRow;
      if (cur.seq - prev.seq > 1) {
        out.push({ deviceId: cur.deviceId, epoch: cur.epoch, fromSeq: prev.seq + 1, toSeq: cur.seq - 1, missing: cur.seq - prev.seq - 1, before: new Date(prev.measuredAt) });
      }
    }
  }
  return out;
}

/** The invalidating changes among rows stored after `verifiedStoredAt`, as the module comment of gapCheckpoint.ts defines them. */
export function modelChange(rows: ModelRow[], verifiedStoredAt: number, explainedThrough: number): "measured_behind" | "gap_behind" | null {
  let gapBehind = false;
  for (const n of rows.filter((r) => r.storedAt > verifiedStoredAt)) {
    if (n.measuredAt < explainedThrough) return "measured_behind";
    const predecessor = rows.filter((o) => o !== n && seriesKey(o) === seriesKey(n) && compareInSeries(o, n) < 0).sort(compareInSeries).at(-1);
    if (predecessor && predecessor.measuredAt < explainedThrough) gapBehind = true;
  }
  return gapBehind ? "gap_behind" : null;
}

/** A {@link GapSource} over the model. */
export function modelSource(rows: ModelRow[]): GapSource {
  return {
    allGaps: async () => seriesGaps(rows),
    gapsFrom: async (from) => seriesGaps(rows).filter((g) => g.before >= from),
    changedSince: async (checkpoint: GapCheckpoint) => modelChange(rows, checkpoint.verifiedStoredAt.getTime(), checkpoint.explainedThrough.getTime()),
  };
}

export const MINUTE = 60_000;
/** The gaps pass's window: the checkpoint advances to its start. */
export const WINDOW = 60 * MINUTE;

/** One step of a generated history. */
export type Op =
  | { kind: "store"; device: number; epoch: number; seq: number; ageMinutes: number }
  | { kind: "next"; device: number; epoch: number; skip: number; back: number; ageMinutes: number }
  | { kind: "drop"; more: number }
  | { kind: "retention"; keepMinutes: number }
  | { kind: "pass"; advanceMinutes: number };

// Gaps the size of the drop counter, so the budget runs out somewhere in the middle of a history and changes before
// the checkpoint move where it does: a readings generator with random seqs made gaps so large that the explanation
// stopped at the first one, and the test passed even with every invalidation rule removed.
/** Mostly recent, as live readings are; sometimes far behind the window, as replays are; sometimes in the future. */
const age = fc.oneof({ weight: 4, arbitrary: fc.integer({ min: -2, max: 20 }) }, { weight: 1, arbitrary: fc.integer({ min: 0, max: 150 }) });

export const op: fc.Arbitrary<Op> = fc.oneof(
  { weight: 10, arbitrary: fc.record({ kind: fc.constant("next" as const), device: fc.integer({ min: 0, max: 1 }), epoch: fc.oneof({ weight: 4, arbitrary: fc.constant(1) }, { weight: 1, arbitrary: fc.constant(2) }), skip: fc.integer({ min: 0, max: 3 }), back: fc.oneof({ weight: 3, arbitrary: fc.constant(0) }, { weight: 1, arbitrary: fc.integer({ min: 1, max: 5 }) }), ageMinutes: age }) },
  { weight: 1, arbitrary: fc.record({ kind: fc.constant("store" as const), device: fc.integer({ min: 0, max: 2 }), epoch: fc.integer({ min: 1, max: 2 }), seq: fc.integer({ min: 1, max: 30 }), ageMinutes: fc.integer({ min: -2, max: 150 }) }) },
  { weight: 2, arbitrary: fc.record({ kind: fc.constant("drop" as const), more: fc.integer({ min: 1, max: 8 }) }) },
  { weight: 1, arbitrary: fc.record({ kind: fc.constant("retention" as const), keepMinutes: fc.integer({ min: 30, max: 200 }) }) },
  { weight: 3, arbitrary: fc.record({ kind: fc.constant("pass" as const), advanceMinutes: fc.integer({ min: 0, max: 40 }) }) },
);


const store = (device: number, seq: number, ageMinutes: number, epoch = 1): Op => ({ kind: "store", device, epoch, seq, ageMinutes });
const pass = (advanceMinutes = 0): Op => ({ kind: "pass", advanceMinutes });

/**
 * Histories where each invalidation rule changes the result, not just the reason: generated histories rarely hit
 * some of them, so both the unit and the Postgres tests replay these too. Without the named rule, the checkpointed
 * answer differs from explaining all history.
 */
export const FIXED_HISTORIES: Record<string, Op[]> = {
  // gap_behind: a replay fills the gap 1..5 that straddles T, measured after T. Its new gaps free one drop each, and
  // the window's gap 5..7 becomes explained.
  straddlingFill: [store(0, 1, 120), store(0, 5, 10), store(0, 7, 5), { kind: "drop", more: 3 }, pass(), store(0, 3, 30), pass()],
  // gap_behind: device 1's last reading was 100 min ago; on its return its gap starts before T and takes the only drop.
  silentDeviceReturns: [store(1, 1, 100), store(0, 5, 10), store(0, 7, 5), { kind: "drop", more: 1 }, pass(), store(1, 3, 0), pass()],
  // measured_behind: a series that starts before T, below readings already stored, takes the budget.
  seriesStartsBehind: [store(0, 5, 10), store(0, 6, 9), store(0, 8, 5), { kind: "drop", more: 2 }, pass(), store(0, 2, 100), pass()],
  // retention: the oldest gap, which used the whole counter, is dropped with its partition.
  retentionFreesBudget: [store(0, 1, 120), store(0, 4, 110), store(0, 5, 10), store(0, 7, 5), { kind: "drop", more: 2 }, pass(), { kind: "retention", keepMinutes: 115 }, pass()],
  // gapsFrom by seq, not time: seq 3 was measured before seq 1 (the clock stepped back), so the gap 1..3 starts in the
  // window although its second reading is older than T.
  clockSteppedBack: [store(0, 1, 30), store(0, 3, 90), { kind: "drop", more: 1 }, pass()],
};
