/**
 * The gap checkpoint's oracle (S8-4): for any history of readings, drop-counter changes and retention cuts, a
 * checkpointed pass gives exactly what explaining all stored history from scratch gives, for every gap in the window.
 * The history includes fills behind the checkpoint, gaps that straddle it, keys reused with another timestamp, devices
 * silent for longer than the window, and clocks that step backwards.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { explainCheckpointed } from "../src/core/gapCheckpoint.ts";
import type { GapCheckpoint, RecomputeReason } from "../src/core/gapCheckpoint.ts";
import { explainGaps } from "../src/core/health.ts";
import { FIXED_HISTORIES, MINUTE, modelSource, op, seriesGaps, WINDOW } from "./support/gapModel.ts";
import type { ModelRow, Op } from "./support/gapModel.ts";

/** Runs `ops`, checking every pass against the oracle; returns the reasons each pass recomputed (null = resumed). */
async function replay(ops: Op[]): Promise<Array<RecomputeReason | null>> {
  const rows: ModelRow[] = [];
  let now = Date.parse("2026-10-16T12:00:00Z");
  let storedClock = 0;
  let dropped = 0;
  let retainedFrom: number | null = null;
  let checkpoint: GapCheckpoint | null = null;
  const reasons: Array<RecomputeReason | null> = [];
  for (const o of ops) {
    if (o.kind === "store" || o.kind === "next") {
      const deviceId = `d${o.device}`;
      const top = Math.max(0, ...rows.filter((r) => r.deviceId === deviceId && r.epoch === o.epoch).map((r) => r.seq));
      const seq = o.kind === "store" ? o.seq : o.back > 0 ? Math.max(1, top - o.back) : top + 1 + o.skip;
      const row = { deviceId, epoch: o.epoch, seq, measuredAt: now - o.ageMinutes * MINUTE, storedAt: ++storedClock };
      // Ingest rejects what retention no longer keeps (too_old), and the key absorbs a redelivery.
      if (retainedFrom !== null && row.measuredAt < retainedFrom) continue;
      if (rows.some((r) => r.deviceId === row.deviceId && r.epoch === row.epoch && r.seq === row.seq && r.measuredAt === row.measuredAt)) continue;
      rows.push(row);
    } else if (o.kind === "drop") {
      dropped += o.more;
    } else if (o.kind === "retention") {
      const cut = now - o.keepMinutes * MINUTE;
      if (retainedFrom === null || cut > retainedFrom) {
        retainedFrom = cut;
        for (let i = rows.length - 1; i >= 0; i--) if ((rows[i] as ModelRow).measuredAt < cut) rows.splice(i, 1);
      }
    } else {
      now += o.advanceMinutes * MINUTE;
      const windowFrom = new Date(now - WINDOW);
      const result = await explainCheckpointed(modelSource(rows), checkpoint,
        { droppedTelemetry: dropped, retainedFrom: retainedFrom === null ? null : new Date(retainedFrom), passStartedAt: new Date(storedClock), staleBefore: new Date(0) }, windowFrom);
      const oracle = explainGaps(seriesGaps(rows), dropped).filter((g) => g.before >= windowFrom);
      expect(result.gaps).toEqual(oracle);
      checkpoint = result.checkpoint;
      reasons.push(result.recomputed);
    }
  }
  return reasons;
}

describe("the gap checkpoint (S8-4)", () => {
  it("equals explaining all stored history from scratch, after any sequence of readings, drops, retention cuts and passes", async () => {
    let resumed = 0;
    let passes = 0;
    await fc.assert(fc.asyncProperty(fc.array(op, { maxLength: 80 }), async (ops) => {
      const reasons = await replay(ops);
      passes += reasons.length;
      resumed += reasons.filter((r) => r === null).length;
    }), { numRuns: 3000 });
    expect(resumed / passes, "the checkpoint was actually used, not recomputed every time").toBeGreaterThan(0.15);
  });

  it.each(Object.entries(FIXED_HISTORIES))("equals the oracle on the fixed history %s", async (_name, history) => {
    await replay(history);
  });

  const store = (device: number, seq: number, ageMinutes: number, epoch = 1): Op => ({ kind: "store", device, epoch, seq, ageMinutes });
  const pass = (advanceMinutes = 0): Op => ({ kind: "pass", advanceMinutes });

  // The random property alone catches a missing gap_behind or drop-counter rule; the retention and measured_behind
  // rules need histories it rarely generates, so these cases are built so the result, not only the reason, differs.
  it("resumes when readings are appended in order, and recomputes for each change that can alter a folded gap", async () => {
    const history: Op[] = [store(0, 1, 120), store(0, 5, 10), { kind: "drop", more: 3 }, pass()];
    expect(await replay([...history, store(0, 6, 1), pass(1)])).toEqual(["no_checkpoint", null]);
    // A fill of the straddling gap 1..5, measured after T: judged by order, not time.
    expect(await replay([store(0, 1, 120), store(0, 5, 10), pass(), store(0, 3, 30), pass()])).toEqual(["no_checkpoint", "gap_behind"]);
    // A device silent for longer than the window comes back: its new gap starts before T.
    // Its last reading before the silence was measured before T, and so is the predecessor of the one it sends on its return.
    expect(await replay([...history, store(1, 1, 100), pass(), store(1, 4, 0), pass()])).toEqual(["no_checkpoint", "measured_behind", "gap_behind"]);
    expect(await replay([...history, { kind: "drop", more: 1 }, pass()])).toEqual(["no_checkpoint", "dropped_changed"]);
    // Retention drops the oldest gap (2 missing), which had used the whole counter: the window's gap of 1 becomes
    // explained. Resuming would keep the old budget and get it wrong.
    expect(await replay([store(0, 1, 120), store(0, 4, 110), store(0, 5, 10), store(0, 7, 5), { kind: "drop", more: 2 }, pass(),
      { kind: "retention", keepMinutes: 115 }, pass()])).toEqual(["no_checkpoint", "retention"]);
    // A key reused with an earlier timestamp sorts before the stored one, after seq 1, measured before T.
    expect(await replay([...history, store(0, 5, 115), pass()])).toEqual(["no_checkpoint", "measured_behind"]);
    // A series that starts before T, below readings already stored: it has no predecessor, but it was measured before
    // T, and its gap (2 missing) takes the budget that explained the window's gap of 1.
    expect(await replay([store(0, 5, 10), store(0, 6, 9), store(0, 8, 5), { kind: "drop", more: 2 }, pass(), store(0, 2, 100), pass()]))
      .toEqual(["no_checkpoint", "measured_behind"]);
    // The same, with a clock that stepped back far after T: judged by order, so its predecessor gives it away.
    expect(await replay([store(0, 1, 120), store(0, 9, 5), pass(), store(0, 4, -1), pass()])).toEqual(["no_checkpoint", "gap_behind"]);
  });
});
