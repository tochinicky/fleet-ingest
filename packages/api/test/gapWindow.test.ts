/**
 * The incremental open-gap window means the same as the fleet gap query (S7-11). The property simulates the database
 * the health job reads: each insert's `stored_at` is its transaction's start, and it becomes visible only when it
 * commits, up to 60 s later (ingest's `transaction_timeout`). Inserts arrive out of order, as duplicates (the primary
 * key keeps the first to commit), as replays that fill gaps, and as backfill measured long ago. Passes run at random
 * times with the real cursor rule and overlap. At every pass, the window must equal the oracle over exactly the rows
 * committed by then.
 *
 * The deliberate-break checks `cursor-without-overlap` and `replay-does-not-close-gap` must make it fail.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { advanceCursor, CURSOR_OVERLAP_MS, deltaLowerBound, GapWindow, gapsOf } from "../src/core/gapWindow.ts";
import type { GatewayGap, WindowReading } from "../src/core/gapWindow.ts";

const WINDOW_MS = 120_000;
const MAX_TRANSACTION_MS = 60_000;
const DEVICES = [{ deviceId: "d-1", gatewayId: "gw-a" }, { deviceId: "d-2", gatewayId: "gw-a" }, { deviceId: "d-3", gatewayId: "gw-b" }];

/** One insert: which reading, when its transaction started, and how long until it committed. */
const insert = fc.record({
  device: fc.integer({ min: 0, max: DEVICES.length - 1 }),
  epoch: fc.integer({ min: 1, max: 2 }),
  seq: fc.integer({ min: 1, max: 25 }),
  measuredAt: fc.integer({ min: 0, max: 400 }).map((s) => s * 1000),
  startedAt: fc.integer({ min: 0, max: 400 }).map((s) => s * 1000),
  commitDelay: fc.integer({ min: 0, max: MAX_TRANSACTION_MS / 1000 }).map((s) => s * 1000),
});

/** One generated insert. */
type Insert = typeof insert extends fc.Arbitrary<infer T> ? T : never;

/** A stored row: the reading, its stored_at, and when it became visible. */
interface Row extends WindowReading {
  storedAt: number;
  committedAt: number;
}

/** The rows the database ends up with: one per key, the insert that committed first (the others hit ON CONFLICT). */
function rowsOf(inserts: Insert[]): Row[] {
  const byKey = new Map<string, Row>();
  for (const i of inserts) {
    const device = DEVICES[i.device] as (typeof DEVICES)[number];
    const row: Row = { ...device, epoch: i.epoch, seq: i.seq, measuredAt: i.measuredAt, storedAt: i.startedAt, committedAt: i.startedAt + i.commitDelay };
    const key = `${row.deviceId}/${row.epoch}/${row.seq}`;
    const held = byKey.get(key);
    if (!held || row.committedAt < held.committedAt) byKey.set(key, row);
  }
  return [...byKey.values()];
}

/** Gaps as comparable strings, independent of the order of gaps with the same `before`. */
const normalise = (gaps: GatewayGap[]) => gaps.map((g) => `${g.gatewayId} ${g.deviceId} e${g.epoch} ${g.fromSeq}-${g.toSeq} (${g.missing}) @${g.before.getTime()}`).sort();

describe("GapWindow, fed through the cursor", () => {
  it("equals the full-window query at every pass, whatever the order, duplicates, replays, late commits and ageing", () => {
    fc.assert(fc.property(
      fc.array(insert, { minLength: 1, maxLength: 120 }),
      fc.uniqueArray(fc.integer({ min: 1, max: 500 }).map((s) => s * 1000), { minLength: 1, maxLength: 25 }),
      (inserts, passTimes) => {
        const rows = rowsOf(inserts);
        const lastCommit = Math.max(...rows.map((r) => r.committedAt));
        const sorted = [...passTimes].sort((a, b) => a - b);
        // Time only moves forward; the last pass comes after every commit, so it sees everything.
        const passes = [...sorted, Math.max(lastCommit + 1000, (sorted[sorted.length - 1] as number) + 1000)];
        const window = new GapWindow();
        let cursor: number | null = null;
        for (const now of passes) {
          const from = now - WINDOW_MS;
          const bound = deltaLowerBound(cursor);
          const delta = rows.filter((r) => r.committedAt <= now && (bound === null || r.storedAt > bound));
          window.add(delta, from);
          window.prune(from);
          cursor = advanceCursor(cursor, delta.map((r) => r.storedAt));
          const visible = rows.filter((r) => r.committedAt <= now);
          expect(normalise(window.openGaps(from, now))).toEqual(normalise(gapsOf(visible, from, now)));
        }
      },
    ), { numRuns: 300 });
  });

  it("the overlap outlasts ingest's longest transaction (transaction_timeout 60 s) with margin", () => {
    expect(CURSOR_OVERLAP_MS).toBeGreaterThanOrEqual(MAX_TRANSACTION_MS + 30_000);
  });
});

describe("gapsOf (the oracle)", () => {
  const at = (s: number) => s * 1000;
  const r = (seq: number, measured: number, epoch = 1): WindowReading => ({ deviceId: "d-1", gatewayId: "gw-a", epoch, seq, measuredAt: at(measured) });

  it("finds a gap per (device, epoch), never across epochs, and only among readings measured in the window", () => {
    const readings = [r(1, 10), r(2, 11), r(5, 14), r(1, 20, 2), r(3, 22, 2), r(9, 500)];
    expect(normalise(gapsOf(readings, at(0), at(100)))).toEqual([
      `gw-a d-1 e1 3-4 (2) @${at(11)}`,
      `gw-a d-1 e2 2-2 (1) @${at(20)}`,
    ]);
  });

  it("a replay that fills the gap closes it", () => {
    expect(gapsOf([r(1, 10), r(2, 11), r(3, 12)], at(0), at(100))).toEqual([]);
  });
});
