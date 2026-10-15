/**
 * Unit and property tests for fleet-sim's chaos knobs (scenarios 1 and 5): the reorderer neither loses nor
 * duplicates, the decisions follow their rates, settings are validated, and every poison kind is built so that ingest
 * must dead-letter it with the reason it's named after, without touching a real reading's key.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { telemetryMessageSchema } from "@fleet-ingest/contract";
import type { TelemetryMessage } from "@fleet-ingest/contract";
import { decide, NO_CHAOS, parseChaosSettings, poisonMessage, POISON_KINDS, Reorderer } from "../src/core/chaos.ts";

/** A seeded generator (mulberry32), so the rate checks are repeatable. */
function seeded(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const reading: TelemetryMessage = { deviceId: "sim-gw-0001-inv-1", epoch: 7, seq: 42, timestamp: "2026-10-14T12:00:00.000Z", kind: "Inverter", pvPowerW: 1200 };

describe("Reorderer", () => {
  it("property: every item pushed comes out exactly once, and at most window - 1 are ever held", () => {
    fc.assert(fc.property(fc.array(fc.integer(), { maxLength: 300 }), fc.integer({ min: 0, max: 60 }), fc.integer(), (items, window, seed) => {
      const reorderer = new Reorderer<number>(seeded(seed));
      const out: number[] = [];
      for (const item of items) {
        out.push(...reorderer.push(item, window));
        expect(reorderer.size()).toBeLessThanOrEqual(Math.max(0, window - 1));
      }
      out.push(...reorderer.drain());
      expect([...out].sort((a, b) => a - b)).toEqual([...items].sort((a, b) => a - b));
    }));
  });

  it("keeps order with a window of 0 or 1, and actually reorders with a window of 50", () => {
    const items = Array.from({ length: 200 }, (_, i) => i);
    for (const window of [0, 1]) {
      const reorderer = new Reorderer<number>(seeded(1));
      expect(items.flatMap((i) => reorderer.push(i, window))).toEqual(items);
    }
    const reorderer = new Reorderer<number>(seeded(1));
    const out = [...items.flatMap((i) => reorderer.push(i, 50)), ...reorderer.drain()];
    expect(out).not.toEqual(items);
  });
});

describe("decide", () => {
  it("does nothing with every knob off", () => {
    const random = seeded(3);
    for (let i = 0; i < 1000; i++) expect(decide(NO_CHAOS, random)).toEqual({ copies: 1, poison: null });
  });

  it("follows the duplicate and poison rates, and uses every poison kind", () => {
    const random = seeded(4);
    const decisions = Array.from({ length: 20_000 }, () => decide({ duplicateRate: 0.2, reorderWindow: 0, poisonRate: 0.05 }, random));
    const duplicates = decisions.filter((d) => d.copies === 2).length / decisions.length;
    const poisons = decisions.filter((d) => d.poison !== null);
    expect(duplicates).toBeGreaterThan(0.18);
    expect(duplicates).toBeLessThan(0.22);
    expect(poisons.length / decisions.length).toBeGreaterThan(0.04);
    expect(poisons.length / decisions.length).toBeLessThan(0.06);
    expect(new Set(poisons.map((d) => d.poison))).toEqual(new Set(POISON_KINDS));
  });
});

describe("parseChaosSettings", () => {
  it("merges a partial update and rejects out-of-range values", () => {
    expect(parseChaosSettings({ duplicateRate: 0.2, reorderWindow: 50 }, NO_CHAOS)).toEqual({ duplicateRate: 0.2, reorderWindow: 50, poisonRate: 0 });
    expect(parseChaosSettings({ duplicateRate: 1.5 }, NO_CHAOS)).toMatch(/duplicateRate/);
    expect(parseChaosSettings({ reorderWindow: 2.5 }, NO_CHAOS)).toMatch(/reorderWindow/);
    expect(parseChaosSettings("x", NO_CHAOS)).toMatch(/object/);
  });
});

describe("poisonMessage", () => {
  const now = new Date("2026-10-14T12:00:00Z");

  it("builds each kind so it fails the way its name says", () => {
    const parse = (payload: string) => { try { return { json: true, valid: telemetryMessageSchema.safeParse(JSON.parse(payload)).success }; } catch { return { json: false, valid: false }; } };
    const invalid = poisonMessage("invalid_json", reading, "sim-gw-0002-inv-1", now);
    expect(parse(invalid.payload).json).toBe(false);
    expect(parse(poisonMessage("schema_mismatch", reading, "x", now).payload)).toEqual({ json: true, valid: false });
    const mismatch = poisonMessage("device_id_mismatch", reading, "x", now);
    expect(JSON.parse(mismatch.payload).deviceId).not.toBe(mismatch.topicDeviceId);
    const future = JSON.parse(poisonMessage("future_timestamp", reading, "x", now).payload);
    expect(new Date(future.timestamp).getTime() - now.getTime()).toBe(600_000);
    const foreign = poisonMessage("foreign_device", reading, "sim-gw-0002-inv-1", now);
    expect(foreign).toMatchObject({ topicDeviceId: "sim-gw-0002-inv-1" });
    expect(JSON.parse(foreign.payload).deviceId).toBe("sim-gw-0002-inv-1");
  });

  it("never reuses a real reading's key: every parseable poison has a seq far outside real ones", () => {
    for (const kind of POISON_KINDS.filter((k) => k !== "invalid_json" && k !== "schema_mismatch")) {
      expect(JSON.parse(poisonMessage(kind, reading, "sim-gw-0002-inv-1", now).payload).seq).toBe(900_000_042);
    }
  });
});
