/**
 * Unit tests for fleet-sim's pure core: the outbox rules, the numbering, the pace, and that every simulated reading
 * satisfies the shared contract. It also measures payload sizes, for `max_queued_bytes`.
 */
import { describe, expect, it } from "vitest";
import { decodeTelemetry } from "@fleet-ingest/contract";
import type { TelemetryMessage } from "@fleet-ingest/contract";
import { devicesFor, measure, seededRandom } from "../src/core/devices.ts";
import { AckLedger, RangeSet } from "../src/core/ackLedger.ts";
import { EvenPace } from "../src/core/rateLimiter.ts";
import { Sequencer } from "../src/core/sequencer.ts";
import { TelemetryBuffer } from "../src/core/telemetryBuffer.ts";

describe("TelemetryBuffer", () => {
  it("is first-in, first-out", () => {
    const buffer = new TelemetryBuffer<number>(10);
    [1, 2, 3].forEach((n) => buffer.push(n));
    expect([buffer.shift(), buffer.shift(), buffer.shift(), buffer.shift()]).toEqual([1, 2, 3, undefined]);
  });

  it("drops the oldest when full, and counts the drops", () => {
    const buffer = new TelemetryBuffer<number>(3);
    [1, 2, 3, 4, 5].forEach((n) => buffer.push(n));
    expect(buffer.dropped()).toBe(2);
    expect([buffer.shift(), buffer.shift(), buffer.shift()]).toEqual([3, 4, 5]);
  });

  it("puts unacknowledged messages back at the front, in their original order", () => {
    const buffer = new TelemetryBuffer<number>(10);
    [1, 2, 3, 4].forEach((n) => buffer.push(n));
    const taken = [buffer.shift() as number, buffer.shift() as number];
    buffer.returnToFront(taken);
    expect(Array.from({ length: 4 }, () => buffer.shift())).toEqual([1, 2, 3, 4]);
  });

  it("stays bounded over many messages", () => {
    const buffer = new TelemetryBuffer<number>(100);
    for (let n = 0; n < 100_000; n++) buffer.push(n);
    expect(buffer.size()).toBe(100);
    expect(buffer.dropped()).toBe(99_900);
    expect(buffer.shift()).toBe(99_900);
  });
});

describe("RangeSet and AckLedger", () => {
  it("merges values into ranges in any order, ignoring repeats", () => {
    const set = new RangeSet();
    [5, 1, 3, 2, 9, 4, 4, 10, 7].forEach((v) => set.add(v));
    expect(set.ranges()).toEqual([[1, 5], [7, 7], [9, 10]]);
    expect(set.count()).toBe(8);
    set.add(8);
    expect(set.ranges()).toEqual([[1, 5], [7, 10]]);
    set.add(6);
    expect(set.ranges()).toEqual([[1, 10]]);
  });

  it("counts acknowledged statuses per gateway", () => {
    const ledger = new AckLedger();
    ledger.recordStatus("g1");
    ledger.recordStatus("g1");
    ledger.recordStatus("g2");
    expect(ledger.statuses()).toEqual({ total: 3, byGateway: { g1: 2, g2: 1 } });
  });

  it("keeps one entry per device and epoch", () => {
    const ledger = new AckLedger();
    [1, 2, 3].forEach((seq) => ledger.record("d", 1, seq));
    ledger.record("d", 2, 1);
    expect(ledger.entries()).toEqual([
      { deviceId: "d", epoch: 1, ranges: [[1, 3]], count: 3 },
      { deviceId: "d", epoch: 2, ranges: [[1, 1]], count: 1 },
    ]);
  });
});

describe("Sequencer", () => {
  it("counts each device from 1 within the epoch", () => {
    const sequencer = new Sequencer(7);
    expect([sequencer.next("a"), sequencer.next("a"), sequencer.next("b")]).toEqual([
      { epoch: 7, seq: 1 }, { epoch: 7, seq: 2 }, { epoch: 7, seq: 1 },
    ]);
  });
});

describe("Sequencer.powerLoss", () => {
  it("moves to the next epoch and rolls each device's seq back, never below 1", () => {
    const sequencer = new Sequencer(7);
    for (let i = 0; i < 10; i++) sequencer.next("a");
    sequencer.next("b");
    sequencer.powerLoss(4);
    expect([sequencer.next("a"), sequencer.next("b")]).toEqual([{ epoch: 8, seq: 7 }, { epoch: 8, seq: 1 }]);
  });
});

describe("EvenPace", () => {
  it("spaces 200/s claims 5 ms apart, without bursts", () => {
    const pace = new EvenPace(200);
    expect([pace.claim(0), pace.claim(0), pace.claim(0), pace.claim(20)]).toEqual([0, 5, 10, 0]);
  });
});

describe("simulated readings", () => {
  const random = seededRandom(1);
  const sequencer = new Sequencer(1);
  const messages: TelemetryMessage[] = [];
  for (let g = 0; g < 50; g++) {
    for (const device of devicesFor(`sim-gw-${String(g).padStart(4, "0")}`, 4)) {
      for (let h = 0; h < 24; h++) {
        const at = new Date(Date.UTC(2026, 9, 14, h, 30));
        messages.push({ deviceId: device.deviceId, ...sequencer.next(device.deviceId), timestamp: at.toISOString(), kind: device.kind, ...measure(device, at, random) });
      }
    }
  }

  it("all satisfy the shared contract", () => {
    const failures = messages.filter((m) => !decodeTelemetry(new TextEncoder().encode(JSON.stringify(m))).ok);
    expect(failures).toEqual([]);
  });

  it("have device IDs unique across the fleet", () => {
    const ids = new Set(messages.map((m) => m.deviceId));
    expect(ids.size).toBe(200);
  });

  it("are about as large as the real gateway's, for sizing max_queued_bytes", () => {
    const sizes = messages.map((m) => Buffer.byteLength(JSON.stringify(m))).sort((a, b) => a - b);
    const p95 = sizes[Math.floor(sizes.length * 0.95)] as number;
    expect(p95).toBeLessThanOrEqual(200);
  });
});
