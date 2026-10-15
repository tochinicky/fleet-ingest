/**
 * Unit and property tests for the pure parts of commands as desired state (decisions.md S7-1): the serial queue the
 * reconciler publishes through, and the properties of the drift rule `commandStatus`.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import { COMMAND_STUCK_MS, commandStatus } from "../src/core/health.ts";
import type { GatewayState } from "../src/core/health.ts";
import { createSerialQueue } from "../src/core/serialQueue.ts";

/** Resolves after `ms` milliseconds. */
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe("createSerialQueue", () => {
  it("runs items one at a time, in the order they were added, even when later ones are faster", async () => {
    const queue = createSerialQueue();
    const events: string[] = [];
    let running = 0;
    const item = (name: string, ms: number) => queue.run(async () => {
      running++;
      expect(running).toBe(1);
      events.push(`start ${name}`);
      await pause(ms);
      events.push(`end ${name}`);
      running--;
      return name;
    });
    const results = await Promise.all([item("a", 30), item("b", 1), item("c", 10)]);
    expect(results).toEqual(["a", "b", "c"]);
    expect(events).toEqual(["start a", "end a", "start b", "end b", "start c", "end c"]);
  });

  it("rejects a failed item on its own promise and still runs the items after it", async () => {
    const queue = createSerialQueue();
    const failed = queue.run(async () => { throw new Error("broker refused"); });
    const next = queue.run(async () => "published");
    await expect(failed).rejects.toThrow("broker refused");
    await expect(next).resolves.toBe("published");
  });

  it("property: the last write added is the last one applied, whatever each item takes", async () => {
    await fc.assert(fc.asyncProperty(fc.array(fc.tuple(fc.constantFrom("surplus", "max", "off"), fc.integer({ min: 0, max: 5 })), { minLength: 1, maxLength: 8 }), async (writes) => {
      const queue = createSerialQueue();
      let retained: string | null = null;
      await Promise.all(writes.map(([mode, ms]) => queue.run(async () => { await pause(ms); retained = mode; })));
      expect(retained).toBe(writes[writes.length - 1]?.[0]);
    }), { numRuns: 50 });
  });
});

const now = new Date("2026-10-14T12:00:00Z");
const mode = fc.option(fc.constantFrom("surplus", "max", "off"), { nil: null });
const pastTime = fc.option(fc.integer({ min: 0, max: 86_400_000 }).map((ms) => new Date(now.getTime() - ms)), { nil: null });
const state = fc.constantFrom<GatewayState>("online", "offline", "unknown");
const evidence = fc.record({ desiredMode: mode, desiredAt: pastTime, reportedMode: mode, cameBackAt: pastTime });

describe("commandStatus properties (S7-1)", () => {
  it("is pending exactly when a desired mode exists and differs from the reported one", () => {
    fc.assert(fc.property(evidence, state, (e, s) => {
      expect(commandStatus(e, s, now).pending).toBe(e.desiredMode !== null && e.desiredMode !== e.reportedMode);
    }));
  });

  it("is never stuck unless pending and online; pendingSince is set exactly when pending", () => {
    fc.assert(fc.property(evidence, state, (e, s) => {
      const status = commandStatus(e, s, now);
      if (status.stuck) expect(status.pending && s === "online").toBe(true);
      expect(status.pendingSince).toEqual(status.pending ? e.desiredAt : null);
    }));
  });

  it("once stuck, stays stuck as time passes (until the gateway reports the mode)", () => {
    fc.assert(fc.property(evidence, fc.integer({ min: 0, max: 3_600_000 }), (e, laterMs) => {
      if (commandStatus(e, "online", now).stuck) {
        expect(commandStatus(e, "online", new Date(now.getTime() + laterMs)).stuck).toBe(true);
      }
    }));
  });

  it("is never stuck within 5 minutes of the gateway coming back", () => {
    fc.assert(fc.property(evidence, fc.integer({ min: 0, max: COMMAND_STUCK_MS }), (e, sinceBackMs) => {
      const cameBackAt = new Date(now.getTime() - sinceBackMs);
      expect(commandStatus({ ...e, cameBackAt }, "online", now).stuck).toBe(false);
    }));
  });
});
