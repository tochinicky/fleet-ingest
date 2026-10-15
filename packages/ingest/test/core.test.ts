/**
 * Unit tests for ingest's pure core: ack ordering, error classification and message routing.
 */
import { describe, expect, it } from "vitest";
import { AckTracker } from "../src/core/ackTracker.ts";
import { classifyDatabaseError } from "../src/core/errors.ts";
import { routeMessage } from "../src/core/routeMessage.ts";

/** Encodes a JSON value as payload bytes. */
function json(value: unknown): Uint8Array {
  return new TextEncoder().encode(JSON.stringify(value));
}

const receivedAt = new Date("2026-10-14T12:00:00Z");
const reading = { deviceId: "dev-1", epoch: 3, seq: 7, timestamp: "2026-10-14T11:59:55.1234567+00:00", kind: "Battery", batteryPowerW: 500, socPercent: 40 };

describe("AckTracker", () => {
  it("releases in arrival order even when messages finish out of order", () => {
    const tracker = new AckTracker<string>();
    const [a, b, c] = [tracker.track(), tracker.track(), tracker.track()];
    expect(tracker.complete(c, "c")).toEqual([]);
    expect(tracker.complete(b, "b")).toEqual([]);
    expect(tracker.complete(a, "a")).toEqual(["a", "b", "c"]);
    expect(tracker.outstanding()).toBe(0);
  });

  it("releases immediately when the finished message is the oldest", () => {
    const tracker = new AckTracker<string>();
    const a = tracker.track();
    tracker.track();
    expect(tracker.complete(a, "a")).toEqual(["a"]);
    expect(tracker.outstanding()).toBe(1);
  });

  it("refuses to complete a position twice or one never tracked", () => {
    const tracker = new AckTracker<string>();
    const a = tracker.track();
    tracker.complete(a, "a");
    expect(() => tracker.complete(a, "a")).toThrow(/not outstanding/);
    expect(() => tracker.complete(5, "x")).toThrow(/not outstanding/);
  });
});

describe("classifyDatabaseError", () => {
  it.each([
    [{ code: "08006" }, "retryable"],
    [{ code: "57P01" }, "retryable"],
    [{ code: "57014" }, "retryable"],
    [{ code: "25P04" }, "retryable"],
    [{ code: "40001" }, "retryable"],
    [{ code: "40P01" }, "retryable"],
    [{ code: "53300" }, "retryable"],
    [{ code: "ECONNREFUSED" }, "retryable"],
    [new Error("Connection terminated unexpectedly"), "retryable"],
    [new Error("Connection terminated"), "retryable"],
    [{ code: "ECONNRESET" }, "retryable"],
    [{ code: "ETIMEDOUT" }, "retryable"],
    [{ code: "EPIPE" }, "retryable"],
    [new Error("timeout exceeded when trying to connect"), "retryable"],
    [new Error("timeout expired"), "retryable"],
    [new Error("Query read timeout"), "retryable"],
    [new Error("Connection terminated due to connection timeout", { cause: new Error("timeout expired") }), "retryable"],
    [Object.assign(new AggregateError([Object.assign(new Error("connect ECONNREFUSED"), { code: "ECONNREFUSED" })], ""), { code: "ECONNREFUSED" }), "retryable"],
    [new Error("query failed", { cause: Object.assign(new Error("read ECONNRESET"), { code: "ECONNRESET" }) }), "retryable"],
    [{ code: "22003" }, "poison"],
    [{ code: "23514" }, "poison"],
    [{ code: "42P01" }, "fatal"],
    [new Error("something else"), "fatal"],
    ["not an error at all", "fatal"],
  ])("%j is %s", (error, kind) => {
    expect(classifyDatabaseError(error)).toBe(kind);
  });
});

describe("routeMessage", () => {
  it("turns a valid reading into a row, with absent measurements as null", () => {
    const routed = routeMessage("homes/gw-1/devices/dev-1/telemetry", json(reading), receivedAt);
    expect(routed).toEqual({
      type: "reading",
      row: {
        gatewayId: "gw-1", deviceId: "dev-1", epoch: 3, seq: 7, kind: "Battery",
        measuredAt: new Date("2026-10-14T11:59:55.123Z"), receivedAt,
        pvPowerW: null, batteryPowerW: 500, gridPowerW: null, evPowerW: null, energyWh: null, socPercent: 40, chargeLimitA: null,
      },
    });
  });

  it("uses the receive time for the Last Will, which has no timestamp", () => {
    const routed = routeMessage("homes/gw-1/status", json({ state: "offline" }), receivedAt);
    expect(routed).toMatchObject({ type: "status", row: { gatewayId: "gw-1", state: "offline", statusAt: receivedAt, buffered: null } });
  });

  it("dead-letters a reading more than 5 minutes ahead of ingest's clock, but accepts one just inside", () => {
    const at = (ms: number) => json({ ...reading, timestamp: new Date(receivedAt.getTime() + ms).toISOString() });
    expect(routeMessage("homes/gw-1/devices/dev-1/telemetry", at(5 * 60_000 + 1000), receivedAt)).toMatchObject({ type: "dead_letter", row: { reason: "future_timestamp" } });
    expect(routeMessage("homes/gw-1/devices/dev-1/telemetry", at(5 * 60_000 - 1000), receivedAt)).toMatchObject({ type: "reading" });
  });

  it("dead-letters a status from a broken clock too, so it can't freeze the gateway's status", () => {
    const status = json({ state: "online", timestamp: new Date(receivedAt.getTime() + 3600_000).toISOString() });
    expect(routeMessage("homes/gw-1/status", status, receivedAt)).toMatchObject({ type: "dead_letter", row: { reason: "future_timestamp" } });
  });

  it.each([
    ["an unknown topic", "homes/gw-1/commands", json({ mode: "max" }), "unknown_topic"],
    ["bad JSON", "homes/gw-1/devices/dev-1/telemetry", new TextEncoder().encode("{"), "invalid_json"],
    ["a schema failure", "homes/gw-1/devices/dev-1/telemetry", json({ ...reading, seq: -1 }), "schema_mismatch"],
    ["a device mismatch", "homes/gw-1/devices/dev-2/telemetry", json(reading), "device_id_mismatch"],
    ["a bad status", "homes/gw-1/status", json({ state: "sleeping" }), "schema_mismatch"],
  ])("dead-letters %s", (_label, topic, payload, reason) => {
    expect(routeMessage(topic, payload, receivedAt)).toMatchObject({ type: "dead_letter", row: { topic, reason, receivedAt } });
  });
});
