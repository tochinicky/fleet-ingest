/**
 * Tests the contract against messages captured from the real `home-energy-gateway` (2026-10-14, demo stack, 114
 * messages across all four device kinds, including an `offline` Last Will), plus the rejections ingest relies on.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { decodeStatus, decodeTelemetry, parseTopic, statusTopic, telemetryTopic } from "../src/index.ts";

/** One captured line: topic, retained flag, payload. */
interface Captured {
  topic: string;
  retained: boolean;
  payload: string;
}

/** Loads the tab-separated capture made with `mosquitto_sub -F '%t\t%r\t%p'`. */
function loadCapture(): Captured[] {
  const text = readFileSync(join(import.meta.dirname, "fixtures", "gateway-capture.tsv"), "utf8");
  return text.trim().split("\n").map((line) => {
    const [topic = "", retained = "0", payload = ""] = line.split("\t");
    return { topic, retained: retained === "1", payload };
  });
}

/** Encodes a string as payload bytes, the way MQTT delivers it. */
function bytes(text: string): Uint8Array {
  return new TextEncoder().encode(text);
}

const capture = loadCapture();

describe("messages captured from the real gateway", () => {
  it("covers every device kind and both status states", () => {
    const kinds = new Set(capture.filter((m) => m.topic.endsWith("/telemetry")).map((m) => JSON.parse(m.payload).kind));
    const states = new Set(capture.filter((m) => m.topic.endsWith("/status")).map((m) => JSON.parse(m.payload).state));
    expect([...kinds].sort()).toEqual(["Battery", "Charger", "GridMeter", "Inverter"]);
    expect([...states].sort()).toEqual(["offline", "online"]);
  });

  it.each(capture.map((m) => [m.topic, m] as const))("decodes %s", (_topic, message) => {
    const parsed = parseTopic(message.topic);
    if (parsed.kind === "telemetry") {
      const decoded = decodeTelemetry(bytes(message.payload));
      expect(decoded).toMatchObject({ ok: true });
      if (decoded.ok) expect(decoded.value.deviceId).toBe(parsed.deviceId);
    } else {
      expect(parsed.kind).toBe("status");
      expect(decodeStatus(bytes(message.payload))).toMatchObject({ ok: true });
    }
  });
});

describe("parseTopic", () => {
  it("round-trips the topics it builds", () => {
    expect(parseTopic(telemetryTopic("gw-1", "dev-1"))).toEqual({ kind: "telemetry", gatewayId: "gw-1", deviceId: "dev-1" });
    expect(parseTopic(statusTopic("gw-1"))).toEqual({ kind: "status", gatewayId: "gw-1" });
  });

  it.each([
    "homes/gw-1/commands",
    "homes//status",
    "homes/gw-1/devices//telemetry",
    "homes/gw-1/devices/dev-1/telemetry/extra",
    "other/gw-1/status",
    "",
  ])("treats %j as unknown", (topic) => {
    expect(parseTopic(topic)).toEqual({ kind: "unknown" });
  });
});

describe("decodeTelemetry rejects", () => {
  const valid = { deviceId: "d", epoch: 1, seq: 2, timestamp: "2026-10-14T11:07:52.4362141+00:00", kind: "Battery" };

  it("bytes that aren't JSON", () => {
    expect(decodeTelemetry(bytes("{not json"))).toMatchObject({ ok: false, reason: "invalid_json" });
  });

  it("bytes that aren't UTF-8", () => {
    expect(decodeTelemetry(new Uint8Array([0xff, 0xfe, 0x7b]))).toMatchObject({ ok: false, reason: "invalid_json" });
  });

  it.each([
    ["a missing seq", { ...valid, seq: undefined }],
    ["a negative seq", { ...valid, seq: -1 }],
    ["a fractional epoch", { ...valid, epoch: 1.5 }],
    ["an unknown kind", { ...valid, kind: "Heatpump" }],
    ["a timestamp without offset", { ...valid, timestamp: "2026-10-14T11:07:52" }],
    ["a null measurement", { ...valid, pvPowerW: null }],
    ["a state of charge over 100", { ...valid, socPercent: 101 }],
  ])("%s", (_label, payload) => {
    expect(decodeTelemetry(bytes(JSON.stringify(payload)))).toMatchObject({ ok: false, reason: "schema_mismatch" });
  });

  it("but strips unknown extra fields instead of rejecting them", () => {
    const decoded = decodeTelemetry(bytes(JSON.stringify({ ...valid, firmware: "2.1" })));
    expect(decoded).toMatchObject({ ok: true });
    if (decoded.ok) expect(decoded.value).not.toHaveProperty("firmware");
  });
});
