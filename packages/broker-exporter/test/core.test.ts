/**
 * Unit tests for the exporter's pure parts: `$SYS` value parsing and log-line classification.
 */
import { describe, expect, it } from "vitest";
import { classifyLogLine } from "../src/core/logEvents.ts";
import { parseSysValue } from "../src/core/sysTopics.ts";

describe("parseSysValue", () => {
  it.each([["1234", 1234], ["5160 seconds", 5160], ["0.25", 0.25], ["active", null], ["", null]])("%j → %j", (payload, value) => {
    expect(parseSysValue(payload)).toBe(value);
  });
});

describe("classifyLogLine", () => {
  it("recognises a drop episode and names the client", () => {
    expect(classifyLogLine("2026-10-14T10:56:30: Outgoing messages are being dropped for client ingest-1.")).toEqual({ kind: "dropping", clientId: "ingest-1" });
  });

  it("recognises a session expiry", () => {
    expect(classifyLogLine("1791996720: Expiring client expiry-probe due to timeout.")).toEqual({ kind: "expiring", clientId: "expiry-probe" });
  });

  it("ignores everything else", () => {
    expect(classifyLogLine("2026-10-14T10:56:30: New client connected from 192.168.32.6 as ingest-1 (p5, c0, k30).")).toBeNull();
  });
});
