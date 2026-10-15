/**
 * Unit tests for the health rules, including the two clock cases from the slice 2 review: a gateway clock that runs
 * behind, and a Last Will processed late by a replica that was down.
 */
import { describe, expect, it } from "vitest";
import { classifyGateway, commandStatus, explainGaps, isDeviceStale } from "../src/core/health.ts";
import type { Gap } from "../src/core/health.ts";

const now = new Date("2026-10-14T12:00:00Z");
const ago = (seconds: number) => new Date(now.getTime() - seconds * 1000);

describe("classifyGateway", () => {
  it("is online with a reading received after the latest offline status, whatever the stored status says", () => {
    expect(classifyGateway({ lastStatus: "offline", lastOfflineReceivedAt: ago(30), lastStatusReceivedAt: ago(30), lastReadingReceivedAt: ago(10) }, now).state).toBe("online");
  });

  it("dead link: readings sent before the Last Will don't keep the gateway online, so it's offline as soon as the Will arrives", () => {
    expect(classifyGateway({ lastStatus: "offline", lastOfflineReceivedAt: ago(1), lastStatusReceivedAt: ago(1), lastReadingReceivedAt: ago(23) }, now).state).toBe("offline");
  });

  it("is unknown with no status ever and no recent reading", () => {
    expect(classifyGateway({ lastStatus: null, lastOfflineReceivedAt: null, lastStatusReceivedAt: null, lastReadingReceivedAt: ago(600) }, now).state).toBe("unknown");
  });

  it("is online with no status ever but a recent reading (a cold start, S1-2)", () => {
    expect(classifyGateway({ lastStatus: null, lastOfflineReceivedAt: null, lastStatusReceivedAt: null, lastReadingReceivedAt: ago(10) }, now).state).toBe("online");
  });

  it("is offline when the last status is offline and nothing arrived since", () => {
    expect(classifyGateway({ lastStatus: "offline", lastOfflineReceivedAt: ago(30), lastStatusReceivedAt: ago(30), lastReadingReceivedAt: ago(90) }, now))
      .toEqual({ state: "offline", reason: "last status is offline" });
  });

  it("is offline when the last status is online but nothing was heard for 3 minutes (a lost Last Will)", () => {
    expect(classifyGateway({ lastStatus: "online", lastOfflineReceivedAt: null, lastStatusReceivedAt: ago(200), lastReadingReceivedAt: ago(190) }, now).state).toBe("offline");
  });

  it("is online when the last status is online and something was heard within 3 minutes", () => {
    expect(classifyGateway({ lastStatus: "online", lastOfflineReceivedAt: null, lastStatusReceivedAt: ago(170), lastReadingReceivedAt: null }, now).state).toBe("online");
  });

  it("slow gateway clock (10 min behind): its reboot 'online' is older than the Last Will, so the stored status stays offline, but its readings after the Will keep it online", () => {
    expect(classifyGateway({ lastStatus: "offline", lastOfflineReceivedAt: ago(40), lastStatusReceivedAt: ago(1), lastReadingReceivedAt: ago(3) }, now).state).toBe("online");
  });

  it("late queued Last Will: processed after the gateway came back, it shows offline only until the next reading (≤ 5 s)", () => {
    const lateWill = { lastStatus: "offline" as const, lastOfflineReceivedAt: ago(2), lastStatusReceivedAt: ago(2) };
    expect(classifyGateway({ ...lateWill, lastReadingReceivedAt: ago(4) }, now).state).toBe("offline");
    expect(classifyGateway({ ...lateWill, lastReadingReceivedAt: ago(0) }, now).state).toBe("online");
  });
});

describe("health is only as fresh as ingest", () => {
  it("a backlogged replica delays offline detection by at most the backlog plus 60 s", () => {
    const willAt = ago(300);
    const backlogDrainedAt = ago(120);
    const evidence = { lastStatus: "offline" as const, lastOfflineReceivedAt: willAt, lastStatusReceivedAt: willAt, lastReadingReceivedAt: backlogDrainedAt };
    const at = (secondsAfterDrain: number) => new Date(backlogDrainedAt.getTime() + secondsAfterDrain * 1000);
    expect(classifyGateway(evidence, at(59)).state).toBe("online");
    expect(classifyGateway(evidence, at(61)).state).toBe("offline");
  });
});

describe("isDeviceStale", () => {
  it("is stale only when the gateway is online and the device silent for over 60 s", () => {
    expect(isDeviceStale("online", ago(61), now)).toBe(true);
    expect(isDeviceStale("online", ago(59), now)).toBe(false);
    expect(isDeviceStale("online", null, now)).toBe(true);
    expect(isDeviceStale("offline", ago(600), now)).toBe(false);
    expect(isDeviceStale("unknown", ago(600), now)).toBe(false);
  });
});

describe("explainGaps", () => {
  const gap = (deviceId: string, missing: number, secondsAgo: number): Gap =>
    ({ deviceId, epoch: 1, fromSeq: 10, toSeq: 9 + missing, missing, before: ago(secondsAgo) });

  it("explains the oldest gaps first, up to the gateway's dropped count", () => {
    const result = explainGaps([gap("b", 5, 100), gap("a", 10, 300), gap("c", 2, 50)], 15);
    expect(result.map((g) => [g.deviceId, g.explained])).toEqual([["a", true], ["b", true], ["c", false]]);
  });

  it("stops at the first gap that doesn't fit: a later, smaller gap stays unexplained (explain-back Q4: 300 vs 200, 150, 50)", () => {
    const result = explainGaps([gap("a", 200, 300), gap("b", 150, 200), gap("c", 50, 100)], 300);
    expect(result.map((g) => g.explained)).toEqual([true, false, false]);
  });

  it("lifetime drops are spent on old gaps first, so a new gap in today's window is unexplained", () => {
    const old = gap("a", 300, 86_400 * 3);
    const recent = gap("a", 50, 600);
    expect(explainGaps([recent, old], 300).find((g) => g.before === recent.before)?.explained).toBe(false);
  });

  it("explains nothing when nothing was dropped", () => {
    expect(explainGaps([gap("a", 1, 10)], 0).every((g) => !g.explained)).toBe(true);
  });
});

describe("commandStatus (S7-1)", () => {
  const set = (desiredMode: string | null, desiredSecondsAgo: number, reportedMode: string | null, cameBackSecondsAgo: number | null = null) => ({
    desiredMode, desiredAt: desiredMode ? ago(desiredSecondsAgo) : null, reportedMode, cameBackAt: cameBackSecondsAgo === null ? null : ago(cameBackSecondsAgo),
  });

  it("is not pending with no desired mode, or once the gateway reports it", () => {
    expect(commandStatus(set(null, 0, "surplus"), "online", now)).toEqual({ pending: false, pendingSince: null, stuck: false });
    expect(commandStatus(set("max", 900, "max"), "online", now).pending).toBe(false);
  });

  it("is pending since the desired mode was set, and stuck after 5 minutes online", () => {
    expect(commandStatus(set("max", 60, "surplus"), "online", now)).toEqual({ pending: true, pendingSince: ago(60), stuck: false });
    expect(commandStatus(set("max", 301, "surplus"), "online", now).stuck).toBe(true);
  });

  it("is never stuck while the gateway is offline or unknown", () => {
    expect(commandStatus(set("max", 3600, "surplus"), "offline", now)).toEqual({ pending: true, pendingSince: ago(3600), stuck: false });
    expect(commandStatus(set("max", 3600, null), "unknown", now).stuck).toBe(false);
  });

  it("counts the 5 minutes from when the gateway came back, if that is later", () => {
    expect(commandStatus(set("max", 3600, "surplus", 120), "online", now).stuck).toBe(false);
    expect(commandStatus(set("max", 3600, "surplus", 400), "online", now).stuck).toBe(true);
    expect(commandStatus(set("max", 400, "surplus", 3600), "online", now).stuck).toBe(true);
  });
});
