/**
 * The partition window rules (S8-2): what's kept, what's dropped, what ingest accepts, and the one day between the
 * last two that makes a missing partition impossible at insert time.
 */
import fc from "fast-check";
import { describe, expect, it } from "vitest";
import {
  acceptedSince, assertPartitionSettings, covers, keptDays, parseBound, partitionName, planPartitions, RETENTION_FLOOR_DAYS, startOfUtcDay,
} from "../src/partitions.ts";
import type { PartitionRange } from "../src/partitions.ts";

const DAY = 86_400_000;
const at = (iso: string) => new Date(iso);
const settings = { retentionDays: 30, aheadDays: 7 };

describe("the window", () => {
  it("keeps [T - D, T + A] and drops what ends at or before T - D", () => {
    const now = at("2026-10-16T13:00:00Z");
    const days = keptDays(now, settings);
    expect(days[0]?.from).toEqual(at("2026-09-16T00:00:00Z"));
    expect(days[days.length - 1]?.from).toEqual(at("2026-10-23T00:00:00Z"));
    const existing: PartitionRange[] = [
      { name: "readings_p20260915", from: at("2026-09-15T00:00:00Z"), to: at("2026-09-16T00:00:00Z") },
      { name: "readings_p20260916", from: at("2026-09-16T00:00:00Z"), to: at("2026-09-17T00:00:00Z") },
    ];
    const plan = planPartitions(existing, now, settings);
    expect(plan.drop.map((p) => p.name)).toEqual(["readings_p20260915"]);
    expect(plan.create.map((p) => p.name)).not.toContain("readings_p20260916");
    expect(plan.create).toHaveLength(days.length - 1);
  });

  it("ingest accepts from T - (D - 1), one whole day after the oldest kept partition", () => {
    expect(acceptedSince(at("2026-10-16T00:00:01Z"), 30)).toEqual(at("2026-09-17T00:00:00Z"));
  });

  it("whatever the clock says, every accepted reading has a kept partition, even when ingest and the job read the clock across midnight", () => {
    fc.assert(fc.property(
      fc.integer({ min: Date.parse("2026-01-01"), max: Date.parse("2027-01-01") }),
      fc.integer({ min: 0, max: 60_000 }),
      fc.integer({ min: RETENTION_FLOOR_DAYS, max: 90 }),
      (jobMs, ingestLagMs, retentionDays) => {
        const jobNow = new Date(jobMs);
        const ingestNow = new Date(jobMs + ingestLagMs);
        const kept = keptDays(jobNow, { retentionDays, aheadDays: 2 });
        const cutoff = acceptedSince(ingestNow, retentionDays);
        const ingestLimit = new Date(ingestNow.getTime() + 5 * 60_000);
        expect(covers(kept, cutoff, ingestLimit)).toBe(true);
      },
    ));
  });
});

describe("settings and helpers", () => {
  it("refuses a retention under the floor, or less than two days ahead", () => {
    expect(() => assertPartitionSettings({ retentionDays: RETENTION_FLOOR_DAYS - 1, aheadDays: 7 })).toThrow(/at least 4/);
    expect(() => assertPartitionSettings({ retentionDays: 30, aheadDays: 1 })).toThrow(/at least 2/);
    expect(() => assertPartitionSettings(settings)).not.toThrow();
  });

  it("names a day's partition by its UTC date", () => {
    expect(partitionName(startOfUtcDay(at("2026-10-16T23:59:59Z")))).toBe("readings_p20261016");
  });

  it("covers detects a hole", () => {
    const day = (iso: string) => ({ name: "x", from: at(iso), to: new Date(at(iso).getTime() + DAY) });
    expect(covers([day("2026-10-01T00:00:00Z"), day("2026-10-02T00:00:00Z")], at("2026-10-01T05:00:00Z"), at("2026-10-02T23:00:00Z"))).toBe(true);
    expect(covers([day("2026-10-01T00:00:00Z"), day("2026-10-03T00:00:00Z")], at("2026-10-01T05:00:00Z"), at("2026-10-03T01:00:00Z"))).toBe(false);
  });

  it("parses Postgres's bound expressions, in UTC or another session time zone", () => {
    expect(parseBound("FOR VALUES FROM ('2026-10-16 00:00:00+00') TO ('2026-10-17 00:00:00+00')")).toEqual({ from: at("2026-10-16T00:00:00Z"), to: at("2026-10-17T00:00:00Z") });
    expect(parseBound("FOR VALUES FROM ('2026-10-16 02:00:00+02') TO ('2026-10-17 05:30:00+05:30')")).toEqual({ from: at("2026-10-16T00:00:00Z"), to: at("2026-10-17T00:00:00Z") });
  });
});
