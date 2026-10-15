/**
 * Unit tests for the reading-history request rules: defaults, bounds and cursors.
 */
import { describe, expect, it } from "vitest";
import { decodeCursor, encodeCursor, MAX_PAGE_SIZE, parseReadingQuery } from "../src/core/readingQuery.ts";

const now = new Date("2026-10-14T12:00:00Z");

describe("parseReadingQuery", () => {
  it("defaults to the last hour and 100 rows", () => {
    expect(parseReadingQuery({}, now)).toEqual({ from: new Date("2026-10-14T11:00:00Z"), to: now, limit: 100, after: null });
  });

  it.each([
    [{ from: "2026-10-14T12:00:00Z", to: "2026-10-14T11:00:00Z" }, /not be after/],
    [{ from: "2026-10-01T00:00:00Z", to: "2026-10-14T00:00:00Z" }, /at most 7 days/],
    [{ limit: String(MAX_PAGE_SIZE + 1) }, /limit/],
    [{ from: "yesterday" }, /from/],
    [{ cursor: "not-a-cursor" }, /cursor/],
  ])("rejects %j", (raw, message) => {
    const result = parseReadingQuery(raw, now);
    expect(result).toHaveProperty("error");
    if ("error" in result) expect(result.error).toMatch(message);
  });

  it("accepts a cursor it issued", () => {
    const cursor = { measuredAt: "2026-10-14T11:30:00.000Z", epoch: 2, seq: 40 };
    expect(parseReadingQuery({ cursor: encodeCursor(cursor) }, now)).toMatchObject({ after: cursor });
  });
});

describe("cursors", () => {
  it("round-trip", () => {
    const cursor = { measuredAt: "2026-10-14T11:30:00.000Z", epoch: 2, seq: 40 };
    expect(decodeCursor(encodeCursor(cursor))).toEqual(cursor);
  });

  it("reject tampered input", () => {
    expect(decodeCursor(Buffer.from('{"epoch":"x"}').toString("base64url"))).toBeNull();
  });
});
