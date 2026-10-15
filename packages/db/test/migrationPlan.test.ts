/**
 * Unit tests for the pure migration plan: ordering, and the refusals that protect history.
 */
import { describe, expect, it } from "vitest";
import { MigrationPlanError, parseMigrationFiles, pendingMigrations } from "../src/migrationPlan.ts";

const files = [
  { name: "002_second.sql", sql: "B" },
  { name: "001_first.sql", sql: "A" },
  { name: "003_third.sql", sql: "C" },
];

describe("parseMigrationFiles", () => {
  it("sorts by version", () => {
    expect(parseMigrationFiles(files).map((m) => m.version)).toEqual([1, 2, 3]);
  });

  it.each(["1_first.sql", "001-first.sql", "001_First.sql", "001_first.SQL"])("rejects the name %s", (name) => {
    expect(() => parseMigrationFiles([{ name, sql: "" }])).toThrow(MigrationPlanError);
  });

  it("rejects two files with the same version", () => {
    expect(() => parseMigrationFiles([{ name: "001_a.sql", sql: "" }, { name: "001_b.sql", sql: "" }])).toThrow(/share version 1/);
  });
});

describe("pendingMigrations", () => {
  const all = parseMigrationFiles(files);

  it("returns everything on a fresh database", () => {
    expect(pendingMigrations(all, []).map((m) => m.version)).toEqual([1, 2, 3]);
  });

  it("returns only what's new", () => {
    expect(pendingMigrations(all, [1, 2]).map((m) => m.version)).toEqual([3]);
  });

  it("returns nothing when up to date", () => {
    expect(pendingMigrations(all, [1, 2, 3])).toEqual([]);
  });

  it("refuses when an applied version has no file", () => {
    expect(() => pendingMigrations(all, [1, 2, 3, 4])).toThrow(/no file: 4/);
  });

  it("refuses to run a migration older than the newest applied", () => {
    expect(() => pendingMigrations(all, [1, 3])).toThrow(/002_second.sql/);
  });
});
