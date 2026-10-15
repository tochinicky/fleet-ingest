/**
 * The stall rule's truth table (S7-11): a failed recovery check becomes INVALID only when the VM's CPU pressure, the
 * untouched replica and the broker all show the stall. Any one or two alone change nothing, and a pass stays a pass.
 */
import { describe, expect, it } from "vitest";
import { gatedByStall, replicaStalled, stallExcuses } from "./stallRule.ts";
import type { StallEvidence } from "./stallRule.ts";

const steady = [1938, 2000, 1828, 2100, 1950];
const stalled = [1938, 1083, 513, 0, 271, 681];
const evidence = (psi: number, timeouts: number, samples: number[]): StallEvidence => ({ psiAvg10Peak: psi, brokerTimeouts: timeouts, untouched: { name: "ingest-2", samples } });
const failed = { check: "lag recovered", pass: false, detail: "p95 29.63 s" };

describe("replicaStalled", () => {
  it.each([
    [stalled, true],
    [[129, 141, 187, 213, 138, 14], true],
    [steady, false],
    [[1200, 900, 1100, 700], false],
    [[], false],
    [[0, 0, 0], false],
  ])("%j → %s", (samples, expected) => {
    expect(replicaStalled(samples)).toBe(expected);
  });
});

describe("stallExcuses: all three or nothing", () => {
  it.each([
    ["PSI, broker and replica", evidence(86, 367, stalled), true],
    ["exactly at the thresholds", evidence(60, 10, stalled), true],
    ["PSI just under", evidence(59.9, 367, stalled), false],
    ["broker quiet", evidence(86, 9, stalled), false],
    ["replica kept going", evidence(86, 367, steady), false],
    ["only PSI", evidence(86, 0, steady), false],
    ["only the broker (it could be our broker)", evidence(20, 500, steady), false],
    ["only the replica", evidence(20, 0, stalled), false],
  ])("%s → %s", (_name, e, expected) => {
    expect(stallExcuses(e)).toBe(expected);
  });
});

describe("gatedByStall", () => {
  it("turns an excused failure into INVALID and prints the evidence", () => {
    const verdict = gatedByStall(evidence(86, 367, [129, 141, 187, 213, 138, 14]), failed);
    expect(verdict.invalid).toBe("the VM stalled during the check: PSI avg10 86%, broker timed out 367 gateways, ingest-2 down to 14");
  });

  it("leaves a pass a pass, and an unexcused failure a failure", () => {
    expect(gatedByStall(evidence(86, 367, stalled), { ...failed, pass: true }).invalid).toBeUndefined();
    expect(gatedByStall(evidence(86, 3, stalled), failed).invalid).toBeUndefined();
  });
});
