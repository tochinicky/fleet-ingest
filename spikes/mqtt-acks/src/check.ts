/**
 * The shape every spike check returns, and the go/no-go tier each check belongs to (slice 0).
 */

/**
 * Where a check sits in the slice 0 go/no-go:
 * - `stop`: a failure breaks the core guarantee. The build stops until it is resolved.
 * - `escalate`: a failure breaks a D3 premise. It needs a decision before D3 is built; slices 1–2 may continue.
 * - `record`: a measurement or behaviour to write down and design around. There's no pass bar.
 */
export type Tier = "stop" | "escalate" | "record";

/** The outcome of one check: whether it passed (null for record-only checks) and what it observed. */
export interface CheckResult {
  id: string;
  tier: Tier;
  proves: string;
  pass: boolean | null;
  observed: Record<string, unknown>;
}

/** A named check that the runner can call on its own. */
export interface Check {
  id: string;
  tier: Tier;
  proves: string;
  run: () => Promise<Omit<CheckResult, "id" | "tier" | "proves">>;
}
