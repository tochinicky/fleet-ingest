/**
 * When a stall during a scenario excuses a failed recovery check (S7-7, S7-11). The quiet-baseline gate covers a VM
 * that was unhealthy *before* the fault; this covers one that stalls *during* the judged window, as the 4 GB Docker VM
 * does under the 500-gateway stack. Pure, so its truth table is unit-tested.
 *
 * Only environment evidence counts, and all of it must agree:
 * - the VM was starved: CPU pressure (PSI "some" avg10) reached {@link STALL_PSI_AVG10_PERCENT};
 * - the replica the scenario didn't touch stalled too ({@link replicaStalled});
 * - the broker timed out gateways for missed keepalives ({@link STALL_MIN_BROKER_TIMEOUTS} or more).
 * Any one alone could be this system's own fault (an overloaded broker times gateways out too), so none alone excuses
 * anything.
 *
 * And it only ever applies to **recovery and throughput** checks: lag recovered, the other replica kept storing, a
 * replica receives again. Data checks (lost, stored, dropped, counted ≥ lost) are never gated: a stall can excuse
 * slowness, never loss.
 */
import type { Verdict } from "./lib.ts";

/** CPU pressure at which the VM counts as starved: the level of every whole-fleet stall seen in the lab. */
export const STALL_PSI_AVG10_PERCENT = 60;

/** Keepalive timeouts the broker must have logged in the window: more than a few flaky clients. */
export const STALL_MIN_BROKER_TIMEOUTS = 10;

/** A sample counts as stalled below this share of the replica's median: 0 and 14/s were the stalls seen. */
const STALLED_SHARE_OF_MEDIAN = 0.2;

/** What was measured during a check's judged window. */
export interface StallEvidence {
  /** Highest PSI "some" avg10 sampled during the window, in percent. */
  psiAvg10Peak: number;
  /** Keepalive timeouts ("exceeded timeout") the broker logged during the window. */
  brokerTimeouts: number;
  /** The replica the scenario didn't touch, and its throughput samples over the window. */
  untouched: { name: string; samples: number[] };
}

/** True if any sample fell below 20% of the samples' median: the replica stopped, it didn't just slow down. */
export function replicaStalled(samples: number[]): boolean {
  if (samples.length === 0) return false;
  const sorted = [...samples].sort((a, b) => a - b);
  const median = sorted[Math.floor(sorted.length / 2)] as number;
  const lowest = sorted[0] as number;
  return median > 0 && lowest < STALLED_SHARE_OF_MEDIAN * median;
}

/** True only when all three kinds of environment evidence agree that the VM stalled. */
export function stallExcuses(evidence: StallEvidence): boolean {
  return evidence.psiAvg10Peak >= STALL_PSI_AVG10_PERCENT
    && evidence.brokerTimeouts >= STALL_MIN_BROKER_TIMEOUTS
    && replicaStalled(evidence.untouched.samples);
}

/** The evidence as the verdict line prints it. */
export function describeStall(evidence: StallEvidence): string {
  const lowest = Math.min(...evidence.untouched.samples);
  return `PSI avg10 ${evidence.psiAvg10Peak.toFixed(0)}%, broker timed out ${evidence.brokerTimeouts} gateways, ${evidence.untouched.name} down to ${lowest.toFixed(0)}`;
}

/**
 * A recovery or throughput check, judged with the stall rule: a pass stays a pass; a failure becomes INVALID only if
 * {@link stallExcuses}, with the evidence in its line. Never wrap a data check in this.
 */
export function gatedByStall(evidence: StallEvidence, verdict: Verdict): Verdict {
  if (verdict.pass || verdict.invalid || !stallExcuses(evidence)) return verdict;
  return { ...verdict, invalid: `the VM stalled during the check: ${describeStall(evidence)}` };
}
