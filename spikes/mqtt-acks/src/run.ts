/**
 * Runs the slice 0 spike checks one after another against throwaway Mosquitto 2.1.2 containers, prints a go/no-go table,
 * and writes every observation to `results/results.json` as evidence.
 * Usage: `pnpm spike` runs all checks; `pnpm spike <id> [<id>...]` runs only the named ones.
 */
import { mkdirSync, writeFileSync, existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type { Check, CheckResult } from "./check.ts";
import { deferredHoldKeepsKeepalive, deferredPubackAllowsBatching, documentedHoldBlocksReceiving, documentedHoldStallsKeepalive } from "./checks/ackHolding.ts";
import { unackedRedeliveredAfterResume } from "./checks/redelivery.ts";
import { brokerHonoursReceiveMaximum, inflightIsMinOfReceiveMaximumAndBroker } from "./checks/receiveMaximum.ts";
import { connectedStalledQueueBound, offlineQueueBound, sysTopicsAvailable } from "./checks/queueBound.ts";
import { crashLosesSinceLastSnapshot, queueSurvivesCleanRestart } from "./checks/persistence.ts";
import { sharedSubscriptionFailover } from "./checks/sharedSubscription.ts";
import { sharedBacklogSurvivesRestart, subscriptionsSurviveRestart } from "./checks/subscriptionRestore.ts";
import { memoryAndSnapshotAt240k } from "./checks/memory.ts";
import { droppedCounterRetainedStatuses, droppedCounterTwoStalledMembers } from "./checks/droppedCounter.ts";
import { sqliteCostAt240k, sqliteCrashWindow, sqliteRestoresSharedSubscriptions } from "./checks/persistSqlite.ts";

/** Every check, in the order the go/no-go reads them: stop tier first, then escalate, then record. */
const CHECKS: Check[] = [
  deferredPubackAllowsBatching,
  deferredHoldKeepsKeepalive,
  unackedRedeliveredAfterResume,
  brokerHonoursReceiveMaximum,
  queueSurvivesCleanRestart,
  crashLosesSinceLastSnapshot,
  documentedHoldBlocksReceiving,
  documentedHoldStallsKeepalive,
  inflightIsMinOfReceiveMaximumAndBroker,
  offlineQueueBound,
  connectedStalledQueueBound,
  sysTopicsAvailable,
  sharedSubscriptionFailover,
  subscriptionsSurviveRestart,
  sharedBacklogSurvivesRestart,
  memoryAndSnapshotAt240k,
  droppedCounterTwoStalledMembers,
  droppedCounterRetainedStatuses,
  sqliteRestoresSharedSubscriptions,
  sqliteCrashWindow,
  sqliteCostAt240k,
];

/** Runs one check and turns a thrown error into a failed result instead of aborting the whole run. */
async function runOne(check: Check): Promise<CheckResult> {
  const base = { id: check.id, tier: check.tier, proves: check.proves };
  try {
    return { ...base, ...(await check.run()) };
  } catch (error) {
    return { ...base, pass: false, observed: { error: error instanceof Error ? error.message : String(error) } };
  }
}

/** Formats a result's verdict for the table: PASS, FAIL, or RECORD for measurement-only checks. */
function verdict(result: CheckResult): string {
  if (result.pass === null) return "RECORD";
  return result.pass ? "PASS" : "FAIL";
}

/** Merges new results into any existing results file, so running a single check doesn't wipe the others. */
function saveResults(results: CheckResult[]): string {
  const dir = join(import.meta.dirname, "..", "results");
  const file = join(dir, "results.json");
  mkdirSync(dir, { recursive: true });
  const existing: Record<string, CheckResult> = existsSync(file) ? JSON.parse(readFileSync(file, "utf8")) : {};
  for (const result of results) existing[result.id] = result;
  writeFileSync(file, JSON.stringify(existing, null, 2) + "\n");
  return file;
}

/** Entry point: picks the checks, runs them in sequence, prints the table, saves the evidence. */
async function main(): Promise<void> {
  const wanted = process.argv.slice(2);
  const selected = wanted.length ? CHECKS.filter((check) => wanted.includes(check.id)) : CHECKS;
  const results: CheckResult[] = [];
  for (const check of selected) {
    process.stdout.write(`… ${check.id}\n`);
    const result = await runOne(check);
    results.push(result);
    process.stdout.write(`${verdict(result).padEnd(6)} [${result.tier}] ${result.id}\n${JSON.stringify(result.observed)}\n\n`);
  }
  process.stdout.write(`results: ${saveResults(results)}\n`);
  const stopFailures = results.filter((r) => r.tier === "stop" && r.pass === false);
  process.exit(stopFailures.length ? 1 : 0);
}

await main();
