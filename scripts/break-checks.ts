/**
 * Deliberate-break checks: for each rule the design depends on, this script breaks the rule in the source on
 * purpose, runs the test that is supposed to catch it, and requires that test to FAIL. Then it restores the file.
 * A break that the tests don't notice means the rule is unprotected, and the script exits non-zero.
 *
 * Usage: `node scripts/break-checks.ts` for all checks, or `node scripts/break-checks.ts <name>...` for some.
 * Integration-tier checks need Docker.
 */
import { spawnSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

/** One edit of a deliberate break: the exact text to find (it must occur once) and what to put there instead. */
interface Edit {
  find: string;
  replace: string;
}

/**
 * One deliberate break: which file to change, the edits that break the rule, and which test must then fail. `injects`
 * names the bug the edits introduce, by meaning: when a restructuring changes the code they match, re-target the
 * edits to inject that same bug into the new code, and keep the same test (S7-11).
 */
interface Break {
  name: string;
  rule: string;
  injects: string;
  file: string;
  edits: Edit[];
  project: "unit" | "integration";
  test: string;
}

const ROOT = join(import.meta.dirname, "..");

/** Every break, oldest rule first. `find` must occur exactly once in `file`. */
const BREAKS: Break[] = [
  {
    name: "ack-before-write",
    rule: "A message is acknowledged only after its batch commits (D3)",
    injects: "messages are acknowledged before their batch commits",
    file: "packages/ingest/src/ingestPipeline.ts",
    edits: [{
      find: "    const batch = batcher.take();\n    try {",
      replace: "    const batch = batcher.take();\n    batch.forEach((pending) => pending.delivery.ack());\n    try {",
    }, {
      find: "        pending.delivery.ack();\n",
      replace: "",
    }],
    project: "unit",
    test: "packages/ingest/test/pipeline.test.ts",
  },
  {
    name: "stale-acks-sent",
    rule: "Acks from a closed connection are discarded (S0-1)",
    injects: "acks held from a closed connection are sent on the new one",
    file: "packages/ingest/src/adapters/ackGate.ts",
    edits: [{ find: "    return token.generation !== this.#generation;", replace: "    return false;" }],
    project: "integration",
    test: "packages/ingest/test/ackGate.integration.test.ts",
  },
  {
    name: "insert-without-on-conflict",
    rule: "A reading is stored once per (device_id, epoch, seq, measured_at) (D4, S8-2)",
    injects: "a redelivered reading is inserted again instead of absorbed",
    file: "packages/ingest/src/adapters/postgresStore.ts",
    edits: [{ find: "  ON CONFLICT (device_id, epoch, seq, measured_at) DO NOTHING\n", replace: "" }],
    project: "integration",
    test: "packages/ingest/test/exactlyOnce.property.integration.test.ts",
  },
  {
    name: "device-state-not-newer-only",
    rule: "device_state only moves forward (D4)",
    injects: "device_state is overwritten by an older reading",
    file: "packages/ingest/src/adapters/postgresStore.ts",
    edits: [{ find: "  WHERE (excluded.measured_at, excluded.epoch, excluded.seq) > (device_state.measured_at, device_state.epoch, device_state.seq)`;", replace: "`;" }],
    project: "integration",
    test: "packages/ingest/test/exactlyOnce.property.integration.test.ts",
  },
  {
    name: "status-not-newer-only",
    rule: "Gateway status only moves forward (S2-2)",
    injects: "gateway status is overwritten by an older status",
    file: "packages/ingest/src/adapters/postgresStore.ts",
    edits: [{
      find: 'const STATUS_IS_NEWER = "(gateways.last_status_at IS NULL OR excluded.last_status_at > gateways.last_status_at)";',
      replace: 'const STATUS_IS_NEWER = "true";',
    }],
    project: "integration",
    test: "packages/ingest/test/postgresStore.integration.test.ts",
  },
  {
    name: "future-timestamp-accepted",
    rule: "A timestamp more than 5 min in the future is dead-lettered (D7)",
    injects: "a reading timestamped far in the future is stored",
    file: "packages/ingest/src/core/routeMessage.ts",
    edits: [{ find: "  return aheadMs > MAX_FUTURE_SKEW_MS ?", replace: "  return false ?" }],
    project: "unit",
    test: "packages/ingest/test/core.test.ts",
  },
  {
    name: "foreign-device-accepted",
    rule: "A device bound to one gateway can't be written by another (D6)",
    injects: "a gateway can write another gateway's device",
    file: "packages/ingest/src/adapters/postgresStore.ts",
    edits: [
      { find: "  const accepted = readings.filter((r) => bindings.get(r.deviceId) === r.gatewayId);", replace: "  const accepted = readings;" },
      { find: "  const bound = bindings.get(routed.row.deviceId);", replace: "  const bound = routed.row.gatewayId;" },
    ],
    project: "integration",
    test: "packages/ingest/test/postgresStore.integration.test.ts",
  },
  {
    name: "poison-fails-whole-batch",
    rule: "A poison row is isolated by halving; the rest of the batch commits",
    injects: "one poison row fails its whole batch",
    file: "packages/ingest/src/core/isolatePoison.ts",
    edits: [{
      find: "    if (items.length === 1) return [await ports.onPoison(items[0] as T, error)];",
      replace: "    return Promise.all(items.map((item) => ports.onPoison(item, error)));",
    }],
    project: "unit",
    test: "packages/ingest/test/pipeline.test.ts",
  },
  {
    name: "subscribe-only-once",
    rule: "Ingest subscribes again on every connect; the broker drops shared subscriptions on restart (S3-9)",
    injects: "ingest subscribes only on its first connection",
    file: "packages/ingest/src/adapters/mqttSubscriber.ts",
    edits: [{ find: '    client.on("connect", () => void subscribe(client)', replace: '    client.once("connect", () => void subscribe(client)' }],
    project: "integration",
    test: "packages/ingest/test/brokerRestart.integration.test.ts",
  },
  {
    name: "acl-gateway-writes-anywhere",
    rule: "A gateway can publish only under its own ID (D6 ACL)",
    injects: "a gateway can publish under any gateway's topic",
    file: "deploy/mosquitto/acl",
    edits: [{ find: "pattern write homes/%u/devices/+/telemetry", replace: "pattern write homes/+/devices/+/telemetry" }],
    project: "integration",
    test: "packages/ingest/test/brokerSecurity.integration.test.ts",
  },
  {
    name: "certificate-not-required",
    rule: "Every client must present a certificate from the lab CA (D6 mTLS)",
    injects: "a client without a lab-CA certificate can connect",
    file: "deploy/mosquitto/mosquitto.conf",
    edits: [
      { find: "require_certificate true", replace: "require_certificate false" },
      { find: "allow_anonymous false", replace: "allow_anonymous true" },
    ],
    project: "integration",
    test: "packages/ingest/test/brokerSecurity.integration.test.ts",
  },
  {
    name: "gaps-across-epochs",
    rule: "Gaps are found per (device, epoch); seq restarts after a power loss",
    injects: "seq compared across epochs (a power loss looks like a gap)",
    file: "packages/api/src/queries.ts",
    edits: [{ find: "WINDOW per_epoch AS (PARTITION BY r.epoch ORDER BY r.seq)", replace: "WINDOW per_epoch AS (ORDER BY r.seq)" }],
    project: "integration",
    test: "packages/api/test/api.integration.test.ts",
  },
  {
    name: "gaps-explained-in-window",
    rule: "droppedTelemetry is a lifetime counter: gaps are explained against all-time gaps, then filtered (S5-9)",
    injects: "gaps explained against the window instead of all-time history",
    file: "packages/api/src/healthReport.ts",
    edits: [{ find: "  const since = droppedTelemetry > 0 ? new Date(0) : from;", replace: "  const since = from;" }],
    project: "integration",
    test: "packages/api/test/api.integration.test.ts",
  },
  {
    name: "pool-timeout-is-fatal",
    rule: "A pool connect timeout during an outage is retryable, so ingest retries instead of exiting (S7-9)",
    injects: "a pool connect timeout is classified fatal, so ingest exits",
    file: "packages/ingest/src/core/errors.ts",
    edits: [{ find: "|timeout exceeded when trying to connect", replace: "" }],
    project: "unit",
    test: "packages/ingest/test/core.test.ts",
  },
  {
    name: "driver-message-reworded",
    rule: "The classifier's connection-loss messages match what the pinned driver really says (S7-9 canary)",
    injects: "the classifier no longer matches the driver's real message",
    file: "packages/ingest/src/core/errors.ts",
    edits: [{ find: "|timeout expired|", replace: "|timeout has expired|" }],
    project: "unit",
    test: "packages/ingest/test/driverMessages.test.ts",
  },
  {
    name: "checked-out-client-unguarded",
    rule: "A connection lost while a batch holds it rejects the batch for the retry instead of crashing ingest (S7-10)",
    injects: "a checked-out client has no error listener, so a dropped socket crashes ingest",
    file: "packages/ingest/src/adapters/databasePool.ts",
    edits: [{ find: '  client.on("error", swallow);\n', replace: "" }],
    project: "integration",
    test: "packages/ingest/test/postgresStore.integration.test.ts",
  },
  {
    name: "failed-client-reused",
    rule: "A connection whose batch failed (a query timeout, say) is destroyed and replaced, never reused (S7-10)",
    injects: "a client whose query failed goes back to the pool and is reused",
    file: "packages/ingest/src/adapters/databasePool.ts",
    edits: [{ find: "    client.release(error instanceof Error ? error : new Error(String(error)));\n", replace: "    client.release();\n" }],
    project: "unit",
    test: "packages/ingest/test/databasePool.test.ts",
  },
  {
    name: "database-timeouts-removed",
    rule: "Ingest's connections bound a slow database (statement_timeout) and a silent one (query_timeout) (S7-10)",
    injects: "ingest's queries are unbounded, so a hang holds a batch forever",
    file: "packages/ingest/src/adapters/databasePool.ts",
    edits: [
      { find: "    statement_timeout: options.statementTimeoutMs ?? STATEMENT_TIMEOUT_MS,\n", replace: "" },
      { find: "    query_timeout: options.queryTimeoutMs ?? QUERY_TIMEOUT_MS,\n", replace: "" },
    ],
    project: "unit",
    test: "packages/ingest/test/databasePool.test.ts",
  },
  {
    name: "api-timeouts-removed",
    rule: "The API's queries are bounded, so a hung one can't hold a pool slot and freeze the fleet metrics (S7-10)",
    injects: "the API's queries are unbounded, so a hung one holds a pool slot",
    file: "packages/api/src/databasePool.ts",
    edits: [
      { find: "    statement_timeout: STATEMENT_TIMEOUT_MS,\n", replace: "" },
      { find: "    query_timeout: QUERY_TIMEOUT_MS,\n", replace: "" },
    ],
    project: "unit",
    test: "packages/api/test/databasePool.test.ts",
  },
  {
    name: "cursor-without-overlap",
    rule: "Each gap pass re-reads behind its cursor, so a batch that committed late (older stored_at) isn't missed (S7-11)",
    injects: "the cursor reads strictly after itself, missing late-committed rows",
    file: "packages/api/src/core/gapWindow.ts",
    edits: [{ find: "return cursor === null ? null : cursor - overlapMs;", replace: "return cursor;" }],
    project: "unit",
    test: "packages/api/test/gapWindow.test.ts",
  },
  {
    name: "replay-does-not-close-gap",
    rule: "A reading that arrives after its successors (a replay, a reorder) fills its gap in the window (S7-11)",
    injects: "a reading that arrives after its successors is ignored, so its gap stays open",
    file: "packages/api/src/core/gapWindow.ts",
    edits: [{ find: "      if (series.seqs[at] === row.seq) {", replace: "      if (series.seqs[at] === row.seq || at < series.seqs.length) {" }],
    project: "unit",
    test: "packages/api/test/gapWindow.test.ts",
  },
  {
    name: "gap-window-across-epochs",
    rule: "The incremental window keeps each (device, epoch) apart: seq restarts after a power loss (S7-11)",
    injects: "the window compares seq across epochs",
    file: "packages/api/src/core/gapWindow.ts",
    edits: [{ find: "      const key = `${row.deviceId}\\u0000${row.epoch}`;", replace: "      const key = row.deviceId;" }],
    project: "unit",
    test: "packages/api/test/gapWindow.test.ts",
  },
  {
    name: "reconciler-not-on-connect",
    rule: "Desired modes are published again from Postgres on every broker connection, so a fresh broker gets them back (S7-1)",
    injects: "desired modes aren't republished when the broker connection comes back",
    file: "packages/api/src/commandReconciler.ts",
    edits: [{ find: "  options.commands.onConnect(() => background(false));\n", replace: "" }],
    project: "integration",
    test: "packages/api/test/commandReconciler.integration.test.ts",
  },
  {
    name: "too-old-accepted",
    rule: "A reading measured before what retention keeps is dead-lettered too_old, before any write (S8-2)",
    injects: "readings older than retention are routed to the store, where no partition holds them",
    file: "packages/ingest/src/core/routeMessage.ts",
    edits: [{ find: "  if (row.measuredAt < oldest) {", replace: "  if (false) {" }],
    project: "unit",
    test: "packages/ingest/test/core.test.ts",
  },
  {
    name: "no-partition-is-poison",
    rule: "A missing partition is an operator mistake: the batch retries, nothing is dead-lettered (S8-2)",
    injects: "a reading with no partition is dead-lettered as poison",
    file: "packages/ingest/src/core/errors.ts",
    edits: [{ find: "error.message : \"\")) return \"retryable\";", replace: "error.message : \"\")) return \"poison\";" }],
    project: "unit",
    test: "packages/ingest/test/core.test.ts",
  },
  {
    name: "accepted-since-off-by-a-day",
    rule: "Ingest accepts from T - (D - 1), a whole day inside the kept partitions, whatever the clock (S8-2)",
    injects: "ingest accepts readings from the oldest kept day, which maintenance may already have dropped",
    file: "packages/db/src/partitions.ts",
    edits: [{ find: "-(retentionDays - 1)", replace: "-retentionDays" }],
    project: "unit",
    test: "packages/db/test/partitions.test.ts",
  },
  {
    name: "key-reuse-unnoticed",
    rule: "A key reused with another measured_at is stored twice, and counted and alerted (S8-2)",
    injects: "a reused key is stored as a second row with nothing noticing",
    file: "packages/api/src/core/gapWindow.ts",
    edits: [{ find: "        if (series.measuredAt[at] !== row.measuredAt) this.noteReuse(key, row);\n", replace: "" }],
    project: "unit",
    test: "packages/api/test/gapWindow.test.ts",
  },
];

/** Applies one break, runs its test, restores the file, and reports whether the test caught it (i.e. failed). */
function runBreak(check: Break): boolean {
  const path = join(ROOT, check.file);
  const original = readFileSync(path, "utf8");
  let broken = original;
  for (const edit of check.edits) {
    const occurrences = broken.split(edit.find).length - 1;
    if (occurrences !== 1) throw new Error(`${check.name}: expected the text once in ${check.file}, found it ${occurrences} times`);
    broken = broken.replace(edit.find, edit.replace);
  }
  try {
    writeFileSync(path, broken);
    const result = spawnSync("corepack", ["pnpm", "vitest", "run", "--project", check.project, check.test], { cwd: ROOT, encoding: "utf8" });
    return result.status !== 0;
  } finally {
    writeFileSync(path, original);
  }
}

/** Runs one test file on the unbroken code; true if it passes. */
function testPasses(project: Break["project"], test: string): boolean {
  return spawnSync("corepack", ["pnpm", "vitest", "run", "--project", project, test], { cwd: ROOT, encoding: "utf8" }).status === 0;
}

/**
 * Runs the selected breaks and prints one line each; exits 1 if any break went unnoticed.
 * First, every test file involved must pass on the unbroken code. Otherwise a failure under a break could have any
 * cause (Docker down, a full disk) and "caught" would mean nothing, so the script stops instead.
 */
function main(): void {
  const wanted = process.argv.slice(2);
  const selected = wanted.length ? BREAKS.filter((b) => wanted.includes(b.name)) : BREAKS;
  const baselines = new Map<string, Break["project"]>(selected.map((b) => [b.test, b.project]));
  const failingBaselines = [...baselines].filter(([test, project]) => !testPasses(project, test)).map(([test]) => test);
  if (failingBaselines.length) {
    process.stdout.write(`BASELINE FAILS, results would be meaningless: ${failingBaselines.join(", ")}\n`);
    process.exit(2);
  }
  let unnoticed = 0;
  for (const check of selected) {
    const caught = runBreak(check);
    if (!caught) unnoticed++;
    process.stdout.write(`${caught ? "CAUGHT    " : "UNNOTICED "} ${check.name.padEnd(28)} ${check.rule}  (${check.test})\n`);
  }
  process.exit(unnoticed ? 1 : 0);
}

main();
