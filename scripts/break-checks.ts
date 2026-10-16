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
    edits: [{ find: "WINDOW per_epoch AS (PARTITION BY r.epoch ORDER BY r.seq, r.measured_at)", replace: "WINDOW per_epoch AS (ORDER BY r.seq, r.measured_at)" }],
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
  {
    name: "worker-lock-always-granted",
    rule: "Only one health worker runs the job at a time: the second waits on standby (S8-3)",
    injects: "every worker believes it holds the lock, so two export every fleet counter",
    file: "packages/api/src/workerLock.ts",
    edits: [{ find: "  return rows[0]?.locked === true;", replace: "  return true;" }],
    project: "integration",
    test: "packages/api/test/workerLock.integration.test.ts",
  },
  {
    name: "worker-lock-loss-unchecked",
    rule: "A worker whose lock session dies finds out within one check and stops (S8-3)",
    injects: "the leader never re-checks its lock, so it keeps running after its session is gone",
    file: "packages/api/src/workerLock.ts",
    edits: [{ find: "  const timer = setInterval(() => void check(), everyMs);", replace: "  const timer = setInterval(() => undefined, everyMs);" }],
    project: "integration",
    test: "packages/api/test/workerLock.integration.test.ts",
  },
  {
    name: "snapshot-not-published",
    rule: "Each gaps pass is published to health_snapshot, which /fleet reads (S8-3)",
    injects: "the worker computes gaps but never writes the snapshot, so /fleet freezes",
    file: "packages/api/src/healthJob.ts",
    edits: [{ find: "        if (publish) await timed(metrics, \"snapshot\", () => publish(tracker.last));\n", replace: "" }],
    project: "integration",
    test: "packages/api/test/api.integration.test.ts",
  },
  {
    name: "pass-histogram-not-observed",
    rule: "Every health-job pass is recorded in a histogram, so a slow one can't fall between scrapes (S8-1, S8-3)",
    injects: "only the last pass is kept, as in S8-1, where a 6.13 s pass went unseen",
    file: "packages/api/src/healthJob.ts",
    edits: [{ find: "    metrics.passDuration.labels(part).observe(seconds);\n", replace: "" }],
    project: "unit",
    test: "packages/api/test/databasePool.test.ts",
  },
  {
    name: "zombie-leader-writes",
    rule: "A leader that lost its lock can't overwrite the new leader's snapshot: the write is fenced (S8-3)",
    injects: "the snapshot write no longer checks the lock, so a zombie's stale pass overwrites the new leader's",
    file: "packages/api/src/healthSnapshot.ts",
    edits: [{
      find: "     WHERE EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()\n                     AND classid = 0 AND objid = $4 AND objsubid = 1 AND granted)\n",
      replace: "     WHERE $4::int IS NOT NULL\n",
    }],
    project: "integration",
    test: "packages/api/test/healthSnapshot.integration.test.ts",
  },
  {
    name: "checkpoint-ignores-dropped",
    rule: "A gap checkpoint is recomputed when droppedTelemetry changes (S8-4)",
    injects: "a bigger drop counter resumes the old budget, so gaps it now explains stay unexplained",
    file: "packages/api/src/core/gapCheckpoint.ts",
    edits: [{ find: "  if (checkpoint.droppedTelemetry !== context.droppedTelemetry) return \"dropped_changed\";\n", replace: "" }],
    project: "unit",
    test: "packages/api/test/gapCheckpoint.test.ts",
  },
  {
    name: "checkpoint-ignores-retention",
    rule: "A gap checkpoint is recomputed when retention drops a partition (S8-4)",
    injects: "gaps dropped with their partition keep the budget they used",
    file: "packages/api/src/core/gapCheckpoint.ts",
    edits: [{ find: "  if (!sameTime(checkpoint.retainedFrom, context.retainedFrom)) return \"retention\";\n", replace: "" }],
    project: "unit",
    test: "packages/api/test/gapCheckpoint.test.ts",
  },
  {
    name: "checkpoint-misses-gap-behind",
    rule: "A reading whose predecessor was measured before the checkpoint invalidates it (S8-4)",
    injects: "a replay filling a gap that straddles the checkpoint, or a device back from a long silence, resumes a stale budget",
    file: "packages/api/src/gapCheckpointStore.ts",
    edits: [{ find: "      if (rows[0]?.gap_behind) return \"gap_behind\";\n", replace: "" }],
    project: "integration",
    test: "packages/api/test/gapCheckpoint.integration.test.ts",
  },
  {
    name: "checkpoint-misses-measured-behind",
    rule: "A reading measured before the checkpoint invalidates it (S8-4)",
    injects: "a series that starts before the checkpoint never gets its gap explained in order",
    file: "packages/api/src/gapCheckpointStore.ts",
    edits: [{ find: "      if (rows[0]?.measured_behind) return \"measured_behind\";\n", replace: "" }],
    project: "integration",
    test: "packages/api/test/gapCheckpoint.integration.test.ts",
  },
  {
    name: "gaps-from-by-time",
    rule: "The gaps since a checkpoint are read by seq from the first reading after it, not by time (S8-4)",
    injects: "after a clock steps back, a gap whose second reading is older than the checkpoint is missed",
    file: "packages/api/src/gapCheckpointStore.ts",
    edits: [{ find: "FROM readings r WHERE r.device_id = d.device_id AND r.epoch = s.epoch AND r.seq >= s.from_seq", replace: "FROM readings r WHERE r.device_id = d.device_id AND r.epoch = s.epoch AND r.seq >= s.from_seq AND r.measured_at >= $2" }],
    project: "integration",
    test: "packages/api/test/gapCheckpoint.integration.test.ts",
  },
  {
    name: "kick-before-crl-confirmed",
    rule: "A revoked gateway is kicked only after the broker is seen to refuse its certificate (S8-5)",
    injects: "the kick runs right after SIGHUP, before the CRL reload is confirmed, so the gateway can reconnect",
    file: "scripts/gatewayAdmin.ts",
    edits: [{ find: "  await steps.reloadCrl();\n", replace: "  await steps.reloadCrl();\n  await steps.disableClient(cn);\n" }],
    project: "unit",
    test: "scripts/gatewayAdmin.test.ts",
  },
  {
    name: "dynsec-defaults-allow",
    rule: "With dynsec loaded, the ACL file stays authoritative: dynsec's defaults deny (S8-5)",
    injects: "dynsec allows by default, and since either plugin can grant, a gateway may publish on another's topic",
    file: "deploy/mosquitto/dynamic-security.seed.json",
    edits: [{ find: "    \"publishClientSend\": false,\n", replace: "    \"publishClientSend\": true,\n" }],
    project: "integration",
    test: "packages/ingest/test/brokerSecurity.integration.test.ts",
  },
  {
    name: "ca-key-mounted",
    rule: "No container mounts the CA key: services mount only their own certificate files (S8-5)",
    injects: "the API mounts the whole certificate directory, CA key included, as it did before S8-5",
    file: "deploy/compose.yaml",
    edits: [{ find: "    volumes: [\"./certs/ca.crt:/certs/ca.crt:ro\", \"./certs/api.crt:/certs/api.crt:ro\", \"./certs/api.key:/certs/api.key:ro\"]", replace: "    volumes: [\"./certs:/certs:ro\"]" }],
    project: "unit",
    test: "scripts/certMounts.test.ts",
  },
  {
    name: "crl-expiry-misread",
    rule: "The exporter reads the CRL's nextUpdate, the date after which every client is refused (S8-7)",
    injects: "the exporter reports thisUpdate as the expiry, so an expiring CRL looks freshly issued, or already dead",
    file: "packages/broker-exporter/src/core/crl.ts",
    edits: [{ find: "  const next = fields[i + 1];", replace: "  const next = fields[i];" }],
    project: "unit",
    test: "packages/broker-exporter/test/crl.test.ts",
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
