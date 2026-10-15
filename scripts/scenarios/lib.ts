/**
 * Shared helpers for the failure scenarios: drive the Compose stack, cut the database through toxiproxy, read
 * the broker's `$SYS` counters, compare fleet-sim's PUBACK ledger with what Postgres stored, read container memory,
 * crash a replica, and print PASS/FAIL lines.
 * Each scenario talks to the stack from the host, through the ports Compose publishes on 127.0.0.1.
 */
import { execFile, execFileSync } from "node:child_process";
import { promisify } from "node:util";
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { configSchema as fleetSimConfig } from "../../packages/fleet-sim/src/config.ts";
import { join } from "node:path";
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import pg from "pg";

export { sleep };

const execFileAsync = promisify(execFile);

const ROOT = join(import.meta.dirname, "..", "..");
const BASE_COMPOSE = join(ROOT, "deploy", "compose.yaml");

/** One check's verdict. */
export interface Verdict {
  check: string;
  pass: boolean;
  detail: string;
  /**
   * Set when the check couldn't be judged: the VM wasn't healthy before the fault (see {@link waitForQuietBaseline}).
   * The check is then INVALID, never PASS or FAIL, and the reason is printed with it.
   */
  invalid?: string;
  /**
   * Set for a documented limit the scenario demonstrates rather than judges: printed as KNOWN LIMIT with what was
   * seen, and never counted as a PASS or a FAIL.
   */
  known?: string;
}

/** Exit status of a scenario whose checks all passed or were INVALID, with at least one INVALID (demo.sh reads it). */
export const EXIT_INVALID = 2;

/** Runs `docker compose` with the base file plus any `extraFiles`, in the repo root; returns stdout. */
export function compose(args: string[], extraFiles: string[] = [], env: Record<string, string> = {}): string {
  const files = [BASE_COMPOSE, ...extraFiles.map((f) => join(ROOT, f))].flatMap((f) => ["-f", f]);
  return execFileSync("docker", ["compose", ...files, ...args], { cwd: ROOT, encoding: "utf8", env: { ...process.env, ...env }, maxBuffer: 256 * 1024 * 1024 });
}

/** Turns the toxiproxy route between ingest and Postgres on or off. Off means ingest's database is "down". */
export async function setDatabaseReachable(reachable: boolean): Promise<void> {
  await setProxyEnabled("postgres", reachable);
}

/**
 * Makes the database hang for ingest instead of refusing: a toxiproxy `timeout` toxic holds every connection open and
 * passes nothing, so new connects and queries time out rather than failing fast (S7-9). `false` removes it.
 */
export async function setDatabaseHung(hung: boolean): Promise<void> {
  const url = "http://127.0.0.1:8474/proxies/postgres/toxics";
  if (hung) {
    const response = await fetch(url, { method: "POST", body: JSON.stringify({ name: "hang", type: "timeout", stream: "upstream", attributes: { timeout: 0 } }) });
    if (!response.ok && response.status !== 409) throw new Error(`toxiproxy: ${response.status} ${await response.text()}`);
  } else {
    await fetch(`${url}/hang`, { method: "DELETE" });
  }
}

/** Turns one toxiproxy route on or off (`postgres`, `wan`, `wan-cuttable`, `gateway-wan`). */
export async function setProxyEnabled(proxy: string, enabled: boolean): Promise<void> {
  const response = await fetch(`http://127.0.0.1:8474/proxies/${proxy}`, { method: "POST", body: JSON.stringify({ enabled }) });
  if (!response.ok) throw new Error(`toxiproxy: ${response.status} ${await response.text()}`);
}

/**
 * Reads the broker's `$SYS` values. They're published every `sys_interval` (10 s), so this waits until every topic in
 * `wanted` has been seen, up to 15 s.
 */
export async function readSys(wanted: string[]): Promise<Map<string, number>> {
  const client = await connectWithRetry(30_000);
  const values = new Map<string, number>();
  client.on("message", (topic, payload) => values.set(topic, Number(payload.toString())));
  await client.subscribeAsync(wanted, { qos: 0 });
  const deadline = Date.now() + 15_000;
  while (wanted.some((t) => !values.has(t)) && Date.now() < deadline) await sleep(100);
  await client.endAsync();
  return values;
}

/** The `ops` certificate (CN=ops, read-only on $SYS by the ACL) that the scenarios use to read broker statistics. */
function opsTls(): { ca: Buffer; cert: Buffer; key: Buffer; rejectUnauthorized: true } {
  const dir = join(ROOT, "deploy", "certs");
  return { ca: readFileSync(join(dir, "ca.crt")), cert: readFileSync(join(dir, "ops.crt")), key: readFileSync(join(dir, "ops.key")), rejectUnauthorized: true };
}

/**
 * Connects a throwaway client to the broker, retrying every 500 ms for up to `timeoutMs`. Right after a broker
 * restart the port refuses connections for a moment, and a single attempt would leave the scenario hanging.
 */
async function connectWithRetry(timeoutMs: number): Promise<MqttClient> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const client = connect("mqtts://127.0.0.1:8883", { protocolVersion: 5, clientId: `scenario-sys-${process.pid}-${Date.now()}`, reconnectPeriod: 0, ...opsTls() });
    const connected = await new Promise<boolean>((resolve) => {
      client.once("connect", () => resolve(true));
      client.once("close", () => resolve(false));
    });
    if (connected) return client;
    client.end(true);
    if (Date.now() > deadline) throw new Error("broker not reachable");
    await sleep(500);
  }
}

export const SYS_DROPPED = "$SYS/broker/publish/messages/dropped";
export const SYS_STORED = "$SYS/broker/store/messages/count";
export const SYS_HEAP = "$SYS/broker/heap/current";

/** One device's acknowledged readings, as fleet-sim's ledger reports them. */
interface LedgerEntry {
  deviceId: string;
  epoch: number;
  ranges: Array<[number, number]>;
  count: number;
}

/**
 * Fetches `url` and returns its JSON body. Any status other than 2xx throws, with the method, URL, status and the
 * start of the body in the message, so a 503 from the API reads as what it is, not as a missing field later (S7-11).
 */
export async function getJson<T>(url: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(url, init);
  const text = await response.text();
  if (!response.ok) throw new Error(`${init.method ?? "GET"} ${url} answered ${response.status}: ${text.slice(0, 300)}`);
  return JSON.parse(text) as T;
}

/** The API's base URL and bearer token, as the lab stack sets them. */
export const API = "http://127.0.0.1:8080";
export const API_AUTH = { authorization: `Bearer ${process.env.API_TOKEN ?? "dev-token-change-me-0123"}` };

/** Calls the API at `path` with the token; throws on a non-2xx answer (see {@link getJson}). */
export function apiGet<T>(path: string): Promise<T> {
  return getJson<T>(`${API}${path}`, { headers: API_AUTH });
}

/** Fetches fleet-sim's PUBACK ledger: every reading the broker acknowledged. */
export async function fetchLedger(): Promise<LedgerEntry[]> {
  return (await getJson<{ devices: LedgerEntry[] }>("http://127.0.0.1:8090/ledger")).devices;
}

/** How many status messages the broker has acknowledged to fleet-sim, in total. */
export async function fetchAckedStatuses(): Promise<number> {
  return (await getJson<{ statuses: { total: number } }>("http://127.0.0.1:8090/ledger")).statuses.total;
}

/** Evaluates a PromQL expression now and returns its first value, or NaN if there's none. */
export async function promQuery(expr: string): Promise<number> {
  const body = await getJson<{ data: { result: Array<{ value: [number, string] }> } }>(`http://127.0.0.1:9090/api/v1/query?query=${encodeURIComponent(expr)}`);
  return Number(body.data.result[0]?.value[1] ?? NaN);
}

/**
 * Runs a scenario's `main`. If it throws (an API error, a lost database, a bug in the script), that's printed as a
 * FAIL with the reason, in the same shape as {@link finish}'s, and the exit status is 1: a scenario that couldn't
 * finish never looks like a crash with a stack trace to read, and never like a pass (S7-11).
 */
export async function run(main: () => Promise<void>): Promise<void> {
  try {
    await main();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stdout.write(`\nFAIL  the scenario could not complete  (${message})\n`);
    if (error instanceof Error && error.stack) process.stderr.write(`${error.stack}\n`);
    process.stdout.write("scenario: FAIL (did not complete)\n");
    process.exit(1);
  }
}

/** A pool connected directly to Postgres (not through toxiproxy), for checking what was stored. */
export function openDatabase(): pg.Pool {
  const pool = new pg.Pool({ connectionString: "postgres://fleet:fleet-dev-only@127.0.0.1:5432/fleet", max: 2 });
  pool.on("error", (error) => process.stderr.write(`scenario database connection lost: ${error.message}\n`));
  return pool;
}

/**
 * Counts how many of the ledger's readings are stored. The primary key forbids duplicates, so stored == expected
 * means every acknowledged reading is stored exactly once.
 */
export async function storedOfLedger(pool: pg.Pool, ledger: LedgerEntry[]): Promise<{ expected: number; stored: number }> {
  const rows = ledger.flatMap((e) => e.ranges.map(([lo, hi]) => [e.deviceId, e.epoch, lo, hi] as const));
  const { rows: result } = await pool.query<{ expected: string | null; stored: string }>(
    `WITH r AS (SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[]) AS r(device_id, epoch, lo, hi))
     SELECT (SELECT sum(hi - lo + 1) FROM r)::bigint AS expected,
            (SELECT count(*) FROM readings x JOIN r ON x.device_id = r.device_id AND x.epoch = r.epoch AND x.seq BETWEEN r.lo AND r.hi)::bigint AS stored`,
    [rows.map((r) => r[0]), rows.map((r) => r[1]), rows.map((r) => r[2]), rows.map((r) => r[3])],
  );
  return { expected: Number(result[0]?.expected ?? 0), stored: Number(result[0]?.stored ?? 0) };
}

/**
 * Lists up to `limit` acknowledged readings that Postgres doesn't have, with their gateway and when the reading before
 * them was received. That tells a broker-restart window (S3-9) from random loss.
 */
export async function missingReadings(pool: pg.Pool, limit = 20): Promise<Array<{ deviceId: string; epoch: number; seq: number; previousReceivedAt: string | null }>> {
  const ledger = await fetchLedger();
  const rows = ledger.flatMap((e) => e.ranges.map(([lo, hi]) => [e.deviceId, e.epoch, lo, hi] as const));
  const { rows: missing } = await pool.query<{ device_id: string; epoch: string; seq: number; previous_received_at: Date | null }>(
    `WITH r AS (SELECT * FROM unnest($1::text[], $2::bigint[], $3::bigint[], $4::bigint[]) AS r(device_id, epoch, lo, hi)),
          expected AS (SELECT r.device_id, r.epoch, s.seq FROM r CROSS JOIN LATERAL generate_series(r.lo, r.hi) AS s(seq))
     SELECT e.device_id, e.epoch::text, e.seq::int,
            (SELECT max(x.received_at) FROM readings x WHERE x.device_id = e.device_id AND x.epoch = e.epoch AND x.seq < e.seq) AS previous_received_at
     FROM expected e LEFT JOIN readings x USING (device_id, epoch, seq)
     WHERE x.seq IS NULL ORDER BY e.device_id, e.seq LIMIT $5`,
    [rows.map((r) => r[0]), rows.map((r) => r[1]), rows.map((r) => r[2]), rows.map((r) => r[3]), limit]);
  return missing.map((m) => ({ deviceId: m.device_id, epoch: Number(m.epoch), seq: m.seq, previousReceivedAt: m.previous_received_at?.toISOString() ?? null }));
}

/**
 * Takes a ledger snapshot, then waits up to `timeoutMs` for Postgres to hold all of it. Returns the final counts. The
 * snapshot is fixed first, so readings acknowledged later don't move the target.
 */
export async function waitForLedgerStored(pool: pg.Pool, timeoutMs: number): Promise<{ expected: number; stored: number; waitedMs: number }> {
  const ledger = await fetchLedger();
  const started = Date.now();
  let counts = await storedOfLedger(pool, ledger);
  while (counts.stored < counts.expected && Date.now() - started < timeoutMs) {
    await sleep(2000);
    counts = await storedOfLedger(pool, ledger);
  }
  return { ...counts, waitedMs: Date.now() - started };
}

/**
 * A container's memory use in MB, from `docker stats` (the cgroup's view, as a memory limit sees it). Asynchronous:
 * `docker stats` takes seconds on Docker Desktop, and a synchronous call would freeze the scenario's own timers and
 * measurements while it runs (S6-6).
 */
export async function containerMemoryMb(service: string): Promise<number> {
  const id = compose(["ps", "-q", service]).trim();
  const { stdout } = await execFileAsync("docker", ["stats", "--no-stream", "--format", "{{.MemUsage}}", id], { encoding: "utf8" });
  const usage = stdout.split("/")[0]?.trim() ?? "";
  const match = /([\d.]+)\s*([KMG]i?B)/.exec(usage);
  if (!match) throw new Error(`can't parse memory "${usage}"`);
  const factor = { KiB: 1 / 1024, KB: 1 / 1000, MiB: 1, MB: 1, GiB: 1024, GB: 1000 }[match[2] as string] ?? 1;
  return Number(match[1]) * factor;
}

/** How many times Docker has restarted a service's container. */
export function restartCount(service: string): number {
  const id = compose(["ps", "-q", service]).trim();
  return Number(execFileSync("docker", ["inspect", "--format", "{{.RestartCount}}", id], { encoding: "utf8" }).trim());
}

/** True if the container was killed for running out of memory. */
export function oomKilled(service: string): boolean {
  const id = compose(["ps", "-q", service]).trim();
  return execFileSync("docker", ["inspect", "--format", "{{.State.OOMKilled}}", id], { encoding: "utf8" }).trim() === "true";
}

/**
 * SIGKILLs the Node process inside a service's container, like a crash. PID 1 is Docker's init, which ignores SIGKILL
 * sent from inside, and `docker kill` counts as a manual stop that skips `restart: always`. So this kills the child.
 */
export function crashNode(service: string): void {
  compose(["exec", "-T", service, "sh", "-c",
    'for p in /proc/[0-9]*; do id=${p#/proc/}; [ "$id" = 1 ] && continue; tr "\\0" " " < $p/cmdline 2>/dev/null | grep -q "^node " && kill -9 $id; done']);
}

/** One `summary` log line from ingest (see `ActivityCounters`). */
export interface IngestSummary {
  time: number;
  received: number;
  redelivered: number;
  inserted: number;
  duplicate: number;
  retries: number;
  waiting: number;
  unacked: number;
}

/** The `summary` lines a service logged since `sinceIso`. */
export function ingestSummaries(service: string, sinceIso: string): IngestSummary[] {
  return compose(["logs", "--no-log-prefix", "--since", sinceIso, service])
    .split("\n")
    .filter((line) => line.includes('"msg":"summary"'))
    .map((line) => JSON.parse(line) as IngestSummary);
}

/** Ingest lag over a recent window, by receive time: p95 and max in seconds. */
export async function lagSeconds(pool: pg.Pool, sinceIso: string, untilIso: string): Promise<{ p95: number; max: number }> {
  const { rows } = await pool.query<{ p95: number | null; max: number | null }>(
    `SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - measured_at)) AS p95,
            max(extract(epoch FROM received_at - measured_at))::float8 AS max
     FROM readings WHERE received_at BETWEEN $1 AND $2`, [sinceIso, untilIso]);
  return { p95: Number(rows[0]?.p95 ?? 0), max: Number(rows[0]?.max ?? 0) };
}

/** p95 and max of received − measured for readings selected by `where` (on `measured_at`), received after the run started. */
export async function lagByMeasured(pool: pg.Pool, where: string, params: string[]): Promise<{ p95: number; max: number; n: number }> {
  const { rows } = await pool.query<{ p95: number | null; max: number | null; n: number }>(
    `SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - measured_at)) AS p95,
            max(extract(epoch FROM received_at - measured_at))::float8 AS max, count(*)::int AS n
     FROM readings WHERE ${where} AND received_at > now() - interval '5 minutes'`, params);
  return { p95: Number(rows[0]?.p95 ?? 0), max: Number(rows[0]?.max ?? 0), n: rows[0]?.n ?? 0 };
}

/**
 * Prints the verdicts and a summary line. Exits 0 only if every check passed; 1 if any failed; {@link EXIT_INVALID}
 * if none failed but at least one couldn't be judged. A FAIL outranks an INVALID.
 */
export function finish(scenario: string, verdicts: Verdict[], measurements: Record<string, unknown>): never {
  process.stdout.write(`\n=== ${scenario} ===\n`);
  for (const v of verdicts) {
    const state = v.known ? "KNOWN LIMIT" : v.invalid ? "INVALID" : v.pass ? "PASS" : "FAIL";
    process.stdout.write(`${state}  ${v.check}  (${v.known ? `${v.known}; ` : ""}${v.detail}${v.invalid ? `; ${v.invalid}` : ""})\n`);
  }
  process.stdout.write(`measured: ${JSON.stringify(measurements)}\n`);
  const failed = verdicts.filter((v) => !v.known && !v.invalid && !v.pass).length;
  const invalid = verdicts.filter((v) => !v.known && v.invalid).length;
  if (failed) process.stdout.write(`${scenario}: FAIL (${failed} of ${verdicts.length})\n`);
  else if (invalid) process.stdout.write(`${scenario}: INVALID (${invalid} of ${verdicts.length} couldn't be judged)\n`);
  else process.stdout.write(`${scenario}: PASS\n`);
  process.exit(failed ? 1 : invalid ? EXIT_INVALID : 0);
}

/** The lag bar the recovery checks use, and the baseline the VM must show before a fault (S7-7). */
export const PROMPT_LAG_P95_SECONDS = 2;

/**
 * Waits, up to `maxMs`, until the API reports its gap window warm (`fleet_health_gaps_warm` = 1). Waiting for the
 * system to say it's ready is part of a scenario's setup, not of its judgement: right after a cold start the API's
 * warm-up competes with everything else, and a baseline measured during it measures the start-up (S7-7, S7-11).
 * Returns how long it waited, and whether the API got there.
 */
export async function waitForApiWarm(maxMs = 300_000): Promise<{ warm: boolean; waitedSeconds: number }> {
  const started = Date.now();
  for (;;) {
    const warm = (await promQuery("max(fleet_health_gaps_warm)").catch(() => NaN)) === 1;
    const waitedSeconds = Math.round((Date.now() - started) / 1000);
    if (warm || Date.now() - started >= maxMs) return { warm, waitedSeconds };
    await sleep(5000);
  }
}

/**
 * Samples the VM's CPU pressure (PSI "some" avg10, the whole Docker VM's, read inside a running container) every 5 s
 * until stopped, and keeps the peak: the stall rule's first piece of evidence (stallRule.ts). Asynchronous, so the
 * sampling never blocks the scenario's own timers (S6-6).
 */
export function samplePsi(container = "fleet-ingest-postgres-1"): { stop: () => number } {
  let peak = 0;
  let running = true;
  void (async () => {
    while (running) {
      const { stdout } = await execFileAsync("docker", ["exec", container, "cat", "/proc/pressure/cpu"], { encoding: "utf8" }).catch(() => ({ stdout: "" }));
      const avg10 = Number(/some avg10=([\d.]+)/.exec(stdout)?.[1] ?? NaN);
      if (Number.isFinite(avg10)) peak = Math.max(peak, avg10);
      await sleep(5000);
    }
  })();
  return { stop: () => { running = false; return peak; } };
}

/** How many gateways the broker timed out for missed keepalives since `sinceIso`: the stall rule's broker evidence. */
export function brokerKeepaliveTimeouts(sinceIso: string): number {
  return compose(["logs", "--no-log-prefix", "--since", sinceIso, "mosquitto"]).split("\n").filter((line) => line.includes("exceeded timeout")).length;
}

/** What {@link waitForQuietBaseline} saw. */
export interface Baseline {
  quiet: boolean;
  /** p95 lag of the last 30 s window measured. */
  p95: number;
  waitedSeconds: number;
}

/**
 * Waits, up to `maxMs`, for 30 s of readings with p95 lag under {@link PROMPT_LAG_P95_SECONDS}: proof that the VM is
 * healthy before a fault is injected (S7-7). The window ends 10 s ago, so readings measured in it have had time to
 * arrive. A check that judges recovery against that bar is only meaningful after a quiet baseline: without one, the
 * check is INVALID (see {@link gatedBy}), and the p95 that was measured is recorded. The gate doesn't hide slowness
 * after the fault; it only refuses to judge recovery on a machine that was already stalling before it.
 */
export async function waitForQuietBaseline(pool: pg.Pool, maxMs = 180_000): Promise<Baseline> {
  const started = Date.now();
  for (;;) {
    const end = Date.now() - 10_000;
    const lag = await lagByMeasured(pool, "measured_at > $1 AND measured_at < $2", [new Date(end - 30_000).toISOString(), new Date(end).toISOString()]);
    const waitedSeconds = Math.round((Date.now() - started) / 1000);
    if (lag.n > 0 && lag.p95 < PROMPT_LAG_P95_SECONDS) return { quiet: true, p95: lag.p95, waitedSeconds };
    if (Date.now() - started >= maxMs) return { quiet: false, p95: lag.p95, waitedSeconds };
    await sleep(5000);
  }
}

/** Marks `verdict` INVALID when `baseline` wasn't quiet; otherwise returns it unchanged. */
export function gatedBy(baseline: Baseline, verdict: Verdict): Verdict {
  if (baseline.quiet) return verdict;
  return { ...verdict, invalid: `no quiet baseline: p95 ${baseline.p95.toFixed(2)} s after waiting ${baseline.waitedSeconds} s` };
}

/** Logs a progress line with the time since the scenario started. */
export function progress(started: number, message: string): void {
  process.stdout.write(`[+${((Date.now() - started) / 1000).toFixed(0)}s] ${message}\n`);
}

/** How long 500 TLS gateways take to come up on the lab VM when every first connect succeeds (measured, S6-6). */
const FLEET_START_UP_MS = 180_000;

/**
 * How long {@link ensureFleet} waits for every gateway, from the settings the simulator itself uses, so the two can't
 * drift (S7-11). A gateway whose first connect fails waits out its connect timeout, then a reconnect delay of up to
 * `RECONNECT_JITTER_MAX_MS` (fleet-sim applies the jitter to the first retry too, as a fleet powering on after an
 * outage would), and then needs the same start-up time as everyone else. So the budget is start-up + connect timeout
 * + the jitter maximum: 190 s with no jitter, 490 s for scenario 4's 0–300 s. The connect timeout is fleet-sim's
 * default, because Compose doesn't pass `CONNECT_TIMEOUT_MS` through; the jitter is what this script passes it.
 */
export function fleetStartBudgetMs(env: NodeJS.ProcessEnv = process.env): number {
  const sim = fleetSimConfig.pick({ CONNECT_TIMEOUT_MS: true, RECONNECT_JITTER_MAX_MS: true }).parse({ RECONNECT_JITTER_MAX_MS: env.RECONNECT_JITTER_MAX_MS });
  return FLEET_START_UP_MS + sim.CONNECT_TIMEOUT_MS + sim.RECONNECT_JITTER_MAX_MS;
}

/**
 * Makes sure every certificate exists, brings the stack up with `gateways` virtual gateways (recreating fleet-sim if
 * the count changed), and waits until readings from every gateway are arriving, up to {@link fleetStartBudgetMs}.
 */
export async function ensureFleet(pool: pg.Pool, gateways: number, extraFiles: string[] = []): Promise<void> {
  execFileSync(join(ROOT, "scripts", "dev-certs.sh"), [String(gateways)], { stdio: "ignore" });
  compose(["up", "-d", "--build"], extraFiles, { GATEWAY_COUNT: String(gateways), ...(process.env.RECONNECT_JITTER_MAX_MS ? { RECONNECT_JITTER_MAX_MS: process.env.RECONNECT_JITTER_MAX_MS } : {}), ...(process.env.BATCH_MAX_ROWS ? { BATCH_MAX_ROWS: process.env.BATCH_MAX_ROWS } : {}) });
  const budgetMs = fleetStartBudgetMs();
  const deadline = Date.now() + budgetMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{ n: number }>(
      `SELECT count(DISTINCT d.gateway_id)::int AS n FROM devices d JOIN device_state s USING (device_id) WHERE s.received_at > now() - interval '15 seconds'`).catch(() => ({ rows: [{ n: 0 }] }));
    if ((rows[0]?.n ?? 0) >= gateways) return;
    await sleep(3000);
  }
  throw new Error(`fewer than ${gateways} gateways delivering after ${Math.round(budgetMs / 1000)} s`);
}

/** Changes fleet-sim's chaos knobs (S7-5); unspecified knobs keep their value. Returns the settings now in force. */
export async function setChaos(settings: { duplicateRate?: number; reorderWindow?: number; poisonRate?: number }): Promise<unknown> {
  const response = await fetch("http://127.0.0.1:8090/chaos/settings", { method: "POST", body: JSON.stringify(settings) });
  if (!response.ok) throw new Error(`fleet-sim chaos: ${response.status} ${await response.text()}`);
  return response.json();
}

/**
 * Waits until alert-logger has received `alertname` with `status` (firing or resolved) at or after `sinceIso`.
 * Returns when it arrived, or null after `timeoutMs`. Alerts take their `for:` plus Alertmanager's group wait.
 */
export async function waitForAlert(alertname: string, status: "firing" | "resolved", sinceIso: string, timeoutMs: number): Promise<string | null> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const body = (await (await fetch("http://127.0.0.1:9095/alerts")).json().catch(() => ({ alerts: [] }))) as { alerts: Array<{ status: string; labels: Record<string, string>; receivedAt: string }> };
    const hit = body.alerts.find((a) => a.labels.alertname === alertname && a.status === status && a.receivedAt >= sinceIso);
    if (hit) return hit.receivedAt;
    await sleep(5000);
  }
  return null;
}
