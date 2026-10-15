/**
 * Scenario 9: the real .NET gateway end to end, with a 2-minute WAN cut. The `with-gateway` profile runs
 * home-energy-gateway's gateway and simulated home against this broker over mutual TLS; its internet is toxiproxy's
 * `gateway-wan` route, which this scenario cuts.
 *
 * PASS means the gateway's data is complete in Postgres:
 * - every device the gateway reports has readings measured during the cut (buffered on the gateway, replayed after);
 * - the API reports no gap for any of its devices across the window;
 * - the gateway's buffer is back to 0 and its dropped counter didn't move (nothing was dropped to make room);
 * - while cut, the gateway went offline (its Last Will), and it's online again after.
 *
 * Usage: `node scripts/scenarios/real-gateway.ts` (issues the gateway's certificate and starts the profile itself).
 */
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import type pg from "pg";
import { API, API_AUTH, apiGet, compose, finish, openDatabase, progress, run, setProxyEnabled, sleep } from "./lib.ts";

const GATEWAY = "gw-001";
const CUT_SECONDS = 120;
const ROOT = join(import.meta.dirname, "..", "..");
const PROFILE = ["deploy/compose.with-gateway.yaml"];

/** The gateway's row: its buffer and drop counters, and when ingest last heard an offline status from it. */
interface GatewaySnapshot {
  buffered: number | null;
  dropped: number | null;
  lastOfflineReceivedAt: Date | null;
}

/** Reads the gateway's counters and its latest offline time. */
async function snapshot(pool: pg.Pool): Promise<GatewaySnapshot | null> {
  const { rows } = await pool.query<GatewaySnapshot>(
    `SELECT buffered::int, dropped::int, last_offline_received_at AS "lastOfflineReceivedAt" FROM gateways WHERE gateway_id = $1`, [GATEWAY]);
  return rows[0] ?? null;
}

/** The gateway's devices with readings in the last 30 s; waits up to `timeoutMs` for at least one. */
async function liveDevices(pool: pg.Pool, timeoutMs: number): Promise<string[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const { rows } = await pool.query<{ device_id: string }>(
      `SELECT DISTINCT r.device_id FROM readings r JOIN devices d USING (device_id)
       WHERE d.gateway_id = $1 AND r.received_at > now() - interval '30 seconds' ORDER BY 1`, [GATEWAY]);
    if (rows.length > 0) return rows.map((r) => r.device_id);
    await sleep(5000);
  }
  return [];
}

/** Readings per device measured in [from, to]. */
async function measuredBetween(pool: pg.Pool, devices: string[], from: Date, to: Date): Promise<Map<string, number>> {
  const { rows } = await pool.query<{ device_id: string; n: number }>(
    `SELECT device_id, count(*)::int AS n FROM readings WHERE device_id = ANY($1) AND measured_at BETWEEN $2 AND $3 GROUP BY 1`, [devices, from, to]);
  return new Map(devices.map((d) => [d, rows.find((r) => r.device_id === d)?.n ?? 0]));
}

/** The gateway's health state from the API. */
async function healthState(): Promise<string> {
  const response = await fetch(`${API}/gateways/${GATEWAY}`, { headers: API_AUTH });
  return response.ok ? ((await response.json()) as { health: { state: string } }).health.state : `http ${response.status}`;
}

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  if (!existsSync(join(ROOT, "deploy", "certs", `${GATEWAY}.crt`))) {
    execFileSync(join(ROOT, "scripts", "pki.sh"), ["issue", join(ROOT, "deploy", "certs"), GATEWAY], { stdio: "ignore" });
  }
  progress(started, "starting the with-gateway profile (the first build of the .NET images takes a few minutes)");
  compose(["--profile", "with-gateway", "up", "-d", "--build"], PROFILE);
  await setProxyEnabled("gateway-wan", true);
  const pool = openDatabase();
  const devices = await liveDevices(pool, 600_000);
  if (devices.length === 0) throw new Error(`${GATEWAY} stored no readings within 10 minutes; check \`docker compose logs gateway\``);
  progress(started, `${GATEWAY} is delivering for ${devices.length} devices: ${devices.join(", ")}`);
  await sleep(30_000);
  const before = await snapshot(pool);
  const windowStart = new Date(Date.now() - 60_000);

  const cutAt = new Date();
  progress(started, `cutting ${GATEWAY}'s WAN for ${CUT_SECONDS} s`);
  await setProxyEnabled("gateway-wan", false);
  await sleep(CUT_SECONDS * 1000 - 30_000);
  const stateDuringCut = await healthState();
  await sleep(30_000);
  const restoredAt = new Date();
  progress(started, `restoring the WAN (health during the cut: ${stateDuringCut})`);
  await setProxyEnabled("gateway-wan", true);

  const deadline = Date.now() + 300_000;
  let after = await snapshot(pool);
  let replayed = await measuredBetween(pool, devices, new Date(restoredAt.getTime() - 10_000), restoredAt);
  while (Date.now() < deadline && (after?.buffered !== 0 || [...replayed.values()].some((n) => n === 0) || (after?.lastOfflineReceivedAt ?? 0) <= (before?.lastOfflineReceivedAt ?? 0))) {
    await sleep(5000);
    after = await snapshot(pool);
    replayed = await measuredBetween(pool, devices, new Date(restoredAt.getTime() - 10_000), restoredAt);
  }
  await sleep(20_000);
  after = await snapshot(pool);
  const duringCut = await measuredBetween(pool, devices, new Date(cutAt.getTime() + 5_000), new Date(restoredAt.getTime() - 5_000));
  const now = new Date();
  const gapCounts = await Promise.all(devices.map(async (device) => {
    const report = await apiGet<{ gaps: unknown[] }>(`/devices/${device}/gaps?from=${windowStart.toISOString()}&to=${now.toISOString()}`);
    return report.gaps.length;
  }));
  const stateAfter = await healthState();
  await pool.end();

  finish(`scenario 9: the real gateway, ${CUT_SECONDS} s WAN cut`, [
    { check: "every device has readings measured during the cut (buffered, then replayed)", pass: [...duringCut.values()].every((n) => n > 0), detail: [...duringCut].map(([d, n]) => `${d}: ${n}`).join(", ") },
    { check: "no gap for any device across the window", pass: gapCounts.every((n) => n === 0), detail: `gaps per device: ${gapCounts.join(", ")}` },
    { check: "the gateway's buffer drained and it dropped nothing", pass: after?.buffered === 0 && after?.dropped === before?.dropped, detail: `buffered ${after?.buffered}, dropped ${before?.dropped} → ${after?.dropped}` },
    { check: "it went offline during the cut (Last Will) and is online after", pass: stateDuringCut === "offline" && stateAfter === "online", detail: `during: ${stateDuringCut}, after: ${stateAfter}` },
  ], { gateway: GATEWAY, devices, cutSeconds: CUT_SECONDS, cutAt: cutAt.toISOString(), restoredAt: restoredAt.toISOString(), duringCut: Object.fromEntries(duringCut) });
}

await run(main);
