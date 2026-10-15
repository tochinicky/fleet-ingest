/**
 * Scenario 2: 10% of the gateways lose their internet for 2 minutes.
 *
 * Every 10th gateway reaches the broker through its own toxiproxy route (`wan-cuttable`). The cut is a `timeout` toxic
 * with no close: packets just stop, like a dead WAN link, not a clean disconnect. The broker notices through the
 * gateway's 15 s keepalive (×1.5) and publishes its Last Will. The gateways buffer their readings meanwhile, and replay
 * them in order when the link comes back.
 *
 * PASS means:
 * - all 50 cut gateways are reported offline within about 25 s;
 * - they're all online again after the link returns;
 * - every reading the broker acknowledged is stored exactly once;
 * - their devices have no gaps over the window;
 * - the replayed readings arrived as backfill (ingest lag > 60 s).
 *
 * The GatewayOffline alert itself arrives with Prometheus in slice 6.
 * Usage: `node scripts/scenarios/gateways-offline.ts`
 */
import { apiGet, ensureFleet, finish, openDatabase, progress, run, sleep, waitForLedgerStored } from "./lib.ts";

const GATEWAYS = 500;
const CUT_EVERY = 10;
const CUT_SECONDS = 120;

/** Adds or removes the "dead link" toxics on the cuttable route, in both directions. */
async function setLinkCut(cut: boolean): Promise<void> {
  for (const stream of ["upstream", "downstream"]) {
    const url = `http://127.0.0.1:8474/proxies/wan-cuttable/toxics`;
    const response = cut
      ? await fetch(url, { method: "POST", body: JSON.stringify({ name: `dead-${stream}`, type: "timeout", stream, attributes: { timeout: 0 } }) })
      : await fetch(`${url}/dead-${stream}`, { method: "DELETE" });
    if (!response.ok && !(response.status === 404 && !cut)) throw new Error(`toxiproxy: ${response.status} ${await response.text()}`);
  }
}

/** The fleet's gateway counts from the API. */
async function fleetGateways(): Promise<{ online: number; offline: number; unknown: number }> {
  return (await apiGet<{ gateways: { online: number; offline: number; unknown: number } }>("/fleet")).gateways;
}

/** Polls /fleet until `predicate` holds or `timeoutMs` passes; returns the elapsed ms, or Infinity on timeout. */
async function timeUntil(predicate: (g: { online: number; offline: number }) => boolean, timeoutMs: number): Promise<number> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (predicate(await fleetGateways())) return Date.now() - started;
    await sleep(1000);
  }
  return Infinity;
}

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  const cutCount = GATEWAYS / CUT_EVERY;
  await setLinkCut(false).catch(() => undefined);
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await timeUntil((g) => g.online === GATEWAYS, 120_000);
  await sleep(10_000);

  const cutAt = new Date();
  progress(started, `cutting the internet of ${cutCount} gateways for ${CUT_SECONDS} s`);
  await setLinkCut(true);
  const detectMs = await timeUntil((g) => g.offline >= cutCount, 90_000);
  progress(started, `all ${cutCount} reported offline after ${(detectMs / 1000).toFixed(1)} s`);
  await sleep(Math.max(0, CUT_SECONDS * 1000 - (Date.now() - cutAt.getTime())));
  await setLinkCut(false);
  const restoredAt = new Date();
  progress(started, "link restored");
  const recoverMs = await timeUntil((g) => g.online === GATEWAYS, 180_000);
  progress(started, `all ${GATEWAYS} online again after ${(recoverMs / 1000).toFixed(1)} s`);

  const ledger = await waitForLedgerStored(pool, 180_000);
  const cutGateways = Array.from({ length: cutCount }, (_, i) => `sim-gw-${String((i + 1) * CUT_EVERY).padStart(4, "0")}`);
  const { rows } = await pool.query<{ gaps: number; backfill: number }>(`
    WITH ordered AS (
      SELECT r.seq - lag(r.seq) OVER (PARTITION BY r.device_id, r.epoch ORDER BY r.seq) AS step
      FROM readings r JOIN devices d USING (device_id)
      WHERE d.gateway_id = ANY($1) AND r.measured_at BETWEEN $2::timestamptz - interval '1 minute' AND $3)
    SELECT (SELECT count(*) FROM ordered WHERE step > 1)::int AS gaps,
           (SELECT count(*) FROM readings r JOIN devices d USING (device_id)
             WHERE d.gateway_id = ANY($1) AND r.received_at >= $3 AND r.received_at - r.measured_at > interval '60 seconds')::int AS backfill`,
    [cutGateways, cutAt, restoredAt]);
  await pool.end();
  const result = rows[0] ?? { gaps: -1, backfill: 0 };

  finish(`scenario 2: ${cutCount} of ${GATEWAYS} gateways offline for ${CUT_SECONDS} s`, [
    { check: "the cut gateways are reported offline within ~25 s", pass: detectMs <= 30_000, detail: `${(detectMs / 1000).toFixed(1)} s (keepalive 15 s × 1.5 = 22.5 s, plus polling)` },
    { check: "all gateways online again after the link returns", pass: Number.isFinite(recoverMs), detail: `${(recoverMs / 1000).toFixed(1)} s` },
    { check: "every acknowledged reading stored exactly once (backlog drained)", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}` },
    { check: "no gaps for the cut gateways' devices over the window", pass: result.gaps === 0, detail: `${result.gaps} gaps` },
    { check: "the replayed readings arrived as backfill (lag > 60 s)", pass: result.backfill > 0, detail: `${result.backfill} backfilled readings` },
  ], { detectSeconds: +(detectMs / 1000).toFixed(1), recoverSeconds: +(recoverMs / 1000).toFixed(1), ledger, ...result });
}

await run(main);
