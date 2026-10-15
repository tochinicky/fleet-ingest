/**
 * Does a backlog queued in ingest's shared-subscription sessions survive a clean broker restart (decisions.md
 * S3-9)? Mosquitto restores the sessions without their shared subscriptions. If it also lost
 * or stranded their queued messages, the D3 guarantee ("no loss unless the broker crashes with a backlog") would be
 * wrong for a clean restart.
 *
 * Steps (500 gateways, ~400 msg/s):
 * 1. Cut Postgres for ingest until about 40k messages are queued (~20k per ingest session).
 * 2. Restart the broker cleanly; it writes its persistence (persist-sqlite, or the snapshot fallback) and loads it on start.
 * 3. Bring Postgres back.
 *
 * PASS means every reading the broker acknowledged (fleet-sim's PUBACK ledger) is stored exactly once. The `$SYS` drop
 * counter resets on restart, so the ledger is the only ground truth here.
 * Usage: `node scripts/scenarios/broker-restart-with-backlog.ts`
 */
import {
  compose, ensureFleet, finish, openDatabase, progress, readSys, run, setDatabaseReachable, sleep, SYS_STORED,
  waitForLedgerStored,
} from "./lib.ts";

const GATEWAYS = 500;
const TARGET_BACKLOG = 40_000;

/** Waits until the broker holds at least `target` stored messages; returns the count reached. */
async function waitForBacklog(target: number, timeoutMs: number): Promise<number> {
  const deadline = Date.now() + timeoutMs;
  let stored = 0;
  while (stored < target && Date.now() < deadline) {
    await sleep(5000);
    stored = (await readSys([SYS_STORED])).get(SYS_STORED) ?? 0;
  }
  return stored;
}

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  await setDatabaseReachable(true).catch(() => undefined);
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await setDatabaseReachable(true);
  await sleep(20_000);

  progress(started, "cutting Postgres for ingest until the backlog builds");
  await setDatabaseReachable(false);
  const backlogBefore = await waitForBacklog(TARGET_BACKLOG, 300_000);
  progress(started, `backlog ${backlogBefore}; clean broker restart`);
  compose(["restart", "mosquitto"]);
  const backlogAfter = (await readSys([SYS_STORED])).get(SYS_STORED) ?? 0;
  progress(started, `broker back with ${backlogAfter} stored messages; Postgres back`);
  await sleep(5000);
  await setDatabaseReachable(true);

  const ledger = await waitForLedgerStored(pool, 240_000);
  const { rows } = await pool.query<{ gateways: number }>(
    `SELECT count(DISTINCT d.gateway_id)::int AS gateways FROM devices d JOIN device_state s USING (device_id) WHERE s.received_at > now() - interval '15 seconds'`);
  await pool.end();
  finish("broker restart with a queued backlog", [
    { check: "the broker restored the queued backlog from its persistence", pass: backlogAfter >= backlogBefore * 0.9, detail: `${backlogBefore} before, ${backlogAfter} after restart` },
    { check: "every acknowledged reading stored exactly once", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}, ${ledger.expected - ledger.stored} missing` },
    { check: "ingest receives live traffic again", pass: (rows[0]?.gateways ?? 0) >= GATEWAYS, detail: `${rows[0]?.gateways} gateways delivering` },
  ], { backlogBefore, backlogAfter, ledger });
}

await run(main);
