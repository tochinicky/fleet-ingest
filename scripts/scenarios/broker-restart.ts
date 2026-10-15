/**
 * A clean broker restart under steady load (decisions.md S3-9, S6-15, S7-8).
 *
 * PASS means:
 * - no acknowledged reading is lost. With persist-sqlite the broker keeps ingest's shared subscriptions over a restart,
 *   so readings published before ingest is back queue for it (with the snapshot fallback this fails: the S3-9 window);
 * - every gateway delivers again within its reconnect budget: the connect timeout (`CONNECT_TIMEOUT_MS`, 10 s like the
 *   real gateway) + its reconnect delay (2 s, or the jitter maximum) + a 30 s margin for 500 TLS handshakes on a
 *   saturated VM. Not at a fixed instant: an attempt made while the broker is down waits out the connect timeout,
 *   because toxiproxy (like a cloud load balancer) accepts TCP while the broker is away (S7-8).
 *
 * The time until all were back is recorded. Usage: `node scripts/scenarios/broker-restart.ts`
 */
import type pg from "pg";
import { compose, ensureFleet, finish, openDatabase, progress, readSys, run, sleep, SYS_DROPPED, waitForLedgerStored } from "./lib.ts";

const GATEWAYS = 500;
const CONNECT_TIMEOUT_MS = Number(process.env.CONNECT_TIMEOUT_MS ?? 10_000);
const RECONNECT_DELAY_MS = Number(process.env.RECONNECT_JITTER_MAX_MS || 2_000);
const MARGIN_MS = 30_000;

/** Gateways with a reading received after `since`. */
async function deliveringSince(pool: pg.Pool, since: Date): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    `SELECT count(DISTINCT d.gateway_id)::int AS n FROM devices d JOIN device_state s USING (device_id) WHERE s.received_at > $1`, [since]);
  return rows[0]?.n ?? 0;
}

/** Restarts the broker once under steady load and reports what the ledger says was lost. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await sleep(20_000);
  const before = await readSys([SYS_DROPPED]);
  progress(started, "clean broker restart (SIGTERM, persistence written)");
  compose(["restart", "mosquitto"]);
  const restartedAt = new Date();
  const budgetMs = CONNECT_TIMEOUT_MS + RECONNECT_DELAY_MS + MARGIN_MS;
  let delivering = 0;
  while (Date.now() - restartedAt.getTime() < budgetMs + 60_000 && (delivering = await deliveringSince(pool, restartedAt)) < GATEWAYS) await sleep(2000);
  const allBackMs = delivering >= GATEWAYS ? Date.now() - restartedAt.getTime() : null;
  await sleep(20_000);
  const ledger = await waitForLedgerStored(pool, 60_000);
  const after = await readSys([SYS_DROPPED]);
  await pool.end();
  const lost = ledger.expected - ledger.stored;
  finish("broker restart under load", [
    { check: "no acknowledged reading lost over the restart", pass: lost === 0, detail: `${lost} of ${ledger.expected}` },
    {
      check: `every gateway delivers again within its reconnect budget (${budgetMs / 1000} s)`,
      pass: allBackMs !== null && allBackMs <= budgetMs,
      detail: allBackMs === null ? `${delivering} of ${GATEWAYS} back after ${(budgetMs + 60_000) / 1000} s` : `all ${GATEWAYS} back after ${(allBackMs / 1000).toFixed(0)} s`,
    },
  ], { lost, ledger, allBackSeconds: allBackMs === null ? null : allBackMs / 1000, budgetSeconds: budgetMs / 1000, droppedDelta: (after.get(SYS_DROPPED) ?? 0) - (before.get(SYS_DROPPED) ?? 0) });
}

await run(main);
