/**
 * Scenario 5: poison messages. fleet-sim follows 5% of readings with one poison message of a random kind
 * (S7-5): invalid JSON, a schema mismatch, a device ID that doesn't match the topic, a timestamp 10 minutes in the
 * future, and a device bound to another gateway. Poison starts only once all 500 gateways are delivering, so every
 * device a `foreign_device` poison names is already bound to its own gateway.
 *
 * PASS means:
 * - each of the five kinds is in `dead_letters` with the matching reason;
 * - no poison was stored as a reading (their seqs are all ≥ 900,000,000);
 * - every acknowledged real reading is stored;
 * - throughput is unaffected: readings inserted per second during the poison is no more than 20% below before. Only a
 *   drop is the poison's cost; a rise is the VM, and the first demo failed on 355/s against 295/s (S7-11);
 * - DeadLettersAppearing fires.
 *
 * Usage: `node scripts/scenarios/poison.ts`
 */
import {
  ensureFleet, finish, openDatabase, progress, promQuery, run, setChaos, sleep, waitForAlert, waitForLedgerStored,
} from "./lib.ts";

const GATEWAYS = 500;
const POISON_SECONDS = 90;
const REASONS = ["invalid_json", "schema_mismatch", "device_id_mismatch", "future_timestamp", "foreign_device"];
const INSERT_RATE = 'sum(rate(ingest_readings_stored_total{outcome="inserted"}[1m]))';

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await setChaos({ duplicateRate: 0, reorderWindow: 0, poisonRate: 0 });
  await sleep(70_000);
  const rateBefore = await promQuery(INSERT_RATE);
  const from = new Date();

  progress(started, `poison on for ${POISON_SECONDS} s (5% of readings followed by one poison message)`);
  await setChaos({ poisonRate: 0.05 });
  await sleep(POISON_SECONDS * 1000);
  const rateDuring = await promQuery(INSERT_RATE);
  await setChaos({ poisonRate: 0 });
  const firedAt = await waitForAlert("DeadLettersAppearing", "firing", from.toISOString(), 240_000);

  const ledger = await waitForLedgerStored(pool, 120_000);
  const { rows: byReason } = await pool.query<{ reason: string; n: number }>(
    "SELECT reason, count(*)::int AS n FROM dead_letters WHERE received_at >= $1 GROUP BY reason", [from]);
  const { rows: stored } = await pool.query<{ n: number }>("SELECT count(*)::int AS n FROM readings WHERE seq >= 900000000");
  await pool.end();
  const counts = Object.fromEntries(REASONS.map((r) => [r, byReason.find((row) => row.reason === r)?.n ?? 0]));

  finish(`scenario 5: poison messages (5% for ${POISON_SECONDS} s)`, [
    { check: "every poison kind is dead-lettered with its reason", pass: REASONS.every((r) => (counts[r] ?? 0) > 0), detail: REASONS.map((r) => `${r} ${counts[r]}`).join(", ") },
    { check: "no poison stored as a reading", pass: stored[0]?.n === 0, detail: `${stored[0]?.n} readings with seq ≥ 900,000,000` },
    { check: "every acknowledged real reading stored", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}` },
    { check: "throughput unaffected (inserted/s no more than 20% below before)", pass: rateDuring >= 0.8 * rateBefore, detail: `${rateBefore.toFixed(0)}/s before, ${rateDuring.toFixed(0)}/s during` },
    { check: "DeadLettersAppearing fired", pass: firedAt !== null, detail: firedAt ?? "not within 4 min" },
  ], { from: from.toISOString(), deadLetters: counts, rateBefore, rateDuring, ledger });
}

await run(main);
