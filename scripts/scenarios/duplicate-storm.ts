/**
 * Scenario 1: a duplicate storm. fleet-sim sends 20% of readings twice and reorders them in a window of 50
 * (S7-5), for 120 s, at 500 gateways.
 *
 * PASS means:
 * - every reading the broker acknowledged is stored, once (the ledger counts keys, not copies);
 * - duplicates really arrived and were absorbed (`ingest_readings_stored_total{outcome="duplicate"}` rose);
 * - state never regressed: every device's `device_state` is its newest stored reading by (measured_at, epoch, seq);
 * - no gap: reordering delays readings but loses none.
 *
 * Usage: `node scripts/scenarios/duplicate-storm.ts`
 */
import { ensureFleet, finish, openDatabase, progress, promQuery, run, setChaos, sleep, waitForLedgerStored } from "./lib.ts";

const GATEWAYS = 500;
const STORM_SECONDS = 120;
const DUPLICATES = 'sum(ingest_readings_stored_total{outcome="duplicate"})';

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await setChaos({ duplicateRate: 0, reorderWindow: 0, poisonRate: 0 });
  await sleep(20_000);
  const duplicatesBefore = await promQuery(DUPLICATES);
  const from = new Date();

  progress(started, `duplicate storm for ${STORM_SECONDS} s: 20% sent twice, reorder window 50`);
  await setChaos({ duplicateRate: 0.2, reorderWindow: 50 });
  await sleep(STORM_SECONDS * 1000);
  await setChaos({ duplicateRate: 0, reorderWindow: 0 });
  const to = new Date();
  await sleep(30_000);

  const ledger = await waitForLedgerStored(pool, 120_000);
  await sleep(20_000);
  const duplicatesAbsorbed = (await promQuery(DUPLICATES)) - duplicatesBefore;
  const { rows: regressed } = await pool.query<{ n: number }>(`
    SELECT count(*)::int AS n FROM device_state s
    WHERE s.device_id LIKE 'sim-gw-%' AND (s.measured_at, s.epoch, s.seq) <> (
      SELECT r.measured_at, r.epoch, r.seq FROM readings r WHERE r.device_id = s.device_id
      ORDER BY r.measured_at DESC, r.epoch DESC, r.seq DESC LIMIT 1)`);
  const { rows: gapRows } = await pool.query<{ n: number }>(`
    SELECT count(*)::int AS n FROM (
      SELECT seq - lag(seq) OVER (PARTITION BY device_id, epoch ORDER BY seq) AS step
      FROM readings WHERE device_id LIKE 'sim-gw-%' AND measured_at BETWEEN $1 AND $2) s
    WHERE step > 1`, [from, to]);
  await pool.end();

  finish(`scenario 1: duplicate storm (20% duplicates, reorder window 50, ${STORM_SECONDS} s)`, [
    { check: "every acknowledged reading stored exactly once", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}` },
    { check: "duplicates arrived and were absorbed", pass: duplicatesAbsorbed > 0, detail: `${duplicatesAbsorbed} duplicate copies ignored by ON CONFLICT` },
    { check: "state never regressed: device_state is each device's newest reading", pass: regressed[0]?.n === 0, detail: `${regressed[0]?.n} devices behind their newest reading` },
    { check: "no gap in the storm window", pass: gapRows[0]?.n === 0, detail: `${gapRows[0]?.n} gaps` },
  ], { from: from.toISOString(), to: to.toISOString(), duplicatesAbsorbed, ledger });
}

await run(main);
