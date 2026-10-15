/**
 * Scenario 7b: ingest-1 stays down past a reduced bound, the documented failure mode of D2 (no takeover).
 * The broker keeps ingest-1's session and its round-robin share of every reading; with the bound lowered to 5,000
 * per session (`compose.small-queue.yaml`), that share overflows in about 25 s at 500 gateways.
 *
 * PASS means:
 * - the broker dropped messages and counted them (`$SYS` dropped rose);
 * - every acknowledged reading that wasn't stored is covered by that count (counted ≥ lost: S6-7's upper bound);
 * - BrokerDroppingMessages fired;
 * - ingest-2 kept storing its own share the whole time;
 * - once ingest-1 is back, it drains what its session still holds and receives again.
 * The last two are throughput checks: a stall of the whole VM during the run makes them INVALID, by the stall rule
 * (stallRule.ts). The drop and loss checks are always judged.
 *
 * Usage: `node scripts/scenarios/replica-down-overflow.ts`
 */
import {
  brokerKeepaliveTimeouts, compose, ensureFleet, finish, openDatabase, progress, promQuery, readSys, run, samplePsi,
  sleep, SYS_DROPPED, waitForAlert, waitForLedgerStored,
} from "./lib.ts";
import { gatedByStall } from "./stallRule.ts";
import type { StallEvidence } from "./stallRule.ts";

const GATEWAYS = 500;
const DOWN_SECONDS = 90;
const OVERRIDE = ["deploy/compose.small-queue.yaml"];
const INGEST2_RATE = 'sum(rate(ingest_readings_stored_total{outcome="inserted",instance=~"ingest-2.*"}[30s]))';
/** ingest-1's receive rate over the whole minute after its restart: a 30 s window can land on a VM stall (S7-11). */
const INGEST1_RATE = 'sum(increase(ingest_messages_received_total{instance=~"ingest-1.*"}[60s])) / 60';

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways with the bound at 5,000 per session`);
  await ensureFleet(pool, GATEWAYS, OVERRIDE);
  await sleep(20_000);
  const before = await readSys([SYS_DROPPED]);
  const stoppedAt = new Date().toISOString();
  const psi = samplePsi();

  progress(started, `stopping ingest-1 for ${DOWN_SECONDS} s`);
  compose(["stop", "ingest-1"], OVERRIDE);
  const ingest2Rates: number[] = [];
  for (let t = 0; t < DOWN_SECONDS; t += 15) {
    await sleep(15_000);
    ingest2Rates.push(await promQuery(INGEST2_RATE));
  }
  const firedAt = await waitForAlert("BrokerDroppingMessages", "firing", stoppedAt, 180_000);

  progress(started, "starting ingest-1 again");
  compose(["start", "ingest-1"], OVERRIDE);
  await sleep(60_000);
  const ingest1Rate = await promQuery(INGEST1_RATE);
  const ledger = await waitForLedgerStored(pool, 60_000);
  // The drop count is read only now: drops go on until ingest-1 is back and draining, and `$SYS` is up to one
  // sys_interval (10 s) old, so reading it before the restart missed the last seconds of drops (S7-11).
  await sleep(20_000);
  const dropped = ((await readSys([SYS_DROPPED])).get(SYS_DROPPED) ?? 0) - (before.get(SYS_DROPPED) ?? 0);
  await pool.end();
  const lost = ledger.expected - ledger.stored;
  const stall: StallEvidence = { psiAvg10Peak: psi.stop(), brokerTimeouts: brokerKeepaliveTimeouts(stoppedAt), untouched: { name: "ingest-2", samples: ingest2Rates } };

  finish(`scenario 7b: ingest-1 down ${DOWN_SECONDS} s past a 5,000 bound`, [
    { check: "the broker dropped messages and counted them", pass: dropped > 0, detail: `$SYS dropped +${dropped}` },
    { check: "every lost acknowledged reading is counted (counted ≥ lost)", pass: dropped >= lost, detail: `lost ${lost}, counted ${dropped}` },
    { check: "BrokerDroppingMessages fired", pass: firedAt !== null, detail: firedAt ?? "not within 3 min" },
    gatedByStall(stall, { check: "ingest-2 kept storing its share throughout", pass: ingest2Rates.every((r) => r > 50), detail: `inserted/s: ${ingest2Rates.map((r) => r.toFixed(0)).join(", ")}` }),
    gatedByStall(stall, { check: "ingest-1 receives again after it's back", pass: ingest1Rate > 50, detail: `${ingest1Rate.toFixed(0)} msg/s` }),
  ], { stoppedAt, dropped, lost, ledger, ingest2Rates, ingest1Rate, stall });
}

await run(main);
