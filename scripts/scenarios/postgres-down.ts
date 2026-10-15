/**
 * Scenario 3: Postgres goes down for ingest while 500 gateways keep sending about 400 msg/s.
 *
 * - `3a` (inside the bound): the database is cut for 120 s, about 48k messages, about 24k per ingest session against a
 *   bound of 120k. PASS means no message is dropped, every reading the broker acknowledged is stored exactly once,
 *   the broker's queue stays under its bound and its memory under the container limit, and ingest's memory stays
 *   flat, because the backpressure stops at Receive Maximum.
 * - `3b` (over the bound): the broker runs with `max_queued_messages 5000` and the database is cut for 60 s. PASS means
 *   drops happen and are accounted for (S7-7):
 *   - **readings, exactly:** every acknowledged reading not in Postgres (fleet-sim's ledger is the ground truth) is
 *     covered by the `$SYS` drop counter. The counter counts drop *events*, so with resends it can only be higher (S6-7);
 *   - **statuses, within a tolerance:** the drops left over match the statuses acknowledged but never written, within
 *     ±300. The derivation: 500 gateways × 1 status per 60 s ≈ 8.3 statuses/s; ingest's counter is read from
 *     Prometheus, up to one 15 s scrape old at each end of the window, so the skew is up to 30 s × 8.3 ≈ 250, rounded
 *     up to 300. Fleet size or status interval changes the number. Statuses have no ground truth in Postgres (they're upserts),
 *     so they're counted from ingest's metric with `increase()`, which survives a replica restart; a plain difference
 *     of counter values doesn't (S7-7).
 *   Nothing is OOM-killed, and nothing is stored twice.
 * - Both: neither ingest replica restarts during the outage; it must retry, not exit.
 * - `DB_FAILURE=hang` (either variant): the database hangs instead of refusing connections (a toxiproxy `timeout`
 *   toxic): connections are held and nothing passes. Ingest must retry *during* the hang (its query timeout turns a
 *   silent database into a retryable error), and survive the held connections being closed when the toxic goes,
 *   which once crashed it (S7-10). Readings measured from 60 s after the toxic is removed must be prompt again
 *   (p95 < 2 s): the backlog has drained by then.
 *
 * Usage: `node scripts/scenarios/postgres-down.ts 3a|3b`. The alert checks (IngestLagHigh, BrokerDroppingMessages)
 * arrive with Prometheus in slice 6.
 */
import {
  compose, containerMemoryMb, ensureFleet, fetchAckedStatuses, finish, gatedBy, ingestSummaries, lagByMeasured,
  oomKilled, openDatabase, progress, PROMPT_LAG_P95_SECONDS, promQuery, readSys, restartCount, run, setDatabaseHung,
  setDatabaseReachable, sleep, SYS_DROPPED, SYS_HEAP, SYS_STORED, waitForLedgerStored, waitForQuietBaseline,
} from "./lib.ts";
import type { Verdict } from "./lib.ts";

const GATEWAYS = 500;
const QUEUE_BOUND_PER_SESSION = 120_000;
const BROKER_LIMIT_MB = 267;
const RECEIVE_MAXIMUM = 1000;

/** Samples the broker queue depth, broker heap and memory, and ingest memory every 5 s while `running()` holds. */
async function sampleWhile(running: () => boolean): Promise<{ peakStored: number; peakHeapMb: number; peakBrokerMb: number; ingestMb: number[] }> {
  const peaks = { peakStored: 0, peakHeapMb: 0, peakBrokerMb: 0, ingestMb: [] as number[] };
  while (running()) {
    const sys = await readSys([SYS_STORED, SYS_HEAP]);
    peaks.peakStored = Math.max(peaks.peakStored, sys.get(SYS_STORED) ?? 0);
    peaks.peakHeapMb = Math.max(peaks.peakHeapMb, (sys.get(SYS_HEAP) ?? 0) / 1e6);
    peaks.peakBrokerMb = Math.max(peaks.peakBrokerMb, (await containerMemoryMb("mosquitto")));
    peaks.ingestMb.push(Math.max(await containerMemoryMb("ingest-1"), await containerMemoryMb("ingest-2")));
    await sleep(5000);
  }
  return peaks;
}

/** Waits after recovery until the broker's stored-message count is back near its baseline; returns the time it took. */
async function waitForDrain(baselineStored: number, timeoutMs: number): Promise<number> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const stored = (await readSys([SYS_STORED])).get(SYS_STORED) ?? Infinity;
    if (stored <= baselineStored + 2 * RECEIVE_MAXIMUM) return Date.now() - started;
    await sleep(2000);
  }
  return Infinity;
}

/** Runs the scenario variant and prints its verdicts. */
async function main(): Promise<void> {
  const variant = process.argv[2];
  if (variant !== "3a" && variant !== "3b") throw new Error("usage: postgres-down.ts 3a|3b");
  const started = Date.now();
  const outageSeconds = variant === "3a" ? 120 : 60;
  const overrides = variant === "3b" ? ["deploy/compose.small-queue.yaml"] : [];
  const pool = openDatabase();
  const hang = process.env.DB_FAILURE === "hang";
  await setDatabaseReachable(true).catch(() => undefined);
  await setDatabaseHung(false).catch(() => undefined);
  progress(started, `starting ${GATEWAYS} gateways${variant === "3b" ? " with max_queued_messages 5000" : ""}`);
  await ensureFleet(pool, GATEWAYS, overrides);
  await setDatabaseReachable(true);
  await sleep(30_000);

  const baselineLedger = await waitForLedgerStored(pool, 60_000);
  await sleep(15_000);
  const baseline = hang ? await waitForQuietBaseline(pool) : undefined;
  if (baseline) progress(started, baseline.quiet ? `quiet baseline after ${baseline.waitedSeconds} s (p95 ${baseline.p95.toFixed(2)} s)` : `no quiet baseline in ${baseline.waitedSeconds} s (p95 ${baseline.p95.toFixed(2)} s): the recovery check will be INVALID`);
  const statusesAcked0 = await fetchAckedStatuses();
  const statusesBaselineAt = Date.now();
  const ingestRestartsBefore = restartCount("ingest-1") + restartCount("ingest-2");
  const before = await readSys([SYS_DROPPED, SYS_STORED]);
  const ingestBaselineMb = Math.max(await containerMemoryMb("ingest-1"), await containerMemoryMb("ingest-2"));
  const brokerRestartsBefore = restartCount("mosquitto");
  const outageStart = new Date().toISOString();
  progress(started, `steady: stored=${before.get(SYS_STORED)}, ingest ${ingestBaselineMb.toFixed(0)} MB; cutting Postgres for ${outageSeconds} s`);

  await (hang ? setDatabaseHung(true) : setDatabaseReachable(false));
  let outage = true;
  const sampling = sampleWhile(() => outage);
  await sleep(outageSeconds * 1000);
  await (hang ? setDatabaseHung(false) : setDatabaseReachable(true));
  const restoredAt = Date.now();
  outage = false;
  const peaks = await sampling;
  progress(started, `Postgres back; peak broker queue ${peaks.peakStored}, broker ${peaks.peakBrokerMb.toFixed(0)} MB`);

  const drainMs = await waitForDrain(before.get(SYS_STORED) ?? 0, 300_000);
  progress(started, `drained in ${(drainMs / 1000).toFixed(1)} s; checking every acknowledged reading`);
  const ledgerCheck = await waitForLedgerStored(pool, variant === "3a" ? 120_000 : 30_000);
  await sleep(15_000);
  await sleep(20_000);
  const windowSeconds = Math.ceil((Date.now() - statusesBaselineAt) / 1000);
  const statusesLost = Math.round((await fetchAckedStatuses()) - statusesAcked0 - (await promQuery(`sum(increase(ingest_statuses_stored_total[${windowSeconds}s]))`)));
  const ingestRestarts = restartCount("ingest-1") + restartCount("ingest-2") - ingestRestartsBefore;
  const after = await readSys([SYS_DROPPED]);
  const droppedDelta = (after.get(SYS_DROPPED) ?? 0) - (before.get(SYS_DROPPED) ?? 0);
  const summaries = [...ingestSummaries("ingest-1", outageStart), ...ingestSummaries("ingest-2", outageStart)];
  const retries = summaries.reduce((s, x) => s + x.retries, 0);
  const retriesDuringHang = summaries.filter((x) => x.time <= restoredAt).reduce((s, x) => s + x.retries, 0);
  const promptFrom = restoredAt + 60_000;
  await sleep(Math.max(0, promptFrom + 40_000 - Date.now()));
  const promptLag = await lagByMeasured(pool, "measured_at > $1 AND measured_at < $2", [new Date(promptFrom).toISOString(), new Date(promptFrom + 30_000).toISOString()]);
  const ingestPeakMb = Math.max(...peaks.ingestMb);
  const verdicts: Verdict[] = [{ check: "no ingest replica restarted during the outage (it retries)", pass: ingestRestarts === 0, detail: `${ingestRestarts} restarts` }];
  if (baseline) {
    verdicts.push(gatedBy(baseline, {
      check: "readings measured from 60 s after the hang ended are prompt (p95 < 2 s)", pass: promptLag.n > 0 && promptLag.p95 < PROMPT_LAG_P95_SECONDS, detail: `p95 ${promptLag.p95.toFixed(2)} s over ${promptLag.n} readings`,
    }));
  }

  if (variant === "3a") {
    verdicts.push(
      { check: "no message dropped by the broker", pass: droppedDelta === 0, detail: `$SYS dropped +${droppedDelta}` },
      { check: "every acknowledged reading stored exactly once", pass: ledgerCheck.stored === ledgerCheck.expected, detail: `${ledgerCheck.stored} of ${ledgerCheck.expected}` },
      { check: "broker queue stayed under its bound", pass: peaks.peakStored < 2 * QUEUE_BOUND_PER_SESSION, detail: `peak ${peaks.peakStored} < ${2 * QUEUE_BOUND_PER_SESSION} (2 sessions)` },
      { check: "broker memory stayed under the container limit", pass: peaks.peakBrokerMb < BROKER_LIMIT_MB, detail: `peak ${peaks.peakBrokerMb.toFixed(0)} MB < ${BROKER_LIMIT_MB} MB` },
      { check: "ingest memory stayed flat (within ±20% of baseline)", pass: ingestPeakMb <= ingestBaselineMb * 1.2, detail: `baseline ${ingestBaselineMb.toFixed(0)} MB, peak ${ingestPeakMb.toFixed(0)} MB` },
      hang
        ? { check: "ingest retried during the hang (query timeout), instead of waiting or failing", pass: retriesDuringHang > 0, detail: `${retriesDuringHang} retries during, ${retries} in all` }
        : { check: "ingest retried instead of failing", pass: retries > 0, detail: `${retries} retries` },
    );
  } else {
    const readingsLost = ledgerCheck.expected - ledgerCheck.stored - (baselineLedger.expected - baselineLedger.stored);
    verdicts.push(
      { check: "the bound was exceeded and the broker dropped messages", pass: droppedDelta > 0, detail: `$SYS dropped +${droppedDelta}` },
      { check: "every lost acknowledged reading is counted (readings lost ≤ drops counted)", pass: readingsLost <= droppedDelta, detail: `readings lost ${readingsLost}, counted ${droppedDelta}` },
      {
        check: "the remaining drops match the lost statuses, within ±300",
        pass: Math.abs(droppedDelta - readingsLost - statusesLost) <= 300,
        detail: `counted ${droppedDelta} − readings ${readingsLost} = ${droppedDelta - readingsLost}; statuses lost ${statusesLost}`,
      },
      { check: "no OOM kill and no broker restart", pass: !oomKilled("mosquitto") && restartCount("mosquitto") === brokerRestartsBefore, detail: `restarts ${restartCount("mosquitto") - brokerRestartsBefore}` },
      { check: "nothing stored twice", pass: ledgerCheck.stored <= ledgerCheck.expected, detail: "primary key (device_id, epoch, seq)" },
    );
    progress(started, "restoring the normal broker config");
    compose(["up", "-d", "mosquitto"]);
  }
  await pool.end();
  finish(`scenario ${variant}: Postgres down for ${outageSeconds} s at ~${GATEWAYS * 4 / 5} msg/s`, verdicts, {
    outageSeconds, drainSeconds: +(drainMs / 1000).toFixed(1), peakBrokerQueue: peaks.peakStored, peakBrokerHeapMb: +peaks.peakHeapMb.toFixed(1),
    peakBrokerMb: +peaks.peakBrokerMb.toFixed(1), ingestBaselineMb: +ingestBaselineMb.toFixed(1), ingestPeakMb: +ingestPeakMb.toFixed(1),
    droppedDelta, retries, retriesDuringHang, promptLag, baseline, ledger: ledgerCheck, statusesLost,
  });
}

await run(main);

