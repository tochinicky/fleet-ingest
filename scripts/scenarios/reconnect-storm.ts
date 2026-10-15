/**
 * Scenario 4: a reconnect storm. The broker is stopped (cleanly, so its persistence is written) for 10 minutes
 * while 500 gateways keep measuring. Each buffers about 480 readings, about 240k across the fleet. Then the broker
 * comes back, and every gateway reconnects and replays its backlog at up to 200 msg/s with a 16-message window.
 *
 * Run it twice:
 * - `jitter` (fleet-sim's reconnect delay uniform in 0–300 s, full jitter). PASS means 0 drops, the broker queue
 *   under its bound, every acknowledged reading stored exactly once, and the drain time recorded.
 * - `no-jitter` (every gateway retries every 2 s, so all are back within seconds of the broker). Measured as a finding
 *   (peak queue, drops, drain time), with no PASS bar.
 *
 * Usage: `node scripts/scenarios/reconnect-storm.ts jitter|no-jitter [outageSeconds=600]`
 */
import {
  compose, containerMemoryMb, ensureFleet, finish, missingReadings, openDatabase, progress, readSys, run, sleep,
  SYS_DROPPED, SYS_STORED, waitForLedgerStored,
} from "./lib.ts";
import type { Verdict } from "./lib.ts";

const GATEWAYS = 500;
const QUEUE_BOUND_TOTAL = 2 * 120_000;
const SYS_CLIENTS = "$SYS/broker/clients/connected";

/** fleet-sim's outbox counters. */
async function fleetStats(): Promise<{ buffered: number; dropped: number }> {
  return (await (await fetch("http://127.0.0.1:8090/stats")).json()) as { buffered: number; dropped: number };
}

/** Peak values seen while sampling every 5 s. */
interface Peaks {
  queue: number;
  brokerMb: number;
  ingestMb: number;
  dropped: number;
}

/**
 * After the broker restarts: samples every 5 s until every gateway's outbox is empty and the broker queue is back to
 * near its baseline. Returns the drain time and the peaks. `$SYS` counters started again at 0 with the broker.
 */
async function sampleDrain(baselineStored: number, timeoutMs: number, started: number): Promise<{ drainMs: number; peaks: Peaks }> {
  const t0 = Date.now();
  const peaks: Peaks = { queue: 0, brokerMb: 0, ingestMb: 0, dropped: 0 };
  while (Date.now() - t0 < timeoutMs) {
    const sys = await readSys([SYS_STORED, SYS_DROPPED, SYS_CLIENTS]);
    const stats = await fleetStats();
    peaks.queue = Math.max(peaks.queue, sys.get(SYS_STORED) ?? 0);
    peaks.dropped = Math.max(peaks.dropped, sys.get(SYS_DROPPED) ?? 0);
    peaks.brokerMb = Math.max(peaks.brokerMb, (await containerMemoryMb("mosquitto")));
    peaks.ingestMb = Math.max(peaks.ingestMb, await containerMemoryMb("ingest-1"), await containerMemoryMb("ingest-2"));
    progress(started, `clients ${sys.get(SYS_CLIENTS)}, broker queue ${sys.get(SYS_STORED)}, fleet outbox ${stats.buffered}, dropped ${sys.get(SYS_DROPPED)}`);
    if (stats.buffered === 0 && (sys.get(SYS_CLIENTS) ?? 0) >= GATEWAYS && (sys.get(SYS_STORED) ?? Infinity) <= baselineStored + 2000) {
      return { drainMs: Date.now() - t0, peaks };
    }
    await sleep(5000);
  }
  return { drainMs: Infinity, peaks };
}

/** Runs the scenario variant and prints its verdicts. */
async function main(): Promise<void> {
  const variant = process.argv[2];
  if (variant !== "jitter" && variant !== "no-jitter") throw new Error("usage: reconnect-storm.ts jitter|no-jitter [outageSeconds]");
  const outageSeconds = Number(process.argv[3] ?? "600");
  const jitterMs = variant === "jitter" ? 300_000 : 0;
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways, reconnect ${variant === "jitter" ? "uniform 0–300 s" : "fixed 2 s"}`);
  process.env.RECONNECT_JITTER_MAX_MS = String(jitterMs);
  await ensureFleet(pool, GATEWAYS);
  await sleep(20_000);
  await waitForLedgerStored(pool, 60_000);
  const baselineStored = (await readSys([SYS_STORED])).get(SYS_STORED) ?? 0;

  progress(started, `stopping the broker for ${outageSeconds} s`);
  compose(["stop", "mosquitto"]);
  await sleep(outageSeconds * 1000);
  const buffered = (await fleetStats()).buffered;
  progress(started, `starting the broker; the fleet holds ${buffered} buffered readings`);
  const restartedAt = new Date().toISOString();
  compose(["start", "mosquitto"]);
  const { drainMs, peaks } = await sampleDrain(baselineStored, 30 * 60_000, started);
  progress(started, `drained in ${(drainMs / 1000).toFixed(0)} s; checking every acknowledged reading`);
  const ledger = await waitForLedgerStored(pool, 180_000);
  const missing = ledger.stored < ledger.expected ? await missingReadings(pool) : [];
  const fleetDropped = (await fleetStats()).dropped;
  await pool.end();

  const measured = {
    variant, outageSeconds, bufferedAtRestart: buffered, drainSeconds: +(drainMs / 1000).toFixed(0), peakBrokerQueue: peaks.queue,
    brokerDropped: peaks.dropped, peakBrokerMb: +peaks.brokerMb.toFixed(0), peakIngestMb: +peaks.ingestMb.toFixed(0), fleetOutboxDropped: fleetDropped, ledger,
    brokerRestartedAt: restartedAt, missing,
  };
  const verdicts: Verdict[] = variant === "jitter"
    ? [
      { check: "no message dropped by the broker", pass: peaks.dropped === 0, detail: `$SYS dropped ${peaks.dropped} (counter restarted with the broker)` },
      { check: "the broker queue stayed under its bound", pass: peaks.queue < QUEUE_BOUND_TOTAL, detail: `peak ${peaks.queue} < ${QUEUE_BOUND_TOTAL}` },
      { check: "every acknowledged reading stored exactly once", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}` },
      { check: "the backlog drained (time recorded)", pass: Number.isFinite(drainMs), detail: `${(drainMs / 1000).toFixed(0)} s` },
    ]
    : [{ check: "no-jitter worst case measured (finding, no PASS bar)", pass: true, detail: `peak queue ${peaks.queue}, dropped ${peaks.dropped}, drain ${(drainMs / 1000).toFixed(0)} s, ${ledger.expected - ledger.stored} acknowledged readings not stored` }];
  finish(`scenario 4: reconnect storm after a ${outageSeconds} s broker outage (${variant})`, verdicts, measured);
}

await run(main);
