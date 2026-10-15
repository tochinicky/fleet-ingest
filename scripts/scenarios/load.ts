/**
 * Load measurements for the README, with the hardware noted. With 500 gateways (about 400 msg/s), under the
 * current stack configuration:
 * 1. steady state for 2 minutes: received msg/s; lag (received − measured) p50/p95; receipt → commit p50/p95;
 *    ingest RSS and event-loop lag;
 * 2. capacity: Postgres is cut for 60 s to build a backlog, then restored. While the backlog drains, the peak
 *    received rate (total and per replica) is what ingest sustains flat out. Also the drain time, and ingest RSS
 *    during the flood.
 * Run it once per configuration (two replicas or one; batch 500 or 1) and label it.
 * Usage: `node scripts/scenarios/load.ts <label> [one-replica]`
 */
import { cpus, totalmem } from "node:os";
import {
  containerMemoryMb, ensureFleet, finish, openDatabase, progress, promQuery, readSys, run, setDatabaseReachable,
  sleep, SYS_STORED,
} from "./lib.ts";

const GATEWAYS = 500;

/** Samples `fn` every 5 s for `seconds` and returns the maximum seen. */
async function peakOver(seconds: number, fn: () => Promise<number> | number): Promise<number> {
  let peak = 0;
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    peak = Math.max(peak, await fn());
    await sleep(5000);
  }
  return peak;
}

/** Ingest RSS in MB: the larger of the running replicas. */
async function ingestMb(oneReplica: boolean): Promise<number> {
  return oneReplica ? containerMemoryMb("ingest-1") : Math.max(await containerMemoryMb("ingest-1"), await containerMemoryMb("ingest-2"));
}

/** Runs the measurement and prints it. */
async function main(): Promise<void> {
  const label = process.argv[2] ?? "default";
  const oneReplica = process.argv.includes("one-replica");
  const overrides = oneReplica ? ["deploy/compose.one-replica.yaml"] : [];
  const started = Date.now();
  const pool = openDatabase();
  await setDatabaseReachable(true).catch(() => undefined);
  progress(started, `${label}: starting ${GATEWAYS} gateways${oneReplica ? " (one ingest replica)" : ""}, BATCH_MAX_ROWS=${process.env.BATCH_MAX_ROWS ?? 500}`);
  await ensureFleet(pool, GATEWAYS, overrides);
  await sleep(60_000);

  progress(started, "steady state: 2 minutes");
  const steadyRss = await peakOver(120, () => ingestMb(oneReplica));
  const steady = {
    receivedPerSecond: +(await promQuery("sum(rate(ingest_messages_received_total[2m]))")).toFixed(0),
    lagP50s: +(await promQuery("histogram_quantile(0.5, sum by (le) (rate(ingest_lag_seconds_bucket[2m])))")).toFixed(3),
    lagP95s: +(await promQuery("histogram_quantile(0.95, sum by (le) (rate(ingest_lag_seconds_bucket[2m])))")).toFixed(3),
    commitP50s: +(await promQuery("histogram_quantile(0.5, sum by (le) (rate(ingest_commit_delay_seconds_bucket[2m])))")).toFixed(3),
    commitP95s: +(await promQuery("histogram_quantile(0.95, sum by (le) (rate(ingest_commit_delay_seconds_bucket[2m])))")).toFixed(3),
    eventLoopP99s: +(await promQuery('max(nodejs_eventloop_lag_p99_seconds{service="ingest"})')).toFixed(3),
    ingestRssMb: +steadyRss.toFixed(0),
  };

  progress(started, "capacity: Postgres cut for 60 s to build a backlog");
  const baselineStored = (await readSys([SYS_STORED])).get(SYS_STORED) ?? 0;
  await setDatabaseReachable(false);
  await sleep(60_000);
  const backlog = ((await readSys([SYS_STORED])).get(SYS_STORED) ?? 0) - baselineStored;
  await setDatabaseReachable(true);
  const drainStarted = Date.now();
  let peakRate = 0;
  let peakPerReplica = 0;
  let floodRss = 0;
  for (;;) {
    await sleep(5000);
    peakRate = Math.max(peakRate, await promQuery("sum(rate(ingest_messages_received_total[20s]))"));
    peakPerReplica = Math.max(peakPerReplica, await promQuery("max(sum by (instance) (rate(ingest_messages_received_total[20s])))"));
    floodRss = Math.max(floodRss, await ingestMb(oneReplica));
    const stored = (await readSys([SYS_STORED])).get(SYS_STORED) ?? Infinity;
    if (stored <= baselineStored + 2000 || Date.now() - drainStarted > 20 * 60_000) break;
  }
  const drainSeconds = (Date.now() - drainStarted) / 1000;
  await pool.end();
  finish(`load: ${label}`, [{ check: "measured", pass: true, detail: "see measured" }], {
    label, replicas: oneReplica ? 1 : 2, batchMaxRows: Number(process.env.BATCH_MAX_ROWS ?? 500),
    hardware: { host: `${cpus()[0]?.model} × ${cpus().length}`, hostMemoryGiB: +(totalmem() / 2 ** 30).toFixed(0), dockerVm: "Docker Desktop, 8 CPU / 4 GB" },
    steady,
    capacity: { backlogBuilt: backlog, drainSeconds: +drainSeconds.toFixed(0), peakReceivedPerSecond: +peakRate.toFixed(0), peakPerReplicaPerSecond: +peakPerReplica.toFixed(0), floodIngestRssMb: +floodRss.toFixed(0) },
  });
}

await run(main);
