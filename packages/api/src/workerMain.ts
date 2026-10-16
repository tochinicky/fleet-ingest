/**
 * Composition root of the health worker (S8-3): the health job and the open-gap window, moved out of the API. It's
 * the single writer of the fleet health metrics and of `health_snapshot`, which the API reads for `/fleet`.
 *
 * Start-up: validate config, serve `/metrics` at once (so a worker on standby is still a live scrape target), take
 * the single-writer lock on a connection of its own (waiting on standby while another worker holds it), then run the
 * job with its own pool. Losing the lock connection exits the process with 1, so Docker restarts it on standby.
 * Until the next lock check notices, a worker that has lost the lock can still be mid-pass: its snapshot write is
 * fenced (refused in the statement itself) and makes it exit too. What isn't fenced is its `/metrics` for those few
 * seconds (S8-3). SIGTERM/SIGINT stop the job and exit 0.
 */
import { hostname } from "node:os";
import pg from "pg";
import { pino } from "pino";
import { z } from "zod";
import { Counter, createRegistry, Gauge, startMetricsServer } from "@fleet-ingest/observability";
import { openApiPool } from "./databasePool.ts";
import { startHealthJob } from "./healthJob.ts";
import type { HealthJob } from "./healthJob.ts";
import { writeHealthSnapshot } from "./healthSnapshot.ts";
import { holdWorkerLock, waitForWorkerLock } from "./workerLock.ts";

/** Every setting the worker reads. */
const configSchema = z.object({
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  /** Port of `/metrics`: the fleet health metrics (Compose network only). */
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9464),
  /** Names this worker in `health_snapshot.writer` and the logs. */
  WORKER_ID: z.string().min(1).default(hostname()),
  /** How often a standby worker tries the lock, and how often the leader checks it still holds it. */
  LOCK_CHECK_MS: z.coerce.number().int().min(100).default(5000),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
});

/** Starts the worker. */
async function main(): Promise<void> {
  const parsed = configSchema.safeParse(process.env);
  if (!parsed.success) throw new Error(`invalid configuration:\n${parsed.error.issues.map((i) => `  ${i.path.join(".")}: ${i.message}`).join("\n")}`);
  const config = parsed.data;
  const log = pino({ level: config.LOG_LEVEL, base: { service: "health-worker", worker: config.WORKER_ID } });
  const registry = createRegistry("health-worker");
  const leader = new Gauge({ name: "fleet_health_worker_leader", help: "1 while this worker holds the single-writer lock and runs the health job (S8-3); 0 on standby.", registers: [registry] });
  const fenced = new Counter({ name: "fleet_health_snapshot_fenced_total", help: "Snapshot writes refused because this worker's session no longer held the lock: a zombie leader, stopped (S8-3).", registers: [registry] });
  leader.set(0);
  fenced.inc(0);
  startMetricsServer(registry, config.METRICS_PORT);

  let stopping = false;
  let job: HealthJob | undefined;
  // The lock lives and dies with this one session. Its own timeouts bound a lock check against a silent network.
  const lockClient = new pg.Client({ connectionString: config.DATABASE_URL, keepAlive: true, query_timeout: 10_000, connectionTimeoutMillis: 5000 });
  const lost = (reason: unknown): void => {
    if (stopping) return;
    log.fatal({ err: reason }, "lost the health worker lock; exiting so a restart starts on standby");
    job?.stop();
    process.exit(1);
  };
  lockClient.on("error", lost);
  await lockClient.connect();
  const pool = openApiPool(config.DATABASE_URL, (error) => log.warn({ err: error }, "idle database connection failed"));
  const shutDown = async (): Promise<void> => {
    stopping = true;
    job?.stop();
    await Promise.allSettled([lockClient.end(), pool.end()]);
    process.exit(0);
  };
  process.on("SIGTERM", () => void shutDown());
  process.on("SIGINT", () => void shutDown());

  let reported = false;
  const won = await waitForWorkerLock(lockClient, config.LOCK_CHECK_MS, () => {
    if (!reported) log.info("another health worker holds the lock; on standby");
    reported = true;
  }, () => stopping);
  if (!won) return;
  log.info("holding the health worker lock; starting the health job");
  leader.set(1);
  holdWorkerLock(lockClient, config.LOCK_CHECK_MS, lost);
  // The snapshot goes through the lock connection, and the write itself checks the lock (fencing, see healthSnapshot.ts).
  // A refused write means this worker is no longer leader, whatever its last lock check said: stop now.
  job = startHealthJob(pool, registry, (error) => log.warn({ err: error }, "health job pass failed"), undefined, async (gaps) => {
    if ((await writeHealthSnapshot(lockClient, gaps, config.WORKER_ID)) === "fenced") {
      fenced.inc();
      lost(new Error("snapshot write fenced: this session no longer holds the health worker lock"));
    }
  });
}

await main();
