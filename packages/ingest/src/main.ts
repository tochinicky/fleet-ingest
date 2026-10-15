/**
 * Composition root of ingest: the only place that reads the environment, creates the pool, the logger and the MQTT
 * subscriber, and wires them to the pipeline. Also owns shutdown:
 * - SIGTERM/SIGINT: stop taking messages, write and acknowledge what's in hand (within the grace period), disconnect.
 * - SIGKILL or a crash: whatever wasn't acknowledged is redelivered to this client ID on restart, and stored once.
 * Once subscribed, it writes a ready file for the container healthcheck. On a brand-new broker nothing is queued for
 * ingest until its session exists, so Compose holds the fleet back until both replicas are ready (decisions.md S1-3).
 */
import { readFileSync, rmSync, writeFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import type pg from "pg";
import { pino } from "pino";
import type { Logger } from "pino";
import { loadConfig } from "./config.ts";
import type { Config } from "./config.ts";
import { ActivityCounters } from "./core/activityCounters.ts";
import { startSubscriber } from "./adapters/mqttSubscriber.ts";
import type { Delivery, Subscriber } from "./adapters/mqttSubscriber.ts";
import { openIngestPool } from "./adapters/databasePool.ts";
import { createPostgresStore } from "./adapters/postgresStore.ts";
import type { ItemOutcome, StoreItem } from "./adapters/postgresStore.ts";
import { createIngestPipeline } from "./ingestPipeline.ts";
import { createRegistry, startMetricsServer } from "@fleet-ingest/observability";
import { IngestMetrics } from "./adapters/metrics.ts";
import type { IngestPipeline, PipelineClock } from "./ingestPipeline.ts";

/** What shutdown needs to reach: set up in `main`, read by {@link stopAndExit} and {@link shutDownGracefully}. */
interface Runtime {
  log: Logger;
  pool: pg.Pool;
  subscriber: Subscriber | undefined;
  shuttingDown: boolean;
}

/** The real clock: wall time, real timers, and `Math.random` for jitter. */
const systemClock: PipelineClock = {
  now: () => Date.now(),
  sleep: (ms) => sleep(ms),
  schedule: (ms, fn) => {
    const timer = setTimeout(fn, ms);
    return () => clearTimeout(timer);
  },
  random: Math.random,
};

/**
 * Disconnects from the broker (the session stays, so anything unacknowledged is redelivered later), closes the pool
 * and exits with `code`. A failed disconnect is logged, not thrown, so the process still exits.
 */
async function stopAndExit(runtime: Runtime, code: number): Promise<never> {
  runtime.shuttingDown = true;
  await runtime.subscriber?.close().catch((error) => runtime.log.warn({ err: error }, "disconnect failed"));
  await runtime.pool.end();
  process.exit(code);
}

/**
 * Handles SIGTERM/SIGINT: stops taking new deliveries, gives the pipeline the grace period to write and acknowledge
 * what's in hand, then disconnects and exits 0. A second signal while this runs is ignored.
 */
async function shutDownGracefully(runtime: Runtime, pipeline: IngestPipeline, graceMs: number, signal: string): Promise<void> {
  if (runtime.shuttingDown) return;
  runtime.log.info({ signal, waiting: pipeline.waiting() }, "shutting down: writing the messages in hand");
  runtime.shuttingDown = true;
  await pipeline.stop(graceMs);
  await stopAndExit(runtime, 0);
}

/** Logs one line with the period's counts, so operators (and the demo scripts) can see what ingest is doing. */
function logSummary(log: Logger, counters: ActivityCounters, pipeline: IngestPipeline, subscriber: Subscriber | undefined): void {
  log.info({ ...counters.takeAndReset(), waiting: pipeline.waiting(), unacked: subscriber?.gate.outstanding() ?? 0 }, "summary");
}

/** Builds the pipeline with logging, counting and metrics attached. `onFatal` exits, leaving the batch unacknowledged. */
function buildPipeline(config: Config, runtime: Runtime, counters: ActivityCounters, metrics: IngestMetrics): IngestPipeline {
  const log = runtime.log;
  return createIngestPipeline(
    createPostgresStore(runtime.pool),
    {
      batch: { maxRows: config.BATCH_MAX_ROWS, maxWaitMs: config.BATCH_MAX_WAIT_MS },
      backoff: { baseMs: config.RETRY_BASE_MS, capMs: config.RETRY_CAP_MS },
    },
    systemClock,
    {
      batchCommitted: (size, durationMs, attempts) => {
        counters.batch();
        metrics.batchCommitted(size, durationMs);
        log.debug({ size, durationMs, attempts }, "batch committed");
      },
      itemStored: (delivery, outcome, item) => {
        counters.stored(outcome);
        recordStored(metrics, delivery, outcome, item);
        if (outcome.kind === "dead_letter") log.warn({ topic: delivery.topic, reason: outcome.reason, detail: outcome.detail }, "dead-lettered");
      },
      retrying: (error, attempt, delayMs) => {
        counters.retry();
        metrics.retryStarted(Date.now());
        log.warn({ err: error.cause ?? error, attempt, delayMs }, "batch write failed, retrying");
      },
    },
    (error) => {
      log.error({ err: error, kind: error.kind }, "stopping: batch left unacknowledged for redelivery after restart");
      void stopAndExit(runtime, 1);
    },
  );
}

/** Records one stored message in the metrics: its outcome, its commit delay, and for a reading its lag. */
function recordStored(metrics: IngestMetrics, delivery: Delivery, outcome: ItemOutcome, item: StoreItem): void {
  metrics.commitDelaySeconds.observe((Date.now() - delivery.receivedAt.getTime()) / 1000);
  if (outcome.kind === "inserted" || outcome.kind === "duplicate") metrics.readingsStored.labels(outcome.kind).inc();
  else if (outcome.kind === "status") metrics.statusesStored.inc();
  else metrics.deadLetters.labels(outcome.reason).inc();
  if (outcome.kind === "inserted" && item.routed.type === "reading") metrics.observeLag(item.routed.row.receivedAt, item.routed.row.measuredAt);
}

/** Starts ingest and runs until a signal or a fatal error. */
async function main(): Promise<void> {
  const config = loadConfig(process.env);
  rmSync(config.READY_FILE, { force: true });
  const log = pino({ level: config.LOG_LEVEL, base: { service: "ingest", clientId: config.MQTT_CLIENT_ID } });
  log.info({ sinceProcessStartMs: Math.round(performance.now()) }, "starting");
  const pool = openIngestPool({ url: config.DATABASE_URL, size: config.DATABASE_POOL_SIZE }, (error) => log.warn({ err: error }, "idle database connection failed"));
  const runtime: Runtime = { log, pool, subscriber: undefined, shuttingDown: false };
  const counters = new ActivityCounters();
  const registry = createRegistry("ingest");
  let pipelineRef: IngestPipeline | undefined;
  const metrics = new IngestMetrics(registry, {
    unacked: () => runtime.subscriber?.gate.outstanding() ?? 0,
    waiting: () => pipelineRef?.waiting() ?? 0,
    brokerConnected: () => runtime.subscriber?.client.connected ?? false,
  }, Date.now);
  const pipeline = buildPipeline(config, runtime, counters, metrics);
  pipelineRef = pipeline;
  startMetricsServer(registry, config.METRICS_PORT);

  const connectStartedMs = Math.round(performance.now());
  log.info({ sinceProcessStartMs: connectStartedMs }, "connecting to broker");
  runtime.subscriber = await startSubscriber({
    url: config.MQTT_URL,
    clientId: config.MQTT_CLIENT_ID,
    receiveMaximum: config.RECEIVE_MAXIMUM,
    sessionExpirySeconds: config.SESSION_EXPIRY_SECONDS,
    keepaliveSeconds: config.KEEPALIVE_SECONDS,
    reconnectPeriodMs: config.RECONNECT_PERIOD_MS,
    ...(config.MQTT_CA_FILE && config.MQTT_CERT_FILE && config.MQTT_KEY_FILE
      ? { tls: { ca: readFileSync(config.MQTT_CA_FILE), cert: readFileSync(config.MQTT_CERT_FILE), key: readFileSync(config.MQTT_KEY_FILE) } }
      : {}),
    now: () => new Date(),
    onDelivery: (delivery) => {
      if (runtime.shuttingDown) return;
      counters.received(delivery.redelivered);
      metrics.received.labels(String(delivery.redelivered)).inc();
      metrics.lastReceivedTimestamp.set(delivery.receivedAt.getTime() / 1000);
      pipeline.enqueue(delivery);
    },
    onStaleAck: () => {
      counters.staleAck();
      metrics.staleAcks.inc();
    },
    onSubscribed: (ms) => metrics.connectToSubackSeconds.set(ms / 1000),
    onConnectionEvent: (event, detail) => log.info({ event, detail, sinceProcessStartMs: Math.round(performance.now()) }, "broker connection event"),
    onSubscribeError: (error) => {
      log.error({ err: error }, "re-subscribing after reconnect failed; exiting so the restart subscribes cleanly");
      void stopAndExit(runtime, 1);
    },
  });
  runtime.subscriber.client.on("reconnect", () => log.warn("broker connection lost, reconnecting"));
  runtime.subscriber.client.on("connect", () => log.info("connected to broker"));
  writeFileSync(config.READY_FILE, new Date().toISOString());
  metrics.startupSeconds.set(performance.now() / 1000);
  log.info({
    receiveMaximum: config.RECEIVE_MAXIMUM, batchMaxRows: config.BATCH_MAX_ROWS, batchMaxWaitMs: config.BATCH_MAX_WAIT_MS,
    sinceProcessStartMs: Math.round(performance.now()), firstConnectAttemptMs: connectStartedMs,
  }, "ingest running");

  setInterval(() => logSummary(log, counters, pipeline, runtime.subscriber), config.SUMMARY_INTERVAL_MS).unref();
  process.on("SIGTERM", () => void shutDownGracefully(runtime, pipeline, config.SHUTDOWN_GRACE_MS, "SIGTERM"));
  process.on("SIGINT", () => void shutDownGracefully(runtime, pipeline, config.SHUTDOWN_GRACE_MS, "SIGINT"));
}

await main();
