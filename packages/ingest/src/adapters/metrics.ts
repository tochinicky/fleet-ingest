/**
 * Ingest's Prometheus metrics: what came in, what was stored or dead-lettered, how batches and retries
 * behave, how far behind ingest is, and how the broker connection is doing. `main.ts` feeds these from the pipeline's
 * and the subscriber's events. The pure core never touches them.
 */
import { Counter, Gauge, Histogram } from "@fleet-ingest/observability";
import type { Registry } from "@fleet-ingest/observability";
import type { DeadLetterReason } from "../core/rows.ts";

/** Every dead-letter reason, so each counter series exists at 0 from startup (see `#initialiseLabels`). */
const DEAD_LETTER_REASONS: DeadLetterReason[] = [
  "unknown_topic", "invalid_json", "schema_mismatch", "device_id_mismatch", "future_timestamp", "foreign_device", "rejected_by_database",
];

/** Seconds: from tens of milliseconds to the 30 s retry cap and beyond, for lag-like histograms. */
const LAG_BUCKETS = [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120, 300, 900, 3600];

/** Live values the gauges read when Prometheus scrapes. */
export interface IngestGaugeSources {
  unacked: () => number;
  waiting: () => number;
  brokerConnected: () => boolean;
}

/** Every ingest metric, created on one registry. */
export class IngestMetrics {
  readonly received: Counter<"redelivered">;
  readonly readingsStored: Counter<"outcome">;
  readonly statusesStored: Counter;
  readonly deadLetters: Counter<"reason">;
  readonly staleAcks: Counter;
  readonly retries: Counter;
  readonly batchSize: Histogram;
  readonly batchWriteSeconds: Histogram;
  readonly lagSeconds: Histogram;
  readonly commitDelaySeconds: Histogram;
  readonly connectToSubackSeconds: Gauge;
  readonly startupSeconds: Gauge;
  readonly lastReceivedTimestamp: Gauge;
  #retryingSince: number | null = null;

  /** Registers every metric on `registry`; gauges for live values read `sources` at scrape time. */
  constructor(registry: Registry, sources: IngestGaugeSources, now: () => number) {
    const registers = [registry];
    this.received = new Counter({ name: "ingest_messages_received_total", help: "Messages received from the broker.", labelNames: ["redelivered"], registers });
    this.readingsStored = new Counter({ name: "ingest_readings_stored_total", help: "Readings written, by outcome: inserted (new) or duplicate (already stored).", labelNames: ["outcome"], registers });
    this.statusesStored = new Counter({ name: "ingest_statuses_stored_total", help: "Gateway status messages written.", registers });
    this.deadLetters = new Counter({ name: "ingest_dead_letters_total", help: "Messages dead-lettered, by reason.", labelNames: ["reason"], registers });
    this.staleAcks = new Counter({ name: "ingest_stale_acks_total", help: "Acks discarded because their connection had closed (the broker redelivers).", registers });
    this.retries = new Counter({ name: "ingest_db_retries_total", help: "Batch writes retried after a retryable database error.", registers });
    this.batchSize = new Histogram({ name: "ingest_batch_size", help: "Messages per committed batch.", buckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000], registers });
    this.batchWriteSeconds = new Histogram({ name: "ingest_batch_write_seconds", help: "Time to write one batch, including retries.", buckets: LAG_BUCKETS, registers });
    this.lagSeconds = new Histogram({
      name: "ingest_lag_seconds", buckets: LAG_BUCKETS, registers,
      help: "received_at − measured_at per new reading (compares the gateway's clock with ingest's). Backfill is counted by the API's health job, which knows when each gateway came back (S6-1).",
    });
    this.commitDelaySeconds = new Histogram({
      name: "ingest_commit_delay_seconds", help: "From ingest receiving a message to its batch committing: grows while the database is slow or down.",
      buckets: LAG_BUCKETS, registers,
    });
    this.connectToSubackSeconds = new Gauge({ name: "ingest_connect_to_suback_seconds", help: "Last broker connect: CONNECT sent → both subscriptions acknowledged (S3-9 window).", registers });
    this.startupSeconds = new Gauge({ name: "ingest_startup_seconds", help: "Process start → subscribed and running (S4-6).", registers });
    this.lastReceivedTimestamp = new Gauge({ name: "ingest_last_message_received_timestamp_seconds", help: "When ingest last received a message (IngestStalled).", registers });
    this.#initialiseLabels();
    new Gauge({ name: "ingest_unacked_messages", help: "Messages received but not acknowledged (bounded by Receive Maximum).", registers, collect() { this.set(sources.unacked()); } });
    new Gauge({ name: "ingest_waiting_messages", help: "Messages routed and waiting to be written.", registers, collect() { this.set(sources.waiting()); } });
    new Gauge({ name: "ingest_broker_connected", help: "1 while connected to the broker.", registers, collect() { this.set(sources.brokerConnected() ? 1 : 0); } });
    const retrying = () => this.retryingSeconds(now());
    new Gauge({
      name: "ingest_batch_retrying_seconds", help: "How long the current batch has been failing; 0 when writes succeed.", registers,
      collect() { this.set(retrying()); },
    });
  }

  /**
   * Creates every known label combination at 0 (`inc(0)`: in prom-client, `labels()` alone exports nothing). A counter series that first appears already at 1 has no earlier
   * sample, so `increase()` and `rate()` can't see that first event, and an alert on it would never fire (S6-4).
   */
  #initialiseLabels(): void {
    for (const redelivered of ["true", "false"]) this.received.labels(redelivered).inc(0);
    for (const outcome of ["inserted", "duplicate"]) this.readingsStored.labels(outcome).inc(0);
    for (const reason of DEAD_LETTER_REASONS) this.deadLetters.labels(reason).inc(0);
  }

  /** Seconds since the current batch first failed, or 0 when the last write succeeded. */
  retryingSeconds(nowMs: number): number {
    return this.#retryingSince === null ? 0 : (nowMs - this.#retryingSince) / 1000;
  }

  /** A batch write failed and will be retried; starts the retrying clock on the first failure. */
  retryStarted(atMs: number): void {
    this.retries.inc();
    this.#retryingSince ??= atMs;
  }

  /** A batch committed: records its size and write time, and stops the retrying clock. */
  batchCommitted(size: number, durationMs: number): void {
    this.batchSize.observe(size);
    this.batchWriteSeconds.observe(durationMs / 1000);
    this.#retryingSince = null;
  }

  /** Records one reading's lag (received − measured). */
  observeLag(receivedAt: Date, measuredAt: Date): void {
    this.lagSeconds.observe(Math.max(0, (receivedAt.getTime() - measuredAt.getTime()) / 1000));
  }
}
