/**
 * The ingest pipeline: route each delivery as it arrives, collect deliveries into batches (500 rows or
 * 200 ms, D5), write each batch in one transaction, and acknowledge its messages, in arrival order, only after the
 * commit (D3).
 *
 * The backpressure chain starts here. While a write is failing, the pipeline retries the same batch with full-jitter
 * backoff and acknowledges nothing. New deliveries wait in the batcher, which can't hold more than Receive Maximum
 * because each is unacknowledged. Then the broker stops sending and queues instead.
 *
 * Only one batch is written at a time, and batches go in order, so acknowledgements stay in arrival order.
 */
import { fullJitterDelay } from "./core/backoff.ts";
import type { BackoffSettings } from "./core/backoff.ts";
import { Batcher } from "./core/batcher.ts";
import type { BatchSettings } from "./core/batcher.ts";
import { IngestError } from "./core/errors.ts";
import { writeIsolatingPoison } from "./core/isolatePoison.ts";
import { routeMessage } from "./core/routeMessage.ts";
import type { Delivery } from "./adapters/mqttSubscriber.ts";
import type { IngestStore, ItemOutcome, StoreItem } from "./adapters/postgresStore.ts";

/** Time and randomness, injected so tests control both. */
export interface PipelineClock {
  /** Current time in ms. */
  now: () => number;
  /** Resolves after `ms`. */
  sleep: (ms: number) => Promise<void>;
  /** Calls `fn` once after `ms`; returns a function that cancels it. */
  schedule: (ms: number, fn: () => void) => () => void;
  /** A value in [0, 1), for backoff jitter. */
  random: () => number;
}

/** What the pipeline reports, for logs now and metrics in slice 6. */
export interface PipelineEvents {
  /** A batch committed: its size, how long the write took, and how many attempts it needed. */
  batchCommitted: (size: number, durationMs: number, attempts: number) => void;
  /** One message's outcome, after its batch committed and before it is acknowledged, with the item that was written. */
  itemStored: (delivery: Delivery, outcome: ItemOutcome, item: StoreItem) => void;
  /** A batch write failed with a retryable error; the next attempt starts after `delayMs`. */
  retrying: (error: IngestError, attempt: number, delayMs: number) => void;
}

/** Settings for batching and retrying. */
export interface PipelineSettings {
  batch: BatchSettings;
  backoff: BackoffSettings;
  /** How many days of readings are kept; older ones are dead-lettered as too_old (S8-2). */
  retentionDays: number;
}

/** A running pipeline. */
export interface IngestPipeline {
  /** Routes the delivery and adds it to the current batch. Ignored once the pipeline is stopping or has failed. */
  enqueue: (delivery: Delivery) => void;
  /**
   * Stops taking deliveries and writes what's already in hand (acknowledging it), giving up after `graceMs`.
   * Whatever isn't written by then stays unacknowledged, and the broker redelivers it after restart.
   */
  stop: (graceMs: number) => Promise<void>;
  /** Number of deliveries routed but not yet written. */
  waiting: () => number;
}

/** A routed delivery, waiting to be written. */
interface Pending {
  delivery: Delivery;
  item: StoreItem;
}

/** Creates the pipeline. `onFatal` is called once if a write fails with a fatal error; the process should then exit. */
export function createIngestPipeline(
  store: IngestStore,
  settings: PipelineSettings,
  clock: PipelineClock,
  events: PipelineEvents,
  onFatal: (error: IngestError) => void,
): IngestPipeline {
  const batcher = new Batcher<Pending>(settings.batch);
  const state = { writing: false, stopping: false, failed: false, cancelTimer: undefined as (() => void) | undefined };

  /** Starts writing if a batch is ready and nothing is being written; otherwise makes sure a timer will check again. */
  function kick(): void {
    if (state.writing || state.failed) return;
    if (batcher.isReady(clock.now()) || (state.stopping && batcher.size() > 0)) {
      state.cancelTimer?.();
      state.cancelTimer = undefined;
      state.writing = true;
      void writeNextBatch().finally(() => {
        state.writing = false;
        kick();
      });
      return;
    }
    const due = batcher.msUntilDue(clock.now());
    if (due !== null && !state.cancelTimer) {
      state.cancelTimer = clock.schedule(due, () => {
        state.cancelTimer = undefined;
        kick();
      });
    }
  }

  /** Takes the oldest batch, writes it (retrying), and acknowledges it. A fatal error stops the pipeline. */
  async function writeNextBatch(): Promise<void> {
    const batch = batcher.take();
    try {
      const outcomes = await writeWithRetry(store, batch.map((p) => p.item), settings.backoff, clock, events, () => state.stopping && stopDeadlinePassed());
      batch.forEach((pending, i) => {
        events.itemStored(pending.delivery, outcomes[i] as ItemOutcome, pending.item);
        pending.delivery.ack();
      });
    } catch (error) {
      state.failed = true;
      if (!(error instanceof GaveUp)) onFatal(error instanceof IngestError ? error : new IngestError("fatal", "unexpected write failure", error));
    }
  }

  let stopDeadline = Infinity;
  /** True once a graceful stop has used up its grace period. */
  function stopDeadlinePassed(): boolean {
    return clock.now() > stopDeadline;
  }

  return {
    enqueue: (delivery) => {
      if (state.stopping || state.failed) return;
      const routed = routeMessage(delivery.topic, delivery.payload, delivery.receivedAt, settings.retentionDays);
      batcher.add({ delivery, item: { routed, topic: delivery.topic, payload: delivery.payload } }, clock.now());
      kick();
    },
    stop: async (graceMs) => {
      state.stopping = true;
      stopDeadline = clock.now() + graceMs;
      kick();
      while ((state.writing || batcher.size() > 0) && !state.failed && !stopDeadlinePassed()) await clock.sleep(20);
    },
    waiting: () => batcher.size(),
  };
}

/** Thrown when a graceful stop's deadline ends the retries; the batch is left unacknowledged on purpose. */
class GaveUp extends Error {}

/**
 * Writes one batch, isolating poison rows (they become `rejected_by_database` dead letters) and retrying the whole
 * batch with full-jitter backoff while the error is retryable. Rethrows a fatal error. Gives up only if `shouldGiveUp`
 * says so (a graceful stop past its deadline).
 */
async function writeWithRetry(
  store: IngestStore,
  items: StoreItem[],
  backoff: BackoffSettings,
  clock: PipelineClock,
  events: PipelineEvents,
  shouldGiveUp: () => boolean,
): Promise<ItemOutcome[]> {
  for (let attempt = 0; ; attempt++) {
    const started = clock.now();
    try {
      const outcomes = await writeIsolatingPoison(items, {
        write: (part) => store.writeBatch(part),
        onPoison: (item, error) => deadLetterRejected(store, item, error),
      });
      events.batchCommitted(items.length, clock.now() - started, attempt + 1);
      return outcomes;
    } catch (error) {
      if (!(error instanceof IngestError) || error.kind !== "retryable") throw error;
      if (shouldGiveUp()) throw new GaveUp();
      const delayMs = fullJitterDelay(attempt, backoff, clock.random);
      events.retrying(error, attempt + 1, delayMs);
      await clock.sleep(delayMs);
    }
  }
}

/** Stores a message the database refused as a `rejected_by_database` dead letter, and returns that outcome. */
async function deadLetterRejected(store: IngestStore, item: StoreItem, error: unknown): Promise<ItemOutcome> {
  const detail = error instanceof Error ? error.message : String(error);
  const receivedAt = item.routed.row.receivedAt;
  const [outcome] = await store.writeBatch([{
    routed: { type: "dead_letter", row: { topic: item.topic, payload: item.payload, reason: "rejected_by_database", detail, receivedAt } },
    topic: item.topic,
    payload: item.payload,
  }]);
  return outcome as ItemOutcome;
}
