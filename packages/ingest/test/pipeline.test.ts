/**
 * Unit tests for the slice 3 pipeline and its pure parts, with a fake clock and a fake store: batching by size and by
 * time, acknowledging only after commit and in arrival order, retrying with full-jitter backoff, isolating a poison
 * row, and stopping on a fatal error.
 */
import { describe, expect, it } from "vitest";
import { fullJitterDelay } from "../src/core/backoff.ts";
import { Batcher } from "../src/core/batcher.ts";
import { writeIsolatingPoison } from "../src/core/isolatePoison.ts";
import { createIngestPipeline } from "../src/ingestPipeline.ts";
import type { PipelineClock, PipelineEvents } from "../src/ingestPipeline.ts";
import type { Delivery } from "../src/adapters/mqttSubscriber.ts";
import type { IngestStore, ItemOutcome, StoreItem } from "../src/adapters/postgresStore.ts";
import type { IngestError } from "../src/core/errors.ts";
import { seededRandom } from "./support/random.ts";

/** A clock whose time moves only when the test says so; timers and sleeps fire as time passes them. */
class FakeClock implements PipelineClock {
  time = 0;
  #timers: Array<{ at: number; fn: () => void }> = [];
  random = seededRandom(7);

  now = (): number => this.time;
  sleep = (ms: number): Promise<void> => new Promise((resolve) => this.#timers.push({ at: this.time + ms, fn: resolve }));
  schedule = (ms: number, fn: () => void): (() => void) => {
    const timer = { at: this.time + ms, fn };
    this.#timers.push(timer);
    return () => (this.#timers = this.#timers.filter((t) => t !== timer));
  };

  /** Moves time forward in 1 ms steps, firing due timers and letting promises settle after each. */
  async advance(ms: number): Promise<void> {
    for (let i = 0; i < ms; i++) {
      this.time++;
      const due = this.#timers.filter((t) => t.at <= this.time);
      this.#timers = this.#timers.filter((t) => t.at > this.time);
      due.forEach((t) => t.fn());
      await flush();
    }
  }
}

/** Lets pending promise callbacks run. */
async function flush(): Promise<void> {
  for (let i = 0; i < 10; i++) await Promise.resolve();
}

/** A store that records each batch it's asked to write; `failures` makes the next writes throw, in order. */
class FakeStore implements IngestStore {
  batches: string[][] = [];
  failures: unknown[] = [];
  poisonTopic: string | null = null;

  async writeBatch(items: StoreItem[]): Promise<ItemOutcome[]> {
    const failure = this.failures.shift();
    if (failure) throw failure;
    if (this.poisonTopic && items.some((i) => i.topic === this.poisonTopic && i.routed.type !== "dead_letter")) {
      throw Object.assign(new Error("invalid byte sequence"), { code: "22021" });
    }
    this.batches.push(items.map((i) => i.topic));
    return items.map((i) => (i.routed.type === "dead_letter" ? { kind: "dead_letter", reason: i.routed.row.reason, detail: null } : { kind: "inserted" }));
  }
}

const reading = (seq: number) => ({ deviceId: "dev-1", epoch: 1, seq, timestamp: "1970-01-01T00:00:00Z", kind: "Inverter", pvPowerW: 1 });

/** A delivery named after its seq; its ack is recorded in `acked`. */
function delivery(seq: number, acked: number[], topic = `homes/gw-1/devices/dev-1/telemetry`): Delivery {
  return {
    topic: seq < 0 ? `t${seq}` : topic,
    payload: new TextEncoder().encode(JSON.stringify(reading(Math.abs(seq)))),
    receivedAt: new Date(0),
    redelivered: false,
    ack: () => acked.push(seq),
  };
}

/** Builds a pipeline over the fake store and clock, recording retries and fatal errors. */
function setup(maxRows = 3, maxWaitMs = 200) {
  const store = new FakeStore();
  const clock = new FakeClock();
  const acked: number[] = [];
  const retries: number[] = [];
  const fatal: IngestError[] = [];
  const events: PipelineEvents = { batchCommitted: () => {}, itemStored: () => {}, retrying: (_e, _a, delay) => retries.push(delay) };
  const pipeline = createIngestPipeline(store, { batch: { maxRows, maxWaitMs }, backoff: { baseMs: 200, capMs: 30_000 } }, clock, events, (e) => fatal.push(e));
  return { store, clock, acked, retries, fatal, pipeline };
}

describe("fullJitterDelay", () => {
  it("stays below min(cap, base·2ⁿ)", () => {
    const random = seededRandom(1);
    for (let attempt = 0; attempt < 20; attempt++) {
      for (let i = 0; i < 200; i++) {
        expect(fullJitterDelay(attempt, { baseMs: 200, capMs: 30_000 }, random)).toBeLessThan(Math.min(30_000, 200 * 2 ** attempt));
      }
    }
  });

  it("doesn't synchronise two replicas: their delays for the same attempt are uncorrelated and spread over the range", () => {
    const [a, b] = [seededRandom(11), seededRandom(12)];
    const settings = { baseMs: 200, capMs: 30_000 };
    const pairs = Array.from({ length: 2000 }, () => [fullJitterDelay(10, settings, a), fullJitterDelay(10, settings, b)] as const);
    const mean = (xs: number[]) => xs.reduce((s, x) => s + x, 0) / xs.length;
    const xs = pairs.map((p) => p[0]);
    const ys = pairs.map((p) => p[1]);
    const [mx, my] = [mean(xs), mean(ys)];
    const covariance = mean(pairs.map(([x, y]) => (x - mx) * (y - my)));
    const correlation = covariance / Math.sqrt(mean(xs.map((x) => (x - mx) ** 2)) * mean(ys.map((y) => (y - my) ** 2)));
    expect(Math.abs(correlation)).toBeLessThan(0.1);
    expect(pairs.filter(([x, y]) => Math.abs(x - y) < 100).length / pairs.length).toBeLessThan(0.02);
    expect(mx).toBeGreaterThan(13_000);
    expect(mx).toBeLessThan(17_000);
  });
});

describe("Batcher", () => {
  it("is ready at maxRows, or when the oldest item has waited maxWaitMs", () => {
    const batcher = new Batcher<number>({ maxRows: 3, maxWaitMs: 200 });
    batcher.add(1, 0);
    batcher.add(2, 50);
    expect(batcher.isReady(100)).toBe(false);
    expect(batcher.msUntilDue(100)).toBe(100);
    expect(batcher.isReady(200)).toBe(true);
    batcher.add(3, 60);
    expect(batcher.isReady(61)).toBe(true);
    expect(batcher.take()).toEqual([1, 2, 3]);
  });
});

describe("writeIsolatingPoison", () => {
  it("isolates one bad row by halving, commits every other row, and keeps the order", async () => {
    const written: number[][] = [];
    const items = Array.from({ length: 16 }, (_, i) => i);
    const outcomes = await writeIsolatingPoison(items, {
      write: async (part) => {
        if (part.includes(11)) throw Object.assign(new Error("bad"), { code: "23514" });
        written.push(part);
        return part.map((i) => `ok-${i}`);
      },
      onPoison: async (item) => `dead-${item}`,
    });
    expect(outcomes).toEqual(items.map((i) => (i === 11 ? "dead-11" : `ok-${i}`)));
    expect(written.flat().sort((a, b) => a - b)).toEqual(items.filter((i) => i !== 11));
    expect(written.length).toBeLessThanOrEqual(2 * Math.log2(16));
  });

  it("does not split on a retryable error; it rethrows it classified", async () => {
    await expect(writeIsolatingPoison([1, 2], {
      write: async () => { throw Object.assign(new Error("down"), { code: "ECONNREFUSED" }); },
      onPoison: async () => "dead",
    })).rejects.toMatchObject({ kind: "retryable" });
  });
});

describe("ingest pipeline", () => {
  it("writes a batch at maxRows, and acknowledges only after the write, in arrival order", async () => {
    const { store, acked, pipeline } = setup(3);
    [1, 2, 3, 4].forEach((seq) => pipeline.enqueue(delivery(seq, acked)));
    await flush();
    expect(store.batches.map((b) => b.length)).toEqual([3]);
    expect(acked).toEqual([1, 2, 3]);
  });

  it("writes a partial batch when its oldest message has waited maxWaitMs", async () => {
    const { store, clock, acked, pipeline } = setup(500, 200);
    pipeline.enqueue(delivery(1, acked));
    await clock.advance(199);
    expect(acked).toEqual([]);
    await clock.advance(1);
    expect(store.batches.length).toBe(1);
    expect(acked).toEqual([1]);
  });

  it("retries a failing batch with backoff, acknowledges nothing meanwhile, and keeps collecting new messages", async () => {
    const { store, clock, acked, retries, pipeline } = setup(2);
    const down = Object.assign(new Error("connection refused"), { code: "ECONNREFUSED" });
    store.failures = [down, down, down];
    [1, 2, 3, 4, 5].forEach((seq) => pipeline.enqueue(delivery(seq, acked)));
    await flush();
    expect(acked).toEqual([]);
    await clock.advance(5000);
    expect(retries.length).toBe(3);
    expect(retries[0]).toBeLessThan(200);
    expect(retries[1]).toBeLessThan(400);
    expect(acked).toEqual([1, 2, 3, 4, 5]);
    expect(store.batches.map((b) => b.length)).toEqual([2, 2, 1]);
    expect(pipeline.waiting()).toBe(0);
  });

  it("dead-letters only the poison row of a batch and acknowledges the whole batch", async () => {
    const { store, acked, pipeline } = setup(4);
    store.poisonTopic = "t-3";
    [1, 2, -3, 4].forEach((seq) => pipeline.enqueue(delivery(seq, acked)));
    await flush();
    expect(acked).toEqual([1, 2, -3, 4]);
    expect(store.batches.flat()).toContain("t-3");
  });

  it("stops on a fatal error, leaves the batch unacknowledged, and takes no more work", async () => {
    const { store, acked, fatal, pipeline } = setup(2);
    store.failures = [Object.assign(new Error("relation does not exist"), { code: "42P01" })];
    [1, 2].forEach((seq) => pipeline.enqueue(delivery(seq, acked)));
    await flush();
    pipeline.enqueue(delivery(3, acked));
    await flush();
    expect(acked).toEqual([]);
    expect(fatal.map((f) => f.kind)).toEqual(["fatal"]);
  });

  it("on stop, writes and acknowledges what's in hand without waiting for a full batch", async () => {
    const { clock, acked, pipeline } = setup(500, 10_000);
    pipeline.enqueue(delivery(1, acked));
    const stopping = pipeline.stop(1000);
    await clock.advance(50);
    await stopping;
    expect(acked).toEqual([1]);
  });

  it("on stop with the database down, gives up after the grace period and leaves the batch unacknowledged", async () => {
    const { store, clock, acked, fatal, pipeline } = setup(500, 10_000);
    store.failures = Array.from({ length: 100 }, () => Object.assign(new Error("down"), { code: "ECONNREFUSED" }));
    pipeline.enqueue(delivery(1, acked));
    const stopping = pipeline.stop(1000);
    await clock.advance(3000);
    await stopping;
    expect(acked).toEqual([]);
    expect(fatal).toEqual([]);
  });
});
