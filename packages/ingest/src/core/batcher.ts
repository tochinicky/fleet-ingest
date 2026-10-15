/**
 * Collects messages into batches (D5): a batch is ready at `maxRows` items, or `maxWaitMs` after its oldest item
 * arrived, whichever comes first. Pure: the caller passes the current time and owns the timer.
 *
 * The batcher has no capacity limit of its own, and doesn't need one. Every item in it is an unacknowledged
 * message, and the broker never sends more than Receive Maximum of those. So the batcher holds at most Receive
 * Maximum items (D3).
 */

/** Batch limits; the defaults are 500 rows and 200 ms. */
export interface BatchSettings {
  maxRows: number;
  maxWaitMs: number;
}

/** A first-in, first-out collection that knows when its oldest items form a ready batch. */
export class Batcher<T> {
  readonly #items: Array<{ item: T; arrivedAt: number }> = [];
  readonly #settings: BatchSettings;

  /** Creates an empty batcher. */
  constructor(settings: BatchSettings) {
    if (settings.maxRows < 1 || settings.maxWaitMs < 0) throw new Error("invalid batch settings");
    this.#settings = settings;
  }

  /** Adds an item that arrived at `nowMs`. */
  add(item: T, nowMs: number): void {
    this.#items.push({ item, arrivedAt: nowMs });
  }

  /** True when a batch should be taken now: enough items, or the oldest has waited long enough. */
  isReady(nowMs: number): boolean {
    const oldest = this.#items[0];
    if (!oldest) return false;
    return this.#items.length >= this.#settings.maxRows || nowMs - oldest.arrivedAt >= this.#settings.maxWaitMs;
  }

  /** Milliseconds until the oldest item's wait runs out (0 if already due), or null when empty. */
  msUntilDue(nowMs: number): number | null {
    const oldest = this.#items[0];
    return oldest ? Math.max(0, oldest.arrivedAt + this.#settings.maxWaitMs - nowMs) : null;
  }

  /** Removes and returns up to `maxRows` of the oldest items, in arrival order. */
  take(): T[] {
    return this.#items.splice(0, this.#settings.maxRows).map((entry) => entry.item);
  }

  /** Number of items waiting. */
  size(): number {
    return this.#items.length;
  }
}
