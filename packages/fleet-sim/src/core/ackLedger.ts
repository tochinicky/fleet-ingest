/**
 * Records which readings the broker acknowledged (PUBACK), per device and epoch, as compact seq ranges. This is the
 * ground truth for the scenario checks: every reading in the ledger was handed over to the cloud, so every one must
 * end up stored exactly once (scenarios 3 and 7). Ranges keep memory small: a device that sends seq 1..10,000
 * without gaps takes one range, not 10,000 entries.
 */

/** One device's acknowledged readings in one epoch. */
export interface LedgerEntry {
  deviceId: string;
  epoch: number;
  /** Inclusive [from, to] seq ranges, sorted and non-overlapping. */
  ranges: Array<[number, number]>;
  /** How many distinct seqs the ranges cover. */
  count: number;
}

/** A set of integers stored as sorted, merged inclusive ranges. */
export class RangeSet {
  #ranges: Array<[number, number]> = [];

  /** Adds `value`; adding a value already present changes nothing. */
  add(value: number): void {
    const ranges = this.#ranges;
    let low = 0;
    let high = ranges.length;
    while (low < high) {
      const mid = (low + high) >> 1;
      if ((ranges[mid] as [number, number])[1] < value - 1) low = mid + 1;
      else high = mid;
    }
    const at = ranges[low];
    if (!at || at[0] > value + 1) {
      ranges.splice(low, 0, [value, value]);
      return;
    }
    at[0] = Math.min(at[0], value);
    at[1] = Math.max(at[1], value);
    const next = ranges[low + 1];
    if (next && next[0] <= at[1] + 1) {
      at[1] = Math.max(at[1], next[1]);
      ranges.splice(low + 1, 1);
    }
  }

  /** A copy of the ranges. */
  ranges(): Array<[number, number]> {
    return this.#ranges.map(([from, to]) => [from, to]);
  }

  /** Number of integers in the set. */
  count(): number {
    return this.#ranges.reduce((sum, [from, to]) => sum + to - from + 1, 0);
  }
}

/**
 * The fleet's ledger: one {@link RangeSet} per (device, epoch) for readings, and a count per gateway of the status
 * messages the broker acknowledged. Statuses are queued for ingest like readings, so when the broker drops, it drops
 * both. Scenario 3b needs both counts to match the broker's drop counter exactly.
 */
export class AckLedger {
  readonly #sets = new Map<string, { deviceId: string; epoch: number; seqs: RangeSet }>();
  readonly #statuses = new Map<string, number>();

  /** Records that the broker acknowledged this reading. Recording it twice (a resend) is harmless. */
  record(deviceId: string, epoch: number, seq: number): void {
    const key = `${deviceId}\u0000${epoch}`;
    let entry = this.#sets.get(key);
    if (!entry) {
      entry = { deviceId, epoch, seqs: new RangeSet() };
      this.#sets.set(key, entry);
    }
    entry.seqs.add(seq);
  }

  /** Records that the broker acknowledged one status message from this gateway. */
  recordStatus(gatewayId: string): void {
    this.#statuses.set(gatewayId, (this.#statuses.get(gatewayId) ?? 0) + 1);
  }

  /** Acknowledged status messages: per gateway, and in total. */
  statuses(): { total: number; byGateway: Record<string, number> } {
    const byGateway = Object.fromEntries(this.#statuses);
    return { total: [...this.#statuses.values()].reduce((s, n) => s + n, 0), byGateway };
  }

  /** Every device and epoch with its acknowledged ranges. */
  entries(): LedgerEntry[] {
    return [...this.#sets.values()].map(({ deviceId, epoch, seqs }) => ({ deviceId, epoch, ranges: seqs.ranges(), count: seqs.count() }));
  }
}
