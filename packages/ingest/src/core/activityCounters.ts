/**
 * Counts what ingest did since the last summary: messages received and redelivered, readings inserted and duplicate,
 * dead letters by reason, batches, retries, and discarded stale acks. Logged as one line every few seconds until
 * slice 6 turns these into Prometheus metrics. The demo scripts read them, for example "DUP redeliveries > 0" in
 * scenario 7.
 */

/** One summary period's counts. */
export interface ActivityCounts {
  received: number;
  redelivered: number;
  inserted: number;
  duplicate: number;
  statuses: number;
  deadLettered: Record<string, number>;
  batches: number;
  retries: number;
  staleAcks: number;
}

/** Accumulates counts, and hands them over and resets them in one step. */
export class ActivityCounters {
  #counts: ActivityCounts = ActivityCounters.#empty();

  /** A zeroed set of counts. */
  static #empty(): ActivityCounts {
    return { received: 0, redelivered: 0, inserted: 0, duplicate: 0, statuses: 0, deadLettered: {}, batches: 0, retries: 0, staleAcks: 0 };
  }

  /** Counts a received message, and whether the broker marked it as a redelivery. */
  received(redelivered: boolean): void {
    this.#counts.received++;
    if (redelivered) this.#counts.redelivered++;
  }

  /** Counts a stored message by outcome; a dead letter is counted under its reason. */
  stored(outcome: { kind: string; reason?: string }): void {
    if (outcome.kind === "inserted") this.#counts.inserted++;
    else if (outcome.kind === "duplicate") this.#counts.duplicate++;
    else if (outcome.kind === "status") this.#counts.statuses++;
    else {
      const reason = outcome.reason ?? "unknown";
      this.#counts.deadLettered[reason] = (this.#counts.deadLettered[reason] ?? 0) + 1;
    }
  }

  /** Counts a committed batch. */
  batch(): void {
    this.#counts.batches++;
  }

  /** Counts a retry of a failed batch write. */
  retry(): void {
    this.#counts.retries++;
  }

  /** Counts an acknowledgement discarded because its connection had closed. */
  staleAck(): void {
    this.#counts.staleAcks++;
  }

  /** Returns the counts so far and starts a new period. */
  takeAndReset(): ActivityCounts {
    const counts = this.#counts;
    this.#counts = ActivityCounters.#empty();
    return counts;
  }
}
