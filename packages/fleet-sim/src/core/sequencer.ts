/**
 * Numbers a virtual gateway's readings exactly as the real gateway does: `epoch` is the boot number, and
 * `seq` counts up by one per device and carries on across restarts within the same power state.
 */

/** Hands out (epoch, seq) pairs for one gateway. */
export class Sequencer {
  #epoch: number;
  readonly #nextSeq = new Map<string, number>();

  /** Starts at boot `epoch`; every device's first seq is 1. */
  constructor(epoch: number) {
    this.#epoch = epoch;
  }

  /** The current boot number. */
  epoch(): number {
    return this.#epoch;
  }

  /**
   * A power loss: the gateway reboots into the next epoch and its seq counters lose their last `rollback` values
   * (the real gateway persists seq, but a power cut can lose the most recent writes). Seq never goes below 1, and it
   * only ever goes back together with a new epoch, which is the contract's promise.
   */
  powerLoss(rollback: number): void {
    this.#epoch++;
    for (const [deviceId, next] of this.#nextSeq) this.#nextSeq.set(deviceId, Math.max(1, next - rollback));
  }

  /** The (epoch, seq) for the device's next reading, and advances its counter. */
  next(deviceId: string): { epoch: number; seq: number } {
    const seq = this.#nextSeq.get(deviceId) ?? 1;
    this.#nextSeq.set(deviceId, seq + 1);
    return { epoch: this.#epoch, seq };
  }
}
