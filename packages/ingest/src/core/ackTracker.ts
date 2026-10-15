/**
 * Keeps acknowledgements in arrival order (MQTT requires QoS 1 PUBACKs in the order the PUBLISHes arrived).
 * Messages can finish in any order; a finished message is only released once every message that arrived before it
 * has finished too. Pure: it knows nothing about MQTT or sockets.
 */

/**
 * Tracks one connection's messages from arrival to release. Each message gets a position when it arrives;
 * `complete` returns the items that may now be acknowledged, oldest first.
 */
export class AckTracker<T> {
  #nextPosition = 0;
  #nextToRelease = 0;
  readonly #completed = new Map<number, T>();

  /** Records an arrival and returns its position, which the caller passes back to `complete`. */
  track(): number {
    return this.#nextPosition++;
  }

  /**
   * Marks the message at `position` as finished and returns every item that can now be released in arrival order.
   * Returns an empty list while an earlier message is still unfinished. Completing a position twice, or one that
   * was never tracked, is a bug and throws.
   */
  complete(position: number, item: T): T[] {
    if (position >= this.#nextPosition || position < this.#nextToRelease || this.#completed.has(position)) {
      throw new Error(`position ${position} is not outstanding`);
    }
    this.#completed.set(position, item);
    return this.#releaseReadyPrefix();
  }

  /** Number of messages that have arrived but not been released yet. */
  outstanding(): number {
    return this.#nextPosition - this.#nextToRelease;
  }

  /** Pops the run of completed items at the head of the order. */
  #releaseReadyPrefix(): T[] {
    const ready: T[] = [];
    while (this.#completed.has(this.#nextToRelease)) {
      ready.push(this.#completed.get(this.#nextToRelease) as T);
      this.#completed.delete(this.#nextToRelease);
      this.#nextToRelease++;
    }
    return ready;
  }
}
