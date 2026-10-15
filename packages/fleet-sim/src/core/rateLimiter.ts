/**
 * An even-pace rate limit, the same as the real gateway's replay limit (200 msg/s by default): it doesn't allow
 * bursts, it spaces sends out evenly. Pure: it is given the current time and says how long to wait.
 */

/** Spaces events at least `1000 / perSecond` ms apart. */
export class EvenPace {
  readonly #intervalMs: number;
  #nextAllowedAt = 0;

  /** Creates a pace of `perSecond` events per second. */
  constructor(perSecond: number) {
    if (!(perSecond > 0)) throw new Error("perSecond must be positive");
    this.#intervalMs = 1000 / perSecond;
  }

  /** Claims the next slot. Returns how many ms to wait before using it (0 if it's already due). */
  claim(nowMs: number): number {
    const at = Math.max(nowMs, this.#nextAllowedAt);
    this.#nextAllowedAt = at + this.#intervalMs;
    return at - nowMs;
  }
}
