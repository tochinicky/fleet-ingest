/**
 * Retry delays for a failed batch write: exponential backoff with **full jitter**.
 * The delay for attempt n is a uniformly random value in [0, min(cap, base × 2ⁿ)). The randomness is the point: two
 * replicas that lost the database at the same moment don't come back at the same moment, so a recovering database
 * isn't hit by synchronised retry waves (the "thundering herd").
 *
 * There's no circuit breaker in front of the single database: while it's down, nothing else is waiting on ingest,
 * so retrying with a capped delay already is the breaker (decisions.md S3-3).
 */

/** Backoff settings; the defaults are base 200 ms and cap 30 s. */
export interface BackoffSettings {
  baseMs: number;
  capMs: number;
}

/**
 * The delay before retry number `attempt` (0 for the first retry). `random` returns a value in [0, 1) and is
 * injected, so tests can make it deterministic.
 */
export function fullJitterDelay(attempt: number, settings: BackoffSettings, random: () => number): number {
  const ceiling = Math.min(settings.capMs, settings.baseMs * 2 ** Math.min(attempt, 30));
  return Math.floor(random() * ceiling);
}
