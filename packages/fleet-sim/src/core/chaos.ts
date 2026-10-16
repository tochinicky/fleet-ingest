/**
 * Chaos knobs for the scenarios (scenarios 1 and 5), pure: they decide, the adapters publish. All randomness
 * comes from an injected `random`, so a seeded run is repeatable.
 * - **Duplicates:** a reading is put in the outbox twice, so the broker gets two copies with the same key.
 * - **Reordering:** readings wait in a window and leave it in random order.
 * - **Poison:** an extra message, never a real reading, so the reading stream itself keeps no gaps. One kind per dead
 *   letter reason ingest knows (D7), so scenario 5 can check each one.
 */
import type { TelemetryMessage } from "@fleet-ingest/contract";

/** The poison kinds, named after the dead-letter reason each one must produce. */
export const POISON_KINDS = ["invalid_json", "schema_mismatch", "device_id_mismatch", "future_timestamp", "too_old", "foreign_device"] as const;

/** One poison kind. */
export type PoisonKind = (typeof POISON_KINDS)[number];

/** The knobs, all off by default. */
export interface ChaosSettings {
  /** Probability in [0, 1] that a reading is sent twice. */
  duplicateRate: number;
  /** Readings held back and released in random order; 0 or 1 means in order. */
  reorderWindow: number;
  /** Probability in [0, 1] that a reading is followed by one poison message. */
  poisonRate: number;
}

/** Everything off. */
export const NO_CHAOS: ChaosSettings = { duplicateRate: 0, reorderWindow: 0, poisonRate: 0 };

/** Validates a settings update from the control API; returns the merged settings, or an error message. */
export function parseChaosSettings(body: unknown, current: ChaosSettings): ChaosSettings | string {
  if (typeof body !== "object" || body === null) return "body must be a JSON object";
  const next = { ...current, ...(body as Partial<ChaosSettings>) };
  for (const key of ["duplicateRate", "poisonRate"] as const) {
    if (typeof next[key] !== "number" || !(next[key] >= 0 && next[key] <= 1)) return `${key} must be a number in [0, 1]`;
  }
  if (!Number.isInteger(next.reorderWindow) || next.reorderWindow < 0 || next.reorderWindow > 10_000) return "reorderWindow must be an integer in [0, 10000]";
  return { duplicateRate: next.duplicateRate, reorderWindow: next.reorderWindow, poisonRate: next.poisonRate };
}

/** What happens to one reading: how many copies go to the outbox, and which poison (if any) follows it. */
export interface ChaosDecision {
  copies: 1 | 2;
  poison: PoisonKind | null;
}

/** Draws the decision for one reading. */
export function decide(settings: ChaosSettings, random: () => number): ChaosDecision {
  const copies = random() < settings.duplicateRate ? 2 : 1;
  const poison = random() < settings.poisonRate ? POISON_KINDS[Math.floor(random() * POISON_KINDS.length)] ?? null : null;
  return { copies, poison };
}

/**
 * Holds up to `window` items and releases them in random order. Nothing is lost or duplicated: every item pushed is
 * released exactly once, by a later `push` or by `drain`.
 */
export class Reorderer<T> {
  readonly #held: T[] = [];
  readonly #random: () => number;

  /** Creates an empty reorderer that draws from `random`. */
  constructor(random: () => number) {
    this.#random = random;
  }

  /** Adds an item; returns the items released now (in random order) so at most `window - 1` stay held. */
  push(item: T, window: number): T[] {
    this.#held.push(item);
    const keep = Math.max(0, window - 1);
    const released: T[] = [];
    while (this.#held.length > keep) released.push(this.#takeRandom());
    return released;
  }

  /** Releases everything still held, in random order (when reordering is switched off, or on shutdown). */
  drain(): T[] {
    const released: T[] = [];
    while (this.#held.length > 0) released.push(this.#takeRandom());
    return released;
  }

  /** Items currently held. */
  size(): number {
    return this.#held.length;
  }

  /** Removes and returns a uniformly random held item. */
  #takeRandom(): T {
    const index = Math.floor(this.#random() * this.#held.length);
    const [item] = this.#held.splice(index, 1);
    return item as T;
  }
}

/** A message to publish as-is: the device ID in its topic, and the raw payload. */
export interface RawMessage {
  topicDeviceId: string;
  payload: string;
}

/** How far back a `too_old` poison is stamped: 400 days, older than any retention ingest accepts (S8-2). */
export const TOO_OLD_AGE_MS = 400 * 86_400_000;

/**
 * Builds one poison message from a real reading, without reusing its key: the seq is far outside any real one
 * (`900_000_000 + seq`), so a poison that ingest wrongly accepted would show up as an outlier, never as a duplicate.
 * - `invalid_json`: not JSON at all;
 * - `schema_mismatch`: JSON, but `seq` is a string;
 * - `device_id_mismatch`: the payload names another device than the topic;
 * - `future_timestamp`: stamped 10 minutes ahead (a broken clock). Ingest dead-letters anything more than 5 minutes
 *   ahead of its own clock (D7, `MAX_FUTURE_SKEW_MS` in `packages/ingest/src/core/routeMessage.ts`), so this clears the limit by 5 minutes;
 *   change one, check the other;
 * - `too_old`: stamped {@link TOO_OLD_AGE_MS} back, before what retention keeps; ingest dead-letters it (S8-2);
 * - `foreign_device`: a device ID that belongs to `foreignDeviceId`'s gateway, under this gateway's topic. That device
 *   must already be bound to its own gateway: a poison sent before then would bind it here instead (ingest binds a
 *   device on its first reading, D6), so scenario 5 turns poison on only once every gateway is delivering.
 */
export function poisonMessage(kind: PoisonKind, reading: TelemetryMessage, foreignDeviceId: string, now: Date): RawMessage {
  const fake = { ...reading, seq: 900_000_000 + reading.seq };
  switch (kind) {
    case "invalid_json":
      return { topicDeviceId: reading.deviceId, payload: `{"deviceId":"${reading.deviceId}",` };
    case "schema_mismatch":
      return { topicDeviceId: reading.deviceId, payload: JSON.stringify({ ...fake, seq: "not-a-number" }) };
    case "device_id_mismatch":
      return { topicDeviceId: reading.deviceId, payload: JSON.stringify({ ...fake, deviceId: `${reading.deviceId}-other` }) };
    case "future_timestamp":
      return { topicDeviceId: reading.deviceId, payload: JSON.stringify({ ...fake, timestamp: new Date(now.getTime() + 600_000).toISOString() }) };
    case "too_old":
      return { topicDeviceId: reading.deviceId, payload: JSON.stringify({ ...fake, timestamp: new Date(now.getTime() - TOO_OLD_AGE_MS).toISOString() }) };
    case "foreign_device":
      return { topicDeviceId: foreignDeviceId, payload: JSON.stringify({ ...fake, deviceId: foreignDeviceId }) };
  }
}
