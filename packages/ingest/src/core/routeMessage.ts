/**
 * Decides what to do with one received message: store a reading, store a gateway status, or dead-letter it.
 * Step 1 of the pipeline, pure: the topic, the payload bytes and the receive time go in, a row comes out.
 * The checks that need the database (a device claimed by another gateway) happen in the store, in the same
 * transaction as the write.
 */
import { decodeStatus, decodeTelemetry, parseTopic } from "@fleet-ingest/contract";
import { acceptedSince } from "@fleet-ingest/db";
import type { GatewayStatus, TelemetryMessage } from "@fleet-ingest/contract";
import type { DeadLetterReason, DeadLetterRow, ReadingRow, StatusRow } from "./rows.ts";

/** The outcome of routing: exactly one row to write. */
export type Routed =
  | { type: "reading"; row: ReadingRow }
  | { type: "status"; row: StatusRow }
  | { type: "dead_letter"; row: DeadLetterRow };

/**
 * How far ahead of ingest's clock a timestamp may be before it counts as a broken clock (D7). Both `device_state`
 * and gateway status only accept newer values, so a single far-future timestamp would freeze them until real time
 * caught up. Five minutes leaves room for ordinary clock skew.
 */
export const MAX_FUTURE_SKEW_MS = 5 * 60 * 1000;

/** Builds the dead-letter outcome for a message, keeping its topic and raw bytes. */
function deadLetter(topic: string, payload: Uint8Array, receivedAt: Date, reason: DeadLetterReason, detail: string | null): Routed {
  return { type: "dead_letter", row: { topic, payload, reason, detail, receivedAt } };
}

/** Describes how far `at` is ahead of `receivedAt`, if it's beyond {@link MAX_FUTURE_SKEW_MS}; otherwise null. */
function futureSkew(at: Date, receivedAt: Date): string | null {
  const aheadMs = at.getTime() - receivedAt.getTime();
  return aheadMs > MAX_FUTURE_SKEW_MS ? `timestamp is ${Math.round(aheadMs / 1000)} s ahead of ingest's clock` : null;
}

/**
 * Routes one message. Never throws: anything it can't make sense of becomes a dead letter with a reason.
 * `retentionDays` sets the oldest reading that still has a partition: anything measured before
 * {@link acceptedSince} is `too_old` (S8-2). Statuses aren't partitioned and have no such limit.
 */
export function routeMessage(topic: string, payload: Uint8Array, receivedAt: Date, retentionDays: number): Routed {
  const parsed = parseTopic(topic);
  if (parsed.kind === "unknown") return deadLetter(topic, payload, receivedAt, "unknown_topic", null);
  if (parsed.kind === "status") return routeStatus(topic, payload, receivedAt, parsed.gatewayId);
  const decoded = decodeTelemetry(payload);
  if (!decoded.ok) return deadLetter(topic, payload, receivedAt, decoded.reason, decoded.detail);
  if (decoded.value.deviceId !== parsed.deviceId) {
    return deadLetter(topic, payload, receivedAt, "device_id_mismatch", `topic says ${parsed.deviceId}, payload says ${decoded.value.deviceId}`);
  }
  const row = toReadingRow(parsed.gatewayId, decoded.value, receivedAt);
  const skew = futureSkew(row.measuredAt, receivedAt);
  if (skew) return deadLetter(topic, payload, receivedAt, "future_timestamp", skew);
  const oldest = acceptedSince(receivedAt, retentionDays);
  if (row.measuredAt < oldest) {
    return deadLetter(topic, payload, receivedAt, "too_old", `measured ${row.measuredAt.toISOString()}; retention keeps readings from ${oldest.toISOString()}`);
  }
  return { type: "reading", row };
}

/**
 * Routes a status message. A status from a broken clock is dead-lettered like a reading, because gateway status also
 * only moves forward: one far-future status would hide every real one after it.
 */
function routeStatus(topic: string, payload: Uint8Array, receivedAt: Date, gatewayId: string): Routed {
  const decoded = decodeStatus(payload);
  if (!decoded.ok) return deadLetter(topic, payload, receivedAt, decoded.reason, decoded.detail);
  const row = toStatusRow(gatewayId, decoded.value, receivedAt);
  const skew = futureSkew(row.statusAt, receivedAt);
  return skew ? deadLetter(topic, payload, receivedAt, "future_timestamp", skew) : { type: "status", row };
}

/** Maps a decoded reading to its row; values the device didn't send become null. */
export function toReadingRow(gatewayId: string, message: TelemetryMessage, receivedAt: Date): ReadingRow {
  return {
    gatewayId,
    deviceId: message.deviceId,
    epoch: message.epoch,
    seq: message.seq,
    kind: message.kind,
    measuredAt: new Date(message.timestamp),
    receivedAt,
    pvPowerW: message.pvPowerW ?? null,
    batteryPowerW: message.batteryPowerW ?? null,
    gridPowerW: message.gridPowerW ?? null,
    evPowerW: message.evPowerW ?? null,
    energyWh: message.energyWh ?? null,
    socPercent: message.socPercent ?? null,
    chargeLimitA: message.chargeLimitA ?? null,
  };
}

/**
 * Maps a decoded status to its row. The Last Will (`offline`) carries no timestamp, because the broker publishes it
 * on the gateway's behalf, so the receive time stands in for it.
 */
export function toStatusRow(gatewayId: string, status: GatewayStatus, receivedAt: Date): StatusRow {
  return {
    gatewayId,
    state: status.state,
    statusAt: status.timestamp ? new Date(status.timestamp) : receivedAt,
    receivedAt,
    buffered: status.bufferedTelemetry ?? null,
    dropped: status.droppedTelemetry ?? null,
    mode: status.mode ?? null,
  };
}
