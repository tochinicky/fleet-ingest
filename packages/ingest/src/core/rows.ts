/**
 * The rows ingest writes, in the shape of the tables in `db/migrations`. The core builds them; the Postgres adapter
 * only binds their fields to SQL parameters.
 */
import type { DeviceKind } from "@fleet-ingest/contract";

/** One reading, ready for `readings` and `device_state`. Measurements the device didn't report are null. */
export interface ReadingRow {
  gatewayId: string;
  deviceId: string;
  epoch: number;
  seq: number;
  kind: DeviceKind;
  measuredAt: Date;
  receivedAt: Date;
  pvPowerW: number | null;
  batteryPowerW: number | null;
  gridPowerW: number | null;
  evPowerW: number | null;
  energyWh: number | null;
  socPercent: number | null;
  chargeLimitA: number | null;
}

/** One gateway status, ready for `gateways`. Counters the message didn't carry are null and leave the stored value. */
export interface StatusRow {
  gatewayId: string;
  state: "online" | "offline";
  statusAt: Date;
  receivedAt: Date;
  buffered: number | null;
  dropped: number | null;
  mode: string | null;
}

/** A message that can't be stored, kept byte for byte with a stable reason code. */
export interface DeadLetterRow {
  topic: string;
  payload: Uint8Array;
  reason: DeadLetterReason;
  detail: string | null;
  receivedAt: Date;
}

/**
 * Why a message was dead-lettered (D7). These strings are stored in `dead_letters.reason` and become a metric label.
 * - `unknown_topic`: the topic isn't part of the contract.
 * - `invalid_json` / `schema_mismatch`: the payload failed decoding.
 * - `device_id_mismatch`: the payload names a different device from the topic.
 * - `future_timestamp`: the timestamp is more than 5 minutes ahead of ingest's clock (a broken clock).
 * - `foreign_device`: the device is already bound to a different gateway (D6).
 * - `rejected_by_database`: Postgres refused the row (a data or constraint error).
 */
export type DeadLetterReason =
  | "unknown_topic"
  | "invalid_json"
  | "schema_mismatch"
  | "device_id_mismatch"
  | "future_timestamp"
  | "foreign_device"
  | "rejected_by_database";
