/**
 * Writes a batch of routed messages to Postgres in one transaction, with hand-written, set-based SQL (D1, D5).
 * Each statement covers the whole batch: `unnest` turns parameter arrays into rows, so 500 readings take a handful
 * of round trips, not 2,000.
 *
 * The rules that make delivery order irrelevant live here:
 * - **dedup:** a reading is stored once per (device_id, epoch, seq) (`ON CONFLICT DO NOTHING`, D4);
 * - **newer-only state:** `device_state` only moves forward on (measured_at, epoch, seq) (D4);
 * - **newer-only status:** gateway status only moves forward on `last_status_at` (S2-2);
 * - **device binding:** a device belongs to the first gateway it appeared under (D6).
 */
import type pg from "pg";
import type { DeadLetterReason, DeadLetterRow, ReadingRow, StatusRow } from "../core/rows.ts";
import type { Routed } from "../core/routeMessage.ts";
import { withClient } from "./databasePool.ts";

/** One message to write: how it was routed, plus the raw topic and payload in case it must be dead-lettered. */
export interface StoreItem {
  routed: Routed;
  topic: string;
  payload: Uint8Array;
}

/** What writing one item did. */
export type ItemOutcome =
  | { kind: "inserted" }
  | { kind: "duplicate" }
  | { kind: "status" }
  | { kind: "dead_letter"; reason: DeadLetterReason; detail: string | null };

/** The write ingest needs, as an interface, so the pipeline can be tested with a fake. */
export interface IngestStore {
  /**
   * Writes every item in one transaction and returns one outcome per item, in order. Readings are bound, deduped
   * and folded into `device_state`; readings for a device bound to another gateway become `foreign_device` dead
   * letters; statuses are applied in arrival order, newer-only; dead letters are stored as they are.
   * If any statement fails, nothing is written and the error is thrown for the caller to classify.
   */
  writeBatch(items: StoreItem[]): Promise<ItemOutcome[]>;
}

const EXISTING_BINDINGS = `SELECT device_id, gateway_id FROM devices WHERE device_id = ANY($1::text[])`;

const REGISTER_GATEWAYS = `
  INSERT INTO gateways (gateway_id, first_seen)
  SELECT gateway_id, first_seen FROM unnest($1::text[], $2::timestamptz[]) AS g(gateway_id, first_seen)
  ON CONFLICT (gateway_id) DO NOTHING`;

/**
 * Binds new devices, in device-ID order (the caller sorts them, S3-8). A device another transaction bound first is
 * not returned, and the caller re-reads its binding.
 */
const BIND_NEW_DEVICES = `
  INSERT INTO devices (device_id, gateway_id, kind, first_seen)
  SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::timestamptz[])
  ON CONFLICT (device_id) DO NOTHING
  RETURNING device_id`;

/** The 13 reading columns as `unnest` parameters, shared by the two reading statements. */
const READING_UNNEST = `
  unnest($1::text[], $2::bigint[], $3::bigint[], $4::timestamptz[], $5::timestamptz[], $6::text[],
         $7::float8[], $8::float8[], $9::float8[], $10::float8[], $11::float8[], $12::float8[], $13::float8[])
  AS r(device_id, epoch, seq, measured_at, received_at, kind,
       pv_power_w, battery_power_w, grid_power_w, ev_power_w, energy_wh, soc_percent, charge_limit_a)`;

/** Inserted in key order, so two replicas writing overlapping keys lock them in the same order (S3-8). */
const INSERT_READINGS = `
  INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind,
                        pv_power_w, battery_power_w, grid_power_w, ev_power_w, energy_wh, soc_percent, charge_limit_a)
  SELECT * FROM ${READING_UNNEST}
  ORDER BY device_id, epoch, seq
  ON CONFLICT (device_id, epoch, seq) DO NOTHING
  RETURNING device_id, epoch::text, seq::text`;

/**
 * Newer-only (D4). Within the batch, only each device's newest reading is a candidate (`DISTINCT ON`, ordered by
 * the same tuple). Against the stored row, the tuple comparison breaks timestamp ties deterministically, so the end
 * state doesn't depend on arrival order or on how the stream was cut into batches.
 */
const UPSERT_DEVICE_STATE = `
  INSERT INTO device_state (device_id, epoch, seq, measured_at, received_at, kind,
                            pv_power_w, battery_power_w, grid_power_w, ev_power_w, energy_wh, soc_percent, charge_limit_a)
  SELECT DISTINCT ON (device_id) * FROM ${READING_UNNEST}
  ORDER BY device_id, measured_at DESC, epoch DESC, seq DESC
  ON CONFLICT (device_id) DO UPDATE SET
    epoch = excluded.epoch, seq = excluded.seq, measured_at = excluded.measured_at, received_at = excluded.received_at,
    kind = excluded.kind, pv_power_w = excluded.pv_power_w, battery_power_w = excluded.battery_power_w,
    grid_power_w = excluded.grid_power_w, ev_power_w = excluded.ev_power_w, energy_wh = excluded.energy_wh,
    soc_percent = excluded.soc_percent, charge_limit_a = excluded.charge_limit_a
  WHERE (excluded.measured_at, excluded.epoch, excluded.seq) > (device_state.measured_at, device_state.epoch, device_state.seq)`;

/** The newer-only condition for a status: the gateway has no status yet, or this one is later (S2-2). */
const STATUS_IS_NEWER = "(gateways.last_status_at IS NULL OR excluded.last_status_at > gateways.last_status_at)";

/**
 * Newer-only status. The two replicas each get some of a gateway's status messages, so they can be written out of
 * order. Example: the Last Will `offline` goes to one replica and the reboot's `online` to the other. The state, its
 * time and the counters move only if this status is newer than the stored one.
 * `last_status_received_at` moves on every status received, newer or not: it records when ingest last *heard* from the
 * gateway, by ingest's own clock, which the health rules use (S5-1). `last_offline_received_at` does the same for
 * `offline` statuses only: a reading proves life only if it arrived after the latest one (S5-6). `came_back_at` is the
 * receive time of the first `online` after the latest `offline`: readings measured before it were buffered while the
 * gateway was away, and the SLO doesn't count them (S6-1).
 * Caveat: `last_status_at` is the gateway's clock for `online`, but ingest's receive time for the Last Will, which has
 * no timestamp (decisions.md S2-2). The health rules don't depend on it alone (S5-1).
 */
const UPSERT_STATUS = `
  INSERT INTO gateways (gateway_id, first_seen, last_status, last_status_at, buffered, dropped, mode, last_status_received_at, last_offline_received_at)
  VALUES ($1, $2, $3, $4, $5, $6, $7, $2, CASE WHEN $3 = 'offline' THEN $2::timestamptz END)
  ON CONFLICT (gateway_id) DO UPDATE SET
    last_status = CASE WHEN ${STATUS_IS_NEWER} THEN excluded.last_status ELSE gateways.last_status END,
    last_status_at = CASE WHEN ${STATUS_IS_NEWER} THEN excluded.last_status_at ELSE gateways.last_status_at END,
    buffered = CASE WHEN ${STATUS_IS_NEWER} THEN COALESCE(excluded.buffered, gateways.buffered) ELSE gateways.buffered END,
    dropped = CASE WHEN ${STATUS_IS_NEWER} THEN COALESCE(excluded.dropped, gateways.dropped) ELSE gateways.dropped END,
    mode = CASE WHEN ${STATUS_IS_NEWER} THEN COALESCE(excluded.mode, gateways.mode) ELSE gateways.mode END,
    last_status_received_at = GREATEST(gateways.last_status_received_at, excluded.last_status_received_at),
    last_offline_received_at = GREATEST(gateways.last_offline_received_at, excluded.last_offline_received_at),
    came_back_at = CASE
      WHEN excluded.last_status = 'online' AND gateways.last_offline_received_at IS NOT NULL
           AND (gateways.came_back_at IS NULL OR gateways.came_back_at < gateways.last_offline_received_at)
      THEN excluded.last_status_received_at
      ELSE gateways.came_back_at END`;

const INSERT_DEAD_LETTER = `
  INSERT INTO dead_letters (topic, payload, reason, detail, received_at) VALUES ($1, $2, $3, $4, $5)`;

/** The readings' columns as 13 parallel arrays, the shape `READING_UNNEST` expects. */
function readingArrays(rows: ReadingRow[]): unknown[][] {
  return [
    rows.map((r) => r.deviceId), rows.map((r) => r.epoch), rows.map((r) => r.seq), rows.map((r) => r.measuredAt),
    rows.map((r) => r.receivedAt), rows.map((r) => r.kind), rows.map((r) => r.pvPowerW), rows.map((r) => r.batteryPowerW),
    rows.map((r) => r.gridPowerW), rows.map((r) => r.evPowerW), rows.map((r) => r.energyWh), rows.map((r) => r.socPercent),
    rows.map((r) => r.chargeLimitA),
  ];
}

/** The dedup key of a reading, as a string. */
function readingKey(deviceId: string, epoch: number | string, seq: number | string): string {
  return `${deviceId}\u0000${epoch}\u0000${seq}`;
}

/**
 * Decides, in arrival order, which readings may be written. A device keeps its existing binding. A device new to the
 * database is claimed by the first gateway in the batch that sends it. Returns the binding of every device, plus the
 * devices this batch must insert.
 */
function planBindings(readings: ReadingRow[], existing: Map<string, string>): { bindings: Map<string, string>; newDevices: ReadingRow[] } {
  const bindings = new Map(existing);
  const newDevices: ReadingRow[] = [];
  for (const row of readings) {
    if (bindings.has(row.deviceId)) continue;
    bindings.set(row.deviceId, row.gatewayId);
    newDevices.push(row);
  }
  return { bindings, newDevices };
}

/**
 * Binds the batch's devices and returns each device's final gateway. New devices are inserted; any another
 * transaction bound first are re-read, so a concurrent binding wins and this batch's claim becomes a foreign device.
 */
async function bindDevices(client: pg.PoolClient, readings: ReadingRow[]): Promise<Map<string, string>> {
  const deviceIds = [...new Set(readings.map((r) => r.deviceId))];
  const existing = await client.query<{ device_id: string; gateway_id: string }>(EXISTING_BINDINGS, [deviceIds]);
  const { bindings, newDevices: unsorted } = planBindings(readings, new Map(existing.rows.map((r) => [r.device_id, r.gateway_id])));
  if (unsorted.length === 0) return bindings;
  const newDevices = [...unsorted].sort((a, b) => compare(a.deviceId, b.deviceId));
  await registerGateways(client, newDevices);
  const inserted = await client.query<{ device_id: string }>(BIND_NEW_DEVICES, [
    newDevices.map((r) => r.deviceId), newDevices.map((r) => r.gatewayId), newDevices.map((r) => r.kind), newDevices.map((r) => r.receivedAt),
  ]);
  const won = new Set(inserted.rows.map((r) => r.device_id));
  const lost = newDevices.map((r) => r.deviceId).filter((id) => !won.has(id));
  if (lost.length) {
    const reread = await client.query<{ device_id: string; gateway_id: string }>(EXISTING_BINDINGS, [lost]);
    for (const row of reread.rows) bindings.set(row.device_id, row.gateway_id);
  }
  return bindings;
}

/** Orders strings by code unit, the same way in every replica, for a consistent lock order. */
function compare(a: string, b: string): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Registers the gateways of these readings (first sight only), in gateway-ID order (S3-8). */
async function registerGateways(client: pg.PoolClient, readings: ReadingRow[]): Promise<void> {
  const firstSeen = new Map<string, Date>();
  for (const row of readings) if (!firstSeen.has(row.gatewayId)) firstSeen.set(row.gatewayId, row.receivedAt);
  const ids = [...firstSeen.keys()].sort(compare);
  await client.query(REGISTER_GATEWAYS, [ids, ids.map((id) => firstSeen.get(id))]);
}

/** Inserts the accepted readings and moves `device_state` forward; returns the keys that were new. */
async function writeReadings(client: pg.PoolClient, readings: ReadingRow[]): Promise<Set<string>> {
  if (readings.length === 0) return new Set();
  await registerGateways(client, readings);
  const arrays = readingArrays(readings);
  const inserted = await client.query<{ device_id: string; epoch: string; seq: string }>(INSERT_READINGS, arrays);
  await client.query(UPSERT_DEVICE_STATE, arrays);
  return new Set(inserted.rows.map((r) => readingKey(r.device_id, r.epoch, r.seq)));
}

/** Applies one status, newer-only. */
async function writeStatus(client: pg.PoolClient, row: StatusRow): Promise<void> {
  await client.query(UPSERT_STATUS, [row.gatewayId, row.receivedAt, row.state, row.statusAt, row.buffered, row.dropped, row.mode]);
}

/** Stores one dead letter. */
async function writeDeadLetter(client: pg.PoolClient, row: DeadLetterRow): Promise<void> {
  await client.query(INSERT_DEAD_LETTER, [row.topic, Buffer.from(row.payload), row.reason, row.detail, row.receivedAt]);
}

/**
 * The batch, inside an open transaction. Readings are written set-based; statuses and dead letters, which are rare,
 * one by one in arrival order. Within the batch, the first copy of a reading is "inserted" and later copies are
 * "duplicate".
 */
async function writeBatchIn(client: pg.PoolClient, items: StoreItem[]): Promise<ItemOutcome[]> {
  const readings = items.flatMap((item) => (item.routed.type === "reading" ? [item.routed.row] : []));
  const bindings = readings.length ? await bindDevices(client, readings) : new Map<string, string>();
  const accepted = readings.filter((r) => bindings.get(r.deviceId) === r.gatewayId);
  const newKeys = await writeReadings(client, accepted);
  const outcomes: ItemOutcome[] = new Array(items.length);
  for (const index of lockOrder(items)) outcomes[index] = await writeOne(client, items[index] as StoreItem, bindings, newKeys);
  return outcomes;
}

/**
 * The order to finish items in: statuses sorted by gateway ID (stable, so one gateway's statuses keep their arrival
 * order, which the newer-only guard relies on), then everything else in arrival order. Two replicas updating the
 * same gateways then lock their rows in the same order (S3-8).
 */
function lockOrder(items: StoreItem[]): number[] {
  const gateways = items.map(statusGateway);
  const indexes = items.map((_, i) => i);
  const statuses = indexes.filter((i) => gateways[i] !== null).sort((a, b) => compare(gateways[a] as string, gateways[b] as string) || a - b);
  return [...statuses, ...indexes.filter((i) => gateways[i] === null)];
}

/** The gateway a status item is about, or null for any other item. */
function statusGateway(item: StoreItem): string | null {
  return item.routed.type === "status" ? item.routed.row.gatewayId : null;
}

/** Finishes one item and returns its outcome. Readings were already written set-based; this only reports them. */
async function writeOne(client: pg.PoolClient, item: StoreItem, bindings: Map<string, string>, newKeys: Set<string>): Promise<ItemOutcome> {
  const routed = item.routed;
  if (routed.type === "status") {
    await writeStatus(client, routed.row);
    return { kind: "status" };
  }
  if (routed.type === "dead_letter") {
    await writeDeadLetter(client, routed.row);
    return { kind: "dead_letter", reason: routed.row.reason, detail: routed.row.detail };
  }
  const bound = bindings.get(routed.row.deviceId);
  if (bound !== routed.row.gatewayId) {
    const detail = `device ${routed.row.deviceId} is bound to gateway ${bound}`;
    await writeDeadLetter(client, { topic: item.topic, payload: item.payload, reason: "foreign_device", detail, receivedAt: routed.row.receivedAt });
    return { kind: "dead_letter", reason: "foreign_device", detail };
  }
  const key = readingKey(routed.row.deviceId, routed.row.epoch, routed.row.seq);
  if (!newKeys.delete(key)) return { kind: "duplicate" };
  return { kind: "inserted" };
}

/**
 * Creates the Postgres implementation of {@link IngestStore} on a connection pool owned by the caller.
 * The connection is held through {@link withClient} (S7-10): a socket lost mid-batch rejects the batch for the retry
 * instead of crashing ingest, and a failed batch's connection is destroyed, which aborts its transaction on the
 * server. There is no ROLLBACK: after a query timeout the connection is still waiting on the old query, so a ROLLBACK
 * would only queue behind it and time out too.
 */
export function createPostgresStore(pool: pg.Pool): IngestStore {
  return {
    writeBatch: (items) =>
      withClient(pool, async (client) => {
        await client.query("BEGIN");
        const outcomes = await writeBatchIn(client, items);
        await client.query("COMMIT");
        return outcomes;
      }),
  };
}
