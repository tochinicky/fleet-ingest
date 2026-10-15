/**
 * Every SQL statement the API runs, hand-written (D1), each in a function that returns plain JSON-ready objects.
 * The API reads the database, and writes one thing: a gateway's desired mode (S7-1), which the command reconciler then
 * publishes to the broker.
 */
import type pg from "pg";
import type { WindowReading } from "./core/gapWindow.ts";
import type { Gap } from "./core/health.ts";
import type { Cursor, ReadingQuery } from "./core/readingQuery.ts";

/** Columns shared by `readings` and `device_state`, renamed to the API's camelCase. */
const READING_COLUMNS = `
  device_id AS "deviceId", epoch::int, seq::int, kind,
  measured_at AS "measuredAt", received_at AS "receivedAt",
  pv_power_w AS "pvPowerW", battery_power_w AS "batteryPowerW", grid_power_w AS "gridPowerW",
  ev_power_w AS "evPowerW", energy_wh AS "energyWh", soc_percent AS "socPercent", charge_limit_a AS "chargeLimitA"`;

/** One reading or one device state, as the API returns it. */
export interface ReadingView {
  deviceId: string;
  epoch: number;
  seq: number;
  kind: string;
  measuredAt: Date;
  receivedAt: Date;
  [measurement: string]: unknown;
}

/** The current state of one device, or null if the device has never sent a reading. */
export async function deviceState(pool: pg.Pool, deviceId: string): Promise<ReadingView | null> {
  const { rows } = await pool.query<ReadingView>(`SELECT ${READING_COLUMNS} FROM device_state WHERE device_id = $1`, [deviceId]);
  return rows[0] ?? null;
}

/**
 * One page of a device's readings in the window, oldest first, ordered by (measured_at, epoch, seq) so the order is
 * total and a cursor can resume exactly after the last row. Fetches one row more than asked, to know whether
 * another page exists.
 */
export async function readingsPage(pool: pg.Pool, deviceId: string, query: ReadingQuery): Promise<{ items: ReadingView[]; next: Cursor | null }> {
  const after = query.after;
  const { rows } = await pool.query<ReadingView>(
    `SELECT ${READING_COLUMNS} FROM readings
     WHERE device_id = $1 AND measured_at >= $2 AND measured_at <= $3
       AND ($4::timestamptz IS NULL OR (measured_at, epoch, seq) > ($4::timestamptz, $5::bigint, $6::bigint))
     ORDER BY measured_at, epoch, seq
     LIMIT $7`,
    [deviceId, query.from, query.to, after?.measuredAt ?? null, after?.epoch ?? null, after?.seq ?? null, query.limit + 1],
  );
  const items = rows.slice(0, query.limit);
  const last = items.at(-1);
  const next = rows.length > query.limit && last ? { measuredAt: last.measuredAt.toISOString(), epoch: last.epoch, seq: last.seq } : null;
  return { items, next };
}

/** One gateway's row and the evidence the health rules need: its last status, when ingest last heard it, its counters. */
export interface GatewayRow {
  gatewayId: string;
  firstSeen: Date;
  lastStatus: "online" | "offline" | null;
  lastStatusAt: Date | null;
  lastStatusReceivedAt: Date | null;
  lastOfflineReceivedAt: Date | null;
  lastReadingReceivedAt: Date | null;
  buffered: number | null;
  dropped: number | null;
  /** The mode the gateway reports in its status. */
  mode: string | null;
  /** The mode the cloud asked for (`PUT /gateways/:id/mode`), and when. */
  desiredMode: string | null;
  desiredAt: Date | null;
  /** Ingest's receive time of the first `online` after the latest `offline` (S6-1). */
  cameBackAt: Date | null;
}

const GATEWAY_COLUMNS = `
  g.gateway_id AS "gatewayId", g.first_seen AS "firstSeen", g.last_status AS "lastStatus", g.last_status_at AS "lastStatusAt",
  g.last_status_received_at AS "lastStatusReceivedAt", g.last_offline_received_at AS "lastOfflineReceivedAt", g.buffered::int AS buffered, g.dropped::int AS dropped, g.mode,
  g.desired_mode AS "desiredMode", g.desired_at AS "desiredAt", g.came_back_at AS "cameBackAt",
  (SELECT max(s.received_at) FROM device_state s JOIN devices d USING (device_id) WHERE d.gateway_id = g.gateway_id) AS "lastReadingReceivedAt"`;

/** Every gateway with its health evidence. */
export async function allGateways(pool: pg.Pool): Promise<GatewayRow[]> {
  const { rows } = await pool.query<GatewayRow>(`SELECT ${GATEWAY_COLUMNS} FROM gateways g`);
  return rows;
}

/** One gateway with its health evidence, or null if it has never been seen. */
export async function oneGateway(pool: pg.Pool, gatewayId: string): Promise<GatewayRow | null> {
  const { rows } = await pool.query<GatewayRow>(`SELECT ${GATEWAY_COLUMNS} FROM gateways g WHERE g.gateway_id = $1`, [gatewayId]);
  return rows[0] ?? null;
}

/** Records a gateway's desired mode. Returns false if the gateway has never been seen. */
export async function setDesiredMode(pool: pg.Pool, gatewayId: string, mode: string, at: Date): Promise<boolean> {
  const { rowCount } = await pool.query("UPDATE gateways SET desired_mode = $2, desired_at = $3 WHERE gateway_id = $1", [gatewayId, mode, at]);
  return rowCount === 1;
}

/** A gateway's desired mode, as the reconciler publishes it. */
export interface DesiredMode {
  gatewayId: string;
  mode: string;
}

/** Every gateway's desired mode, or only those the gateway hasn't reported yet (`pendingOnly`). */
export async function desiredModes(pool: pg.Pool, pendingOnly: boolean): Promise<DesiredMode[]> {
  const { rows } = await pool.query<DesiredMode>(
    `SELECT gateway_id AS "gatewayId", desired_mode AS mode FROM gateways
     WHERE desired_mode IS NOT NULL AND (NOT $1 OR mode IS DISTINCT FROM desired_mode)
     ORDER BY gateway_id`,
    [pendingOnly],
  );
  return rows;
}

/** A device and when ingest last received a reading from it. */
export interface DeviceRow {
  deviceId: string;
  gatewayId: string;
  kind: string;
  firstSeen: Date;
  lastReadingReceivedAt: Date | null;
}

/** Devices, with their latest reading's receive time; all of them, or one gateway's. */
export async function devices(pool: pg.Pool, gatewayId?: string): Promise<DeviceRow[]> {
  const { rows } = await pool.query<DeviceRow>(
    `SELECT d.device_id AS "deviceId", d.gateway_id AS "gatewayId", d.kind, d.first_seen AS "firstSeen", s.received_at AS "lastReadingReceivedAt"
     FROM devices d LEFT JOIN device_state s USING (device_id)
     WHERE $1::text IS NULL OR d.gateway_id = $1
     ORDER BY d.device_id`,
    [gatewayId ?? null],
  );
  return rows;
}

/**
 * Gaps in readings measured between `from` and `to`, per (device, epoch): consecutive stored seqs that differ by more
 * than one. The window function is partitioned by epoch as well as device, because seq starts again after a power
 * loss (a new epoch): comparing across epochs would turn every reboot into a false gap.
 * A gap that a later replay fills disappears by itself, because the query reads only what's stored.
 * Optionally limited to one gateway's devices.
 *
 * Per device (S7-11): a lateral subquery per device reads only that device's readings, through its index, instead of
 * a scan of the whole table filtered by time. A fleet-wide window over a large table no longer costs a full scan
 * (`/fleet` took 12.6 s and was cut off by the 10 s statement timeout at 3.5M rows).
 */
export async function gaps(pool: pg.Pool, from: Date, to: Date, gatewayId?: string): Promise<Array<Gap & { gatewayId: string }>> {
  const { rows } = await pool.query<Gap & { gatewayId: string }>(
    `SELECT d.device_id AS "deviceId", d.gateway_id AS "gatewayId", g.epoch::int, (g.prev_seq + 1)::int AS "fromSeq", (g.seq - 1)::int AS "toSeq",
            (g.seq - g.prev_seq - 1)::int AS missing, g.prev_at AS before
     FROM devices d
     CROSS JOIN LATERAL (
       SELECT r.epoch, r.seq, lag(r.seq) OVER per_epoch AS prev_seq, lag(r.measured_at) OVER per_epoch AS prev_at
       FROM readings r
       WHERE r.device_id = d.device_id AND r.measured_at BETWEEN $1 AND $2
       WINDOW per_epoch AS (PARTITION BY r.epoch ORDER BY r.seq)
     ) g
     WHERE ($3::text IS NULL OR d.gateway_id = $3) AND g.seq - g.prev_seq > 1
     ORDER BY d.gateway_id, before`,
    [from, to, gatewayId ?? null],
  );
  return rows;
}

/** A stored reading as the gap window needs it, with when it was stored. Times are epoch milliseconds. */
export interface StoredReading extends WindowReading {
  storedAt: number;
}

/**
 * Readings stored after `storedAfter` (by `stored_at`, through its index), whatever they were measured: the delta a
 * gap pass reads behind its cursor (S7-11). The caller filters by measured time; `stored_at` moves the cursor.
 * Times come back as epoch milliseconds and there's no join: on the lab VM, parsing two timestamps per row into
 * `Date`s and joining `devices` made this the slowest step of the pass. The caller knows each device's gateway.
 */
export async function readingsStoredAfter(pool: pg.Pool, storedAfter: Date): Promise<Array<Omit<StoredReading, "gatewayId">>> {
  const { rows } = await pool.query<{ deviceId: string; epoch: number; seq: number; measuredAt: number; storedAt: number }>(
    `SELECT device_id AS "deviceId", epoch::int, seq::int,
            (extract(epoch FROM measured_at) * 1000)::float8 AS "measuredAt", (extract(epoch FROM stored_at) * 1000)::float8 AS "storedAt"
     FROM readings
     WHERE stored_at > $1`,
    [storedAfter],
  );
  return rows;
}

/**
 * One gateway's readings measured since `from`, per device through `(device_id, measured_at) INCLUDE (epoch, seq)`
 * (an index-only scan): how the gap window is filled at start, one gateway at a time, so no single query reads the
 * whole fleet's hour (S7-11). Times come back as epoch milliseconds, rows as they are: the warm-up reads about 1.4M
 * rows, and `Date`s plus copies made minor GC take 26 s of a cold start's first 6 minutes, stalling the API.
 */
export async function gatewayReadingsSince(pool: pg.Pool, gatewayId: string, from: Date): Promise<WindowReading[]> {
  const { rows } = await pool.query<WindowReading>(
    `SELECT d.device_id AS "deviceId", d.gateway_id AS "gatewayId", r.epoch::int, r.seq::int, (extract(epoch FROM r.measured_at) * 1000)::float8 AS "measuredAt"
     FROM devices d
     CROSS JOIN LATERAL (SELECT epoch, seq, measured_at FROM readings WHERE device_id = d.device_id AND measured_at >= $2) r
     WHERE d.gateway_id = $1`,
    [gatewayId, from],
  );
  return rows;
}

/** The database's clock, for a cursor that starts in the same clock as `stored_at`. */
export async function databaseNow(pool: pg.Pool): Promise<Date> {
  const { rows } = await pool.query<{ now: Date }>("SELECT now() AS now");
  return (rows[0] as { now: Date }).now;
}

/**
 * Dead letters received in the last hour, and p95 ingest lag over the last 5 minutes.
 *
 * Every "readings received in the last N" filter here and in the health job also bounds `stored_at` (S7-11).
 * `received_at` has no index, so on its own it scanned the whole table every time, and that grew with retention.
 * `stored_at` is indexed and never earlier than `received_at` (a reading is received before its batch transaction
 * starts), so `stored_at > now() − (N + 1 min)` drops none of the rows the `received_at` filter keeps. The extra
 * minute covers clock differences between ingest's container and Postgres.
 */
export async function ingestFigures(pool: pg.Pool): Promise<{ deadLettersLastHour: number; p95IngestLagSeconds: number | null }> {
  const { rows } = await pool.query(`
    SELECT
      (SELECT count(*) FROM dead_letters WHERE received_at > now() - interval '1 hour')::int AS "deadLettersLastHour",
      (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - measured_at))
         FROM readings WHERE received_at > now() - interval '5 minutes' AND stored_at > now() - interval '6 minutes') AS "p95IngestLagSeconds"`);
  return rows[0];
}

/** True if the database answers a trivial query. */
export async function databaseReachable(pool: pg.Pool): Promise<boolean> {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}
