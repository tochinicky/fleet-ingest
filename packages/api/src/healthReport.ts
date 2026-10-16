/**
 * Builds the health answers the API returns by combining the queries with the pure rules in
 * `core/health.ts`: the fleet summary, one gateway's health, and a device's gaps with the ones its gateway's
 * dropped-message counter explains.
 */
import type pg from "pg";
import { classifyGateway, commandStatus, explainGaps, isDeviceStale } from "./core/health.ts";
import type { CommandStatus, GatewayHealth, GatewayState } from "./core/health.ts";
import { allGateways, devices, gaps, ingestFigures, oneGateway } from "./queries.ts";
import type { GatewayRow } from "./queries.ts";
import type { FleetGapsReport } from "./healthSnapshot.ts";

/** Health of one gateway from its row. */
function healthOf(row: GatewayRow, now: Date): GatewayHealth {
  return classifyGateway({
    lastStatus: row.lastStatus, lastStatusReceivedAt: row.lastStatusReceivedAt,
    lastOfflineReceivedAt: row.lastOfflineReceivedAt, lastReadingReceivedAt: row.lastReadingReceivedAt,
  }, now);
}

/** Desired versus reported mode of one gateway from its row. */
export function commandOf(row: GatewayRow, state: GatewayState, now: Date): CommandStatus {
  return commandStatus({ desiredMode: row.desiredMode, desiredAt: row.desiredAt, reportedMode: row.mode, cameBackAt: row.cameBackAt }, state, now);
}

/**
 * The fleet at a glance:
 * - gateways online, offline and unknown (unknown is its own count, never folded into the others);
 * - devices, and how many are stale;
 * - open gaps over the last hour, and how many the gateways' dropped counters explain: the health worker's last gaps
 *   pass, read from `health_snapshot`, so about 60 s old while the worker runs, with its `asOf` and `ageSeconds`;
 *   `complete: false` until the worker has finished its first gaps pass (S7-11, S8-3);
 * - dead letters in the last hour, and p95 ingest lag;
 * - commands still pending, and which gateways are stuck on one (the drill-down for CommandNotApplied, S7-1).
 */
export async function fleetReport(pool: pg.Pool, now: Date, fleetGaps: FleetGapsReport): Promise<Record<string, unknown>> {
  const gatewayRows = await allGateways(pool);
  const states = new Map(gatewayRows.map((row) => [row.gatewayId, healthOf(row, now).state]));
  const deviceRows = await devices(pool);
  const stale = deviceRows.filter((d) => isDeviceStale(states.get(d.gatewayId) ?? "unknown", d.lastReadingReceivedAt, now)).length;
  const count = (state: string) => [...states.values()].filter((s) => s === state).length;
  const commandRows = gatewayRows.map((row) => ({ gatewayId: row.gatewayId, ...commandOf(row, states.get(row.gatewayId) ?? "unknown", now) }));
  return {
    gateways: { online: count("online"), offline: count("offline"), unknown: count("unknown"), total: states.size },
    devices: { total: deviceRows.length, stale },
    commands: {
      pending: commandRows.filter((c) => c.pending).length,
      stuckGateways: commandRows.filter((c) => c.stuck).map((c) => c.gatewayId),
    },
    openGapsLastHour: fleetGaps,
    ...(await ingestFigures(pool)),
    healthAsOfLagSeconds: await currentIngestLag(pool),
  };
}

/**
 * How far behind ingest currently is: p95 of (received − measured) over readings received in the last 30 s. Health
 * is only as fresh as ingest. While a replica works through a backlog, a dead gateway's queued readings can arrive
 * after its Last Will and keep it "online" for up to that backlog plus 60 s. So every health answer carries this
 * figure, letting a consumer see when health is stale (decisions.md S5-8). Null when nothing arrived recently.
 * The `stored_at` bound only narrows the scan to its index: `stored_at` ≥ `received_at`, plus a minute for clocks (S7-11).
 */
async function currentIngestLag(pool: pg.Pool): Promise<number | null> {
  const { rows } = await pool.query<{ lag: number | null }>(
    `SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM received_at - measured_at)) AS lag
     FROM readings WHERE received_at > now() - interval '30 seconds' AND stored_at > now() - interval '90 seconds'`);
  const lag = rows[0]?.lag;
  return lag === null || lag === undefined ? null : Math.round(Number(lag) * 100) / 100;
}

/**
 * A gateway's gaps whose `before` falls in [from, to], each marked explained or not. Explanation runs over **all** of
 * the gateway's gaps since its counter started (from the epoch of time to `now`), oldest first, because
 * `droppedTelemetry` is a lifetime counter. Only the result is filtered to the window (decisions.md S5-9).
 *
 * A gateway that never dropped anything can't have an explained gap, so its history isn't read at all: only gaps
 * from `from` on (S7-11). That gives the same gaps in the window, because a gap whose `before` is in the window
 * starts at a reading measured in it. The all-time scan is left for gateways with drops, and its cost grows with
 * their history. The health worker uses the checkpointed version instead (`checkpointedGapsInWindow`, S8-4); this one
 * serves `/devices/:id/gaps`, an on-demand read for any window, and is the reference the checkpoint is tested against.
 */
export async function gapsInWindow(pool: pg.Pool, gatewayId: string, droppedTelemetry: number, from: Date, to: Date, now: Date): Promise<ReturnType<typeof explainGaps>> {
  const since = droppedTelemetry > 0 ? new Date(0) : from;
  const found = await gaps(pool, since, now, gatewayId);
  return explainGaps(found, droppedTelemetry).filter((g) => g.before >= from && g.before <= to);
}

/** One gateway's row, health, and devices with their staleness, or null if it has never been seen. */
export async function gatewayReport(pool: pg.Pool, gatewayId: string, now: Date): Promise<Record<string, unknown> | null> {
  const row = await oneGateway(pool, gatewayId);
  if (!row) return null;
  const health = healthOf(row, now);
  const deviceRows = await devices(pool, gatewayId);
  const command = commandOf(row, health.state, now);
  return {
    ...row,
    health,
    command: { desiredMode: row.desiredMode, reportedMode: row.mode, pending: command.pending, pendingSince: command.pendingSince, stuck: command.stuck },
    healthAsOfLagSeconds: await currentIngestLag(pool),
    devices: deviceRows.map((d) => ({ deviceId: d.deviceId, kind: d.kind, firstSeen: d.firstSeen, lastReadingReceivedAt: d.lastReadingReceivedAt, stale: isDeviceStale(health.state, d.lastReadingReceivedAt, now) })),
  };
}

/**
 * A device's gaps between `from` and `to`, each marked explained or not. Explanation is decided across all of the
 * gateway's devices in the window, because the gateway's dropped counter covers all of them.
 */
export async function deviceGapsReport(pool: pg.Pool, deviceId: string, from: Date, to: Date, now: Date): Promise<Record<string, unknown> | null> {
  const { rows } = await pool.query<{ gateway_id: string }>("SELECT gateway_id FROM devices WHERE device_id = $1", [deviceId]);
  const gatewayId = rows[0]?.gateway_id;
  if (!gatewayId) return null;
  const gateway = await oneGateway(pool, gatewayId);
  const explained = (await gapsInWindow(pool, gatewayId, gateway?.dropped ?? 0, from, to, now)).filter((g) => g.deviceId === deviceId);
  return {
    deviceId,
    gatewayId,
    droppedTelemetry: gateway?.dropped ?? null,
    gaps: explained.map(({ epoch, fromSeq, toSeq, missing, before, explained: isExplained }) => ({ epoch, fromSeq, toSeq, missing, before, explained: isExplained })),
  };
}
