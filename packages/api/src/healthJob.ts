/**
 * The background health job. It turns the database's view of the fleet into Prometheus metrics,
 * because alerts are evaluated by Prometheus, not by the API:
 * - every 15 s: each gateway's state, each device's staleness, each gateway's clock skew, the SLO counters, the
 *   backfill counter, and how many commands are pending or stuck (S7-1);
 * - every 60 s: open gaps over the last hour, split into explained and unexplained.
 * The rules themselves are the same pure functions the API endpoints use (`core/health.ts`).
 */
import { setTimeout as sleep } from "node:timers/promises";
import type pg from "pg";
import { Counter, Gauge } from "@fleet-ingest/observability";
import type { Registry } from "@fleet-ingest/observability";
import { classifyGateway, isDeviceStale } from "./core/health.ts";
import type { GatewayState } from "./core/health.ts";
import { isDatabaseTimeout } from "./databasePool.ts";
import { advanceCursor, deltaLowerBound, GapWindow } from "./core/gapWindow.ts";
import { allGateways, databaseNow, devices, gatewayReadingsSince, readingsStoredAfter } from "./queries.ts";
import type { StoredReading } from "./queries.ts";
import { commandOf, gapsInWindow } from "./healthReport.ts";

/** The SLO threshold: a reading must be queryable within this long of being measured (S6-1). */
export const SLO_SECONDS = 30;

/** A gateway whose clock skew exceeds this is excluded from the SLO and counted separately (S6-1). */
export const SLO_MAX_SKEW_SECONDS = 30;

/** The job's metrics. */
interface HealthMetrics {
  gatewayOffline: Gauge<"gateway">;
  gatewaysByState: Gauge<"state">;
  deviceStale: Gauge<"device" | "gateway">;
  devicesStale: Gauge;
  clockSkew: Gauge<"gateway">;
  openGapReadings: Gauge<"explained">;
  sloTotal: Counter;
  sloGood: Counter;
  sloExcludedSkew: Counter;
  backfill: Counter;
  lastRun: Gauge;
  partLastSuccess: Gauge<"part">;
  failures: Counter<"part" | "reason">;
  passSeconds: Gauge<"part">;
  gapsWarm: Gauge;
  gapStepSeconds: Gauge<"step">;
  gapDeltaRows: Gauge;
  gapWindowReadings: Gauge;
  commandsPending: Gauge<"connected">;
  commandsStuck: Gauge;
  keyReuse: Counter<"gateway">;
}

/** Creates the job's metrics on `registry`. */
function createHealthMetrics(registry: Registry): HealthMetrics {
  const registers = [registry];
  return {
    gatewayOffline: new Gauge({ name: "fleet_gateway_offline", help: "1 if the gateway is offline by the health rules (S5-1, S5-6).", labelNames: ["gateway"], registers }),
    gatewaysByState: new Gauge({ name: "fleet_gateways", help: "Gateways by state: online, offline, unknown.", labelNames: ["state"], registers }),
    deviceStale: new Gauge({ name: "fleet_device_stale", help: "1 if the device's gateway is online but it sent nothing for 60 s.", labelNames: ["device", "gateway"], registers }),
    devicesStale: new Gauge({ name: "fleet_devices_stale", help: "Stale devices in the fleet.", registers }),
    clockSkew: new Gauge({
      name: "fleet_gateway_clock_skew_seconds", labelNames: ["gateway"], registers,
      help: "Median received − measured over the last 5 min of live readings (< 60 s lag): the gateway clock's offset plus transit time.",
    }),
    openGapReadings: new Gauge({ name: "fleet_open_gap_readings", help: "Readings missing in gaps over the last hour, by whether droppedTelemetry explains them.", labelNames: ["explained"], registers }),
    sloTotal: new Counter({ name: "fleet_slo_readings_total", help: "Readings counted by the SLO: measured while their gateway was connected (S6-1).", registers }),
    sloGood: new Counter({ name: "fleet_slo_readings_good_total", help: "Of those, stored within 30 s of being measured.", registers }),
    sloExcludedSkew: new Counter({
      name: "fleet_slo_readings_excluded_skew_total", registers,
      help: "Readings left out of the SLO because their gateway's clock skew is over 30 s (its own clock would distort them).",
    }),
    backfill: new Counter({ name: "fleet_backfill_readings_total", help: "Readings stored that were measured before their gateway came back: buffered while it was away (S6-1).", registers }),
    lastRun: new Gauge({ name: "fleet_health_job_last_run_timestamp_seconds", help: "When the health job last completed.", registers }),
    commandsPending: new Gauge({ name: "fleet_commands_pending", help: "Gateways whose reported mode isn't their desired mode yet, by whether the gateway is online (S7-1).", labelNames: ["connected"], registers }),
    commandsStuck: new Gauge({ name: "fleet_commands_stuck", help: "Online gateways that haven't applied their desired mode within 5 min of being able to (S7-1). /fleet lists them.", registers }),
    passSeconds: new Gauge({ name: "fleet_health_job_pass_seconds", help: "How long each part of the last health-job pass took.", labelNames: ["part"], registers }),
    partLastSuccess: new Gauge({
      name: "fleet_health_job_part_last_success_timestamp_seconds", labelNames: ["part"], registers,
      help: "When each part of the health job last succeeded: its metrics are this old (S7-10).",
    }),
    gapsWarm: new Gauge({ name: "fleet_health_gaps_warm", help: "1 once the open-gap window has been filled at start; until then no gap figures are published (S7-11).", registers }),
    gapStepSeconds: new Gauge({ name: "fleet_health_gaps_step_seconds", help: "Each step of the last gaps pass: read (the delta), window (add, prune, open gaps), explain (S7-11).", labelNames: ["step"], registers }),
    gapDeltaRows: new Gauge({ name: "fleet_health_gap_delta_rows", help: "Rows the last gaps pass read behind its cursor, overlap included (S7-11).", registers }),
    gapWindowReadings: new Gauge({ name: "fleet_health_gap_window_readings", help: "Readings held in memory for the last hour's open gaps, about 12 bytes each (S7-11).", registers }),
    keyReuse: new Counter({
      name: "fleet_reading_key_reuse_total", labelNames: ["gateway"], registers,
      help: "Readings whose (device, epoch, seq) was seen again within the hour with another measured_at: a producer bug, stored as two rows (S8-2).",
    }),
    failures: new Counter({
      name: "fleet_health_job_failures_total", labelNames: ["part", "reason"], registers,
      help: "Health-job parts that failed, by reason: timeout (the database didn't answer in time) or error. The pass stops there (S7-10).",
    }),
  };
}

/** Updates the per-gateway and per-device gauges and the totals from one pass over the database. */
async function updateHealth(pool: pg.Pool, metrics: HealthMetrics, now: Date): Promise<void> {
  const gatewayRows = await allGateways(pool);
  const states = new Map<string, GatewayState>();
  for (const row of gatewayRows) {
    const { state } = classifyGateway({
      lastStatus: row.lastStatus, lastStatusReceivedAt: row.lastStatusReceivedAt,
      lastOfflineReceivedAt: row.lastOfflineReceivedAt, lastReadingReceivedAt: row.lastReadingReceivedAt,
    }, now);
    states.set(row.gatewayId, state);
    metrics.gatewayOffline.labels(row.gatewayId).set(state === "offline" ? 1 : 0);
    // At 0 before any reuse: a counter series that first appears at 1 has no increase() for the alert to see.
    metrics.keyReuse.labels(row.gatewayId).inc(0);
  }
  for (const state of ["online", "offline", "unknown"]) metrics.gatewaysByState.labels(state).set([...states.values()].filter((s) => s === state).length);
  const commands = gatewayRows.map((row) => ({ online: states.get(row.gatewayId) === "online", ...commandOf(row, states.get(row.gatewayId) ?? "unknown", now) }));
  metrics.commandsPending.labels("true").set(commands.filter((c) => c.pending && c.online).length);
  metrics.commandsPending.labels("false").set(commands.filter((c) => c.pending && !c.online).length);
  metrics.commandsStuck.set(commands.filter((c) => c.stuck).length);
  let stale = 0;
  for (const device of await devices(pool)) {
    const isStale = isDeviceStale(states.get(device.gatewayId) ?? "unknown", device.lastReadingReceivedAt, now);
    metrics.deviceStale.labels(device.deviceId, device.gatewayId).set(isStale ? 1 : 0);
    if (isStale) stale++;
  }
  metrics.devicesStale.set(stale);
}

/**
 * Sets each gateway's clock skew (the median lag of its live readings over the last 5 minutes) and returns them.
 * The `stored_at` bound only narrows the scan to its index: `stored_at` ≥ `received_at`, plus a minute for clocks (S7-11).
 */
async function updateClockSkew(pool: pg.Pool, metrics: HealthMetrics): Promise<Map<string, number>> {
  const { rows } = await pool.query<{ gateway_id: string; skew: number }>(`
    SELECT d.gateway_id, percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM r.received_at - r.measured_at)) AS skew
    FROM readings r JOIN devices d USING (device_id)
    WHERE r.received_at > now() - interval '5 minutes' AND r.stored_at > now() - interval '6 minutes'
      AND r.received_at - r.measured_at < interval '60 seconds'
    GROUP BY d.gateway_id`);
  const skews = new Map(rows.map((row) => [row.gateway_id, Number(row.skew)]));
  for (const [gatewayId, skew] of skews) metrics.clockSkew.labels(gatewayId).set(skew);
  return skews;
}

/**
 * Adds the readings stored since the last pass to the SLO and backfill counters (S6-1):
 * - **backfill**: measured before its gateway came back (`measured_at < came_back_at`), i.e. buffered while away.
 *   Counted, and left out of the SLO;
 * - **SLO total**: every other reading, except those from gateways whose clock skew is over 30 s;
 * - **SLO good**: of the total, `stored_at − measured_at ≤ 30 s`.
 * The SLO compares the gateway's clock (measured_at) with the database's (stored_at), so a skewed gateway distorts
 * its own readings. The fleet-wide ratio resists one bad clock, the skew gauge shows which one it is, and gateways
 * skewed by more than 30 s are counted separately instead.
 * `watermark` is the stored_at up to which readings were already counted; the last 5 s are left for the next pass,
 * so a transaction committing late isn't missed. Returns the new watermark.
 */
async function updateSlo(pool: pg.Pool, metrics: HealthMetrics, skews: Map<string, number>, watermark: Date, now: Date): Promise<Date> {
  const until = new Date(now.getTime() - 5000);
  if (until <= watermark) return watermark;
  const { rows } = await pool.query<{ gateway_id: string; total: string; good: string; backfill: string }>(`
    SELECT d.gateway_id,
           count(*) FILTER (WHERE NOT backfill) AS total,
           count(*) FILTER (WHERE NOT backfill AND r.stored_at - r.measured_at <= make_interval(secs => $3)) AS good,
           count(*) FILTER (WHERE backfill) AS backfill
    FROM readings r JOIN devices d USING (device_id) JOIN gateways g ON g.gateway_id = d.gateway_id
    CROSS JOIN LATERAL (SELECT g.came_back_at IS NOT NULL AND r.measured_at < g.came_back_at AS backfill) b
    WHERE r.stored_at > $1 AND r.stored_at <= $2
    GROUP BY d.gateway_id`,
    [watermark, until, SLO_SECONDS]);
  for (const row of rows) {
    metrics.backfill.inc(Number(row.backfill));
    if (Math.abs(skews.get(row.gateway_id) ?? 0) > SLO_MAX_SKEW_SECONDS) metrics.sloExcludedSkew.inc(Number(row.total));
    else {
      metrics.sloTotal.inc(Number(row.total));
      metrics.sloGood.inc(Number(row.good));
    }
  }
  return until;
}

/** How far back the fleet's open gaps look. */
export const GAP_WINDOW_MS = 3600_000;

/** The fleet's open gaps over the last hour, as the last gaps pass computed them; `/fleet` reports this (S7-11). */
export type FleetGaps =
  | { complete: true; asOf: string; gaps: number; missingReadings: number; explainedByDroppedTelemetry: number }
  | { complete: false; asOf: null };

/** The incremental gap state: the window, its cursor, and the last result. */
interface GapTracker {
  window: GapWindow;
  cursor: number | null;
  warm: boolean;
  last: FleetGaps;
}

/**
 * Fills the window at start, one gateway at a time through the per-gateway index path (about 50 ms each on the lab's
 * 4.3M rows), so no single query reads the whole fleet's hour, the query that used to hit the timeout under load.
 * It holds one connection at a time (each read is awaited before the next), so it can't starve the other parts of the
 * pool of five; it pauses between reads, and it starts only after the job's first pass. The
 * cursor starts at the database's clock *before* the first gateway is read: anything stored while warming is read
 * again by the first pass, through the overlap. A gateway whose query fails is retried until it succeeds. Each
 * gateway read stamps the gaps part as alive, so HealthJobStale fires for a warm-up that stops making progress for
 * 2 min, not for a slow one. Until it finishes, nothing is published: no false zeros.
 */
async function warmUp(pool: pg.Pool, metrics: HealthMetrics, tracker: GapTracker, onError: (error: unknown) => void, stopped: () => boolean): Promise<void> {
  tracker.cursor = (await retrying(() => timed(metrics, "gaps_warmup", () => databaseNow(pool)), onError, stopped)).getTime();
  const pending = (await retrying(() => allGateways(pool), onError, stopped)).map((g) => g.gatewayId);
  while (pending.length > 0 && !stopped()) {
    const gatewayId = pending[0] as string;
    const from = Date.now() - GAP_WINDOW_MS;
    try {
      tracker.window.add(await timed(metrics, "gaps_warmup", () => gatewayReadingsSince(pool, gatewayId, new Date(from))), from);
      pending.shift();
      // Progress counts as the gaps part being alive: HealthJobStale should fire for a warm-up that stalls, not for
      // one that is slow (500 cold per-gateway reads under load took longer than 2 min in the S7-11 reruns).
      metrics.partLastSuccess.labels("gaps").set(Date.now() / 1000);
      await sleep(WARMUP_YIELD_MS);
    } catch (error) {
      onError(error);
      await sleep(1000);
    }
  }
  tracker.warm = !stopped();
  metrics.gapsWarm.set(tracker.warm ? 1 : 0);
}

/**
 * The pause between two warm-up reads (S7-11). After a cold start the warm-up competed with everything else for the
 * event loop and for Postgres: health passes reached 10 s. The pause adds about 25 s to a 500-gateway warm-up.
 */
const WARMUP_YIELD_MS = 50;

/** Calls `work` until it succeeds (or the job stops), reporting each failure. */
async function retrying<T>(work: () => Promise<T>, onError: (error: unknown) => void, stopped: () => boolean): Promise<T> {
  for (;;) {
    try {
      return await work();
    } catch (error) {
      onError(error);
      if (stopped()) throw error;
      await sleep(5000);
    }
  }
}

/**
 * One gaps pass (S7-11): reads what was stored since the cursor (minus the overlap: `CURSOR_OVERLAP_MS` in core/gapWindow.ts),
 * adds it to the window, drops what aged out of the hour, and reads the open gaps from memory. Gaps are explained
 * against each gateway's lifetime counter (S5-9): only a gateway that has dropped readings *and* has a gap now needs
 * its history, read per gateway (about 50 ms); for every other gateway its gaps are unexplained by definition.
 */
async function updateGaps(pool: pg.Pool, metrics: HealthMetrics, tracker: GapTracker, now: Date): Promise<void> {
  const nowMs = now.getTime();
  const from = nowMs - GAP_WINDOW_MS;
  const bound = deltaLowerBound(tracker.cursor);
  let lap = performance.now();
  const step = (name: string) => {
    metrics.gapStepSeconds.labels(name).set((performance.now() - lap) / 1000);
    lap = performance.now();
  };
  const stored = await readingsStoredAfter(pool, new Date(bound ?? 0));
  const gatewayOf = new Map((await devices(pool)).map((d) => [d.deviceId, d.gatewayId]));
  // In place, not copied: these are the driver's fresh row objects, and copies were most of the pass's garbage.
  const delta = stored as StoredReading[];
  for (const row of delta) row.gatewayId = gatewayOf.get(row.deviceId) ?? "";
  step("read");
  metrics.gapDeltaRows.set(delta.length);
  tracker.window.add(delta, from);
  tracker.window.prune(from);
  for (const reuse of tracker.window.takeKeyReuses()) metrics.keyReuse.labels(reuse.gatewayId).inc();
  tracker.cursor = advanceCursor(tracker.cursor, delta.map((r) => r.storedAt));
  const open = tracker.window.openGaps(from, nowMs);
  step("window");
  const dropped = new Map((await allGateways(pool)).map((g) => [g.gatewayId, g.dropped ?? 0]));
  const withGaps = [...new Set(open.map((g) => g.gatewayId))];
  const explained = new Map<string, Array<{ missing: number; explained: boolean }>>();
  for (const id of withGaps.filter((g) => (dropped.get(g) ?? 0) > 0)) {
    explained.set(id, await gapsInWindow(pool, id, dropped.get(id) ?? 0, new Date(from), now, now));
  }
  const all = withGaps.flatMap((id) => explained.get(id) ?? open.filter((g) => g.gatewayId === id).map((g) => ({ missing: g.missing, explained: false })));
  const missingExplained = all.filter((g) => g.explained).reduce((s, g) => s + g.missing, 0);
  const missingUnexplained = all.filter((g) => !g.explained).reduce((s, g) => s + g.missing, 0);
  step("explain");
  metrics.openGapReadings.labels("true").set(missingExplained);
  metrics.openGapReadings.labels("false").set(missingUnexplained);
  metrics.gapWindowReadings.set(tracker.window.size());
  tracker.last = { complete: true, asOf: now.toISOString(), gaps: all.length, missingReadings: missingExplained + missingUnexplained, explainedByDroppedTelemetry: missingExplained };
}

/**
 * Runs one part of a pass and records how long it took in `fleet_health_job_pass_seconds{part}`. On success it stamps
 * the part's last-success time; on failure it counts the failure, by reason, and rethrows so the pass stops there.
 */
async function timed<T>(metrics: HealthMetrics, part: string, work: () => Promise<T>): Promise<T> {
  const started = performance.now();
  try {
    const result = await work();
    metrics.partLastSuccess.labels(part).set(Date.now() / 1000);
    return result;
  } catch (error) {
    metrics.failures.labels(part, isDatabaseTimeout(error) ? "timeout" : "error").inc();
    throw error;
  } finally {
    metrics.passSeconds.labels(part).set((performance.now() - started) / 1000);
  }
}

/**
 * Starts the job: a pass every 15 s (gaps every 4th pass, so every 60 s). A failing part (a query timeout, S7-10, or
 * any other error) stops the pass: it's counted in `fleet_health_job_failures_total`, reported through `onError`, and
 * retried at the next tick. The parts after it aren't run, so their metrics keep their last values, and
 * `fleet_health_job_part_last_success_timestamp_seconds{part}` says how old those are; HealthJobStale fires on it, and
 * HealthJobFailing on the counter. `intervalMs` is for tests. Returns the job: stop it, or read its last gaps pass.
 */
export function startHealthJob(pool: pg.Pool, registry: Registry, onError: (error: unknown) => void, intervalMs = 15_000): HealthJob {
  const metrics = createHealthMetrics(registry);
  let watermark = new Date();
  let pass = 0;
  let running = false;
  let stopped = false;
  const tracker: GapTracker = { window: new GapWindow(), cursor: null, warm: false, last: { complete: false, asOf: null } };
  // Every part starts "fresh as of the job's start", so a part that never succeeds (a warm-up that never finishes, say)
  // still trips HealthJobStale 2 min later, instead of never having a series at all.
  for (const part of ["health", "clock_skew", "slo", "gaps"]) metrics.partLastSuccess.labels(part).set(Date.now() / 1000);
  metrics.gapsWarm.set(0);
  let warmUpStarted = false;
  /** One pass; skipped if the previous pass is still running, so slow queries can't pile up. */
  const tick = async (): Promise<void> => {
    if (running) return;
    running = true;
    try {
      const now = new Date();
      await timed(metrics, "health", () => updateHealth(pool, metrics, now));
      const skews = await timed(metrics, "clock_skew", () => updateClockSkew(pool, metrics));
      watermark = await timed(metrics, "slo", () => updateSlo(pool, metrics, skews, watermark, now));
      if (pass++ % 4 === 0 && tracker.warm) await timed(metrics, "gaps", () => updateGaps(pool, metrics, tracker, now));
      metrics.lastRun.set(now.getTime() / 1000);
    } catch (error) {
      onError(error);
    } finally {
      running = false;
      // After the first pass, whatever its outcome, so a failing part can't keep the window cold.
      if (!warmUpStarted) {
        warmUpStarted = true;
        void warmUp(pool, metrics, tracker, onError, () => stopped);
      }
    }
  };
  void tick();
  const timer = setInterval(() => void tick(), intervalMs);
  return {
    stop: () => {
      stopped = true;
      clearInterval(timer);
    },
    fleetGaps: () => tracker.last,
  };
}

/** The running job: stop it, or read the fleet's open gaps as of its last gaps pass. */
export interface HealthJob {
  stop: () => void;
  fleetGaps: () => FleetGaps;
}
