/**
 * Integration tests for the API against a real, migrated Postgres. Rows are inserted directly; "now" is pinned to
 * 12:00Z, so health ages are exact. The command publisher is a fake that records what was published; the command
 * reconciler in front of it is the real one.
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { applyMigrations, ensurePartitions, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";
import { createRegistry } from "@fleet-ingest/observability";
import { buildApp } from "../src/app.ts";
import { gapsOf } from "../src/core/gapWindow.ts";
import { startHealthJob } from "../src/healthJob.ts";
import { readHealthSnapshot, writeHealthSnapshot } from "../src/healthSnapshot.ts";
import { tryWorkerLock } from "../src/workerLock.ts";
import { gaps } from "../src/queries.ts";
import type { CommandPublisher } from "../src/commandPublisher.ts";
import { startCommandReconciler } from "../src/commandReconciler.ts";
import type { CommandReconciler } from "../src/commandReconciler.ts";

const TOKEN = "test-token-0123456789";
/** What the health worker's last gaps pass would have written to health_snapshot; /fleet passes it through (S7-11, S8-3). */
const FLEET_GAPS = { complete: true as const, asOf: "2026-10-14T11:59:30.000Z", gaps: 2, missingReadings: 5, explainedByDroppedTelemetry: 3 };
const auth = { authorization: `Bearer ${TOKEN}` };
let container: StartedPostgreSqlContainer;
/** A session holding the health worker lock, as the worker's lock connection does: snapshot writes are fenced on it (S8-3). */
let leader: pg.Client;
let pool: pg.Pool;
let app: ReturnType<typeof buildApp>;
const published: Array<{ gatewayId: string; mode: string }> = [];
let brokerConnected = true;
let reconciler: CommandReconciler;

/** A fake publisher: records commands, and reports the connection state the test sets. */
const commands: CommandPublisher = {
  publishMode: async (gatewayId, mode) => void published.push({ gatewayId, mode }),
  connected: () => brokerConnected,
  onConnect: () => {},
  close: async () => {},
};

/**
 * Four gateways, one per health rule, at "now" = 12:00Z:
 * - gw-1: status online heard at 11:59:30; its device's last reading was received at 11:20:46 → online, device stale;
 * - gw-2: Last Will (offline) at 11:59; readings with a gap and a power loss; droppedTelemetry 3 → offline;
 * - gw-3: no status ever, but a reading received at 11:59:50 → online by evidence;
 * - gw-4: no status ever, last reading hours ago → unknown.
 */
async function seed(): Promise<void> {
  await pool.query(`
    INSERT INTO gateways (gateway_id, first_seen, last_status, last_status_at, last_status_received_at, last_offline_received_at, buffered, dropped, mode) VALUES
      ('gw-1', '2026-10-14 10:00Z', 'online',  '2026-10-14 11:59:30Z', '2026-10-14 11:59:30Z', NULL, 0, 0, 'surplus'),
      ('gw-2', '2026-10-14 10:00Z', 'offline', '2026-10-14 11:59:00Z', '2026-10-14 11:59:00Z', '2026-10-14 11:59:00Z', 0, 3, 'surplus'),
      ('gw-3', '2026-10-14 10:00Z', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
      ('gw-4', '2026-10-14 08:00Z', NULL, NULL, NULL, NULL, NULL, NULL, NULL),
      ('gw-5', '2026-10-10 00:00Z', 'online', '2026-10-14 11:59:30Z', '2026-10-14 11:59:30Z', NULL, 0, 300, 'surplus')`);
  await pool.query(`
    INSERT INTO devices VALUES ('dev-1', 'gw-1', 'Inverter', '2026-10-14 10:00Z'), ('dev-g', 'gw-2', 'Battery', '2026-10-14 10:00Z'),
                               ('dev-3', 'gw-3', 'GridMeter', '2026-10-14 10:00Z'), ('dev-4', 'gw-4', 'Charger', '2026-10-14 08:00Z'),
                               ('dev-life', 'gw-5', 'Battery', '2026-10-10 00:00Z')`);
  await pool.query(`
    INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind, pv_power_w)
    SELECT 'dev-1', 1, s, timestamptz '2026-10-14 11:00:00Z' + s * interval '5 seconds',
           timestamptz '2026-10-14 11:00:01Z' + s * interval '5 seconds', 'Inverter', s * 10
    FROM generate_series(0, 249) AS s`);
  await pool.query(`
    INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind)
    SELECT 'dev-g', 1, s, timestamptz '2026-10-14 11:30:00Z' + s * interval '5 seconds', timestamptz '2026-10-14 11:30:01Z' + s * interval '5 seconds', 'Battery'
    FROM generate_series(1, 10) AS s WHERE s NOT IN (4, 5, 6)
    UNION ALL
    SELECT 'dev-g', 2, s, timestamptz '2026-10-14 11:40:00Z' + s * interval '5 seconds', timestamptz '2026-10-14 11:40:01Z' + s * interval '5 seconds', 'Battery'
    FROM generate_series(1, 5) AS s`);
  await pool.query(`
    INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind)
    SELECT 'dev-life', 1, s, timestamptz '2026-10-11 00:00:00Z' + s * interval '5 seconds', timestamptz '2026-10-11 00:00:01Z' + s * interval '5 seconds', 'Battery'
    FROM generate_series(1, 1000) AS s WHERE s NOT BETWEEN 101 AND 400
    UNION ALL
    SELECT 'dev-life', 2, s, timestamptz '2026-10-14 11:30:00Z' + s * interval '5 seconds', timestamptz '2026-10-14 11:30:01Z' + s * interval '5 seconds', 'Battery'
    FROM generate_series(1, 200) AS s WHERE s NOT BETWEEN 51 AND 100`);
  await pool.query(`
    INSERT INTO device_state (device_id, epoch, seq, measured_at, received_at, kind, pv_power_w) VALUES
      ('dev-1', 1, 249, '2026-10-14 11:20:45Z', '2026-10-14 11:20:46Z', 'Inverter', 2490),
      ('dev-g', 2, 5,   '2026-10-14 11:40:25Z', '2026-10-14 11:40:26Z', 'Battery', NULL),
      ('dev-3', 1, 1,   '2026-10-14 11:59:49Z', '2026-10-14 11:59:50Z', 'GridMeter', NULL),
      ('dev-4', 1, 1,   '2026-10-14 08:00:00Z', '2026-10-14 08:00:01Z', 'Charger', NULL)`);
}

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  const client = await pool.connect();
  await applyMigrations(client, loadMigrations(MIGRATIONS_DIR));
  // The seed's fixed dates need their days to exist: readings has no default partition (S8-2).
  await ensurePartitions(client, new Date("2026-10-01T00:00:00Z"), new Date(Date.now() + 14 * 86_400_000));
  client.release();
  await seed();
  leader = new pg.Client({ connectionString: container.getConnectionUri() });
  await leader.connect();
  expect(await tryWorkerLock(leader)).toBe(true);
  await writeHealthSnapshot(leader, FLEET_GAPS, "test-worker");
  reconciler = startCommandReconciler({ pool, commands, onError: () => {}, intervalMs: 3600_000 });
  app = buildApp({ pool, commands, reconciler, apiToken: TOKEN, now: () => new Date("2026-10-14T12:00:00Z"), logger: false, fleetGaps: (now) => readHealthSnapshot(pool, now) });
});

afterAll(async () => {
  await app?.close();
  reconciler?.stop();
  await leader?.end();
  await pool?.end();
  await container?.stop();
});

describe("health endpoints", () => {
  it("are open, and ready needs both the database and the broker", async () => {
    expect((await app.inject({ url: "/health/live" })).statusCode).toBe(200);
    expect((await app.inject({ url: "/health/ready" })).json()).toEqual({ status: "ready", database: true, broker: true });
    brokerConnected = false;
    expect((await app.inject({ url: "/health/ready" })).statusCode).toBe(503);
    brokerConnected = true;
  });

  it("refuse data endpoints without the right token", async () => {
    expect((await app.inject({ url: "/fleet" })).statusCode).toBe(401);
    expect((await app.inject({ url: "/fleet", headers: { authorization: "Bearer wrong-token-0123456" } })).statusCode).toBe(401);
  });
});

describe("a database that doesn't answer in time (S7-10)", () => {
  it("answers 503, not 500, when a query is cancelled by the statement timeout", async () => {
    const slow = new pg.Pool({ connectionString: container.getConnectionUri(), statement_timeout: 300 });
    const slowApp = buildApp({ pool: slow, commands, reconciler, apiToken: TOKEN, now: () => new Date("2026-10-14T12:00:00Z"), logger: false, fleetGaps: (now) => readHealthSnapshot(slow, now) });
    const holder = new pg.Client({ connectionString: container.getConnectionUri() });
    await holder.connect();
    await holder.query("BEGIN");
    await holder.query("LOCK TABLE gateways IN ACCESS EXCLUSIVE MODE");
    const response = await slowApp.inject({ url: "/fleet", headers: { authorization: `Bearer ${TOKEN}` } });
    await holder.query("ROLLBACK");
    await holder.end();
    await slowApp.close();
    await slow.end();

    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({ error: "the database didn't answer in time" });
  });
});

describe("fleet and gateway health", () => {
  it("counts online, offline and unknown separately, and stale devices", async () => {
    const fleet = (await app.inject({ url: "/fleet", headers: auth })).json();
    expect(fleet.gateways).toEqual({ online: 3, offline: 1, unknown: 1, total: 5 });
    expect(fleet.devices).toEqual({ total: 5, stale: 2 });
    expect(fleet).toHaveProperty("healthAsOfLagSeconds");
  });

  it("explains each gateway's state, and marks its stale devices", async () => {
    const gw1 = (await app.inject({ url: "/gateways/gw-1", headers: auth })).json();
    expect(gw1.health).toEqual({ state: "online", reason: "last status is online" });
    expect(gw1.devices).toMatchObject([{ deviceId: "dev-1", stale: true }]);
    const gw3 = (await app.inject({ url: "/gateways/gw-3", headers: auth })).json();
    expect(gw3.health.state).toBe("online");
    expect((await app.inject({ url: "/gateways/gw-2", headers: auth })).json().health.state).toBe("offline");
    expect((await app.inject({ url: "/gateways/nope", headers: auth })).statusCode).toBe(404);
  });
});

describe("open gaps over the last hour, incrementally (S7-11)", () => {
  it("/fleet reports the health worker's snapshot as it is, with its age (S8-3)", async () => {
    expect((await app.inject({ url: "/fleet", headers: auth })).json().openGapsLastHour).toEqual({ ...FLEET_GAPS, ageSeconds: 30 });
  });

  it("/fleet says not complete before the worker's first gaps pass, and never writes an incomplete pass over a snapshot", async () => {
    await pool.query("DELETE FROM health_snapshot");
    try {
      expect((await app.inject({ url: "/fleet", headers: auth })).json().openGapsLastHour).toEqual({ complete: false, asOf: null, ageSeconds: null });
      expect(await writeHealthSnapshot(leader, { complete: false, asOf: null }, "test-worker")).toBe("incomplete");
      expect((await pool.query("SELECT count(*)::int AS n FROM health_snapshot")).rows[0]?.n).toBe(0);
    } finally {
      await writeHealthSnapshot(leader, FLEET_GAPS, "test-worker");
    }
  });

  it("the SQL gap query and the oracle the incremental window is tested against agree on the seeded readings", async () => {
    const from = new Date("2026-10-10T00:00:00Z");
    const to = new Date("2026-10-14T12:00:00Z");
    const { rows } = await pool.query<{ deviceId: string; gatewayId: string; epoch: number; seq: number; measuredAt: Date }>(
      `SELECT r.device_id AS "deviceId", d.gateway_id AS "gatewayId", r.epoch::int, r.seq::int, r.measured_at AS "measuredAt" FROM readings r JOIN devices d USING (device_id)`);
    const oracle = gapsOf(rows.map((r) => ({ ...r, measuredAt: r.measuredAt.getTime() })), from.getTime(), to.getTime());
    const key = (g: { gatewayId: string; deviceId: string; epoch: number; fromSeq: number; toSeq: number; missing: number; before: Date }) =>
      `${g.gatewayId} ${g.deviceId} ${g.epoch} ${g.fromSeq}-${g.toSeq} ${g.missing} ${g.before.toISOString()}`;
    expect(oracle.length).toBeGreaterThan(0);
    expect((await gaps(pool, from, to)).map(key).sort()).toEqual(oracle.map(key).sort());
  });

  it("the health job warms up, finds a gap measured in the last hour, sees it close when a replay is stored later, and publishes each pass to the snapshot", async () => {
    const live = (seq: number, agoSeconds: number) =>
      pool.query(`INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind) VALUES ('dev-live', 1, $1, now() - $2 * interval '1 second', now(), 'Battery')`, [seq, agoSeconds]);
    await pool.query("INSERT INTO gateways (gateway_id, first_seen) VALUES ('gw-live', now()) ON CONFLICT DO NOTHING");
    await pool.query("INSERT INTO devices (device_id, gateway_id, kind, first_seen) VALUES ('dev-live', 'gw-live', 'Battery', now()) ON CONFLICT DO NOTHING");
    for (const seq of [1, 2, 3, 6, 7]) await live(seq, 600 - seq * 5);
    const job = startHealthJob(pool, createRegistry("api-gaps-test"), () => undefined, 100, async (g) => void (await writeHealthSnapshot(leader, g, "test-worker")));
    const until = async (predicate: () => boolean) => {
      const deadline = Date.now() + 20_000;
      while (!predicate() && Date.now() < deadline) await new Promise((resolve) => setTimeout(resolve, 100));
    };
    try {
      await until(() => job.fleetGaps().complete);
      expect(job.fleetGaps()).toMatchObject({ complete: true, gaps: 1, missingReadings: 2, explainedByDroppedTelemetry: 0 });
      await live(4, 580);
      await live(5, 575);
      await until(() => { const g = job.fleetGaps(); return g.complete && g.gaps === 0; });
      expect(job.fleetGaps()).toMatchObject({ complete: true, gaps: 0, missingReadings: 0 });
      job.stop();
      await new Promise((resolve) => setTimeout(resolve, 300));
      const { ageSeconds: _age, ...published } = await readHealthSnapshot(pool, new Date());
      expect(published).toEqual(job.fleetGaps());
    } finally {
      job.stop();
      await writeHealthSnapshot(leader, FLEET_GAPS, "test-worker");
    }
  });
});

describe("gaps", () => {
  it("finds the gap within an epoch, not across the power loss, and explains it by the gateway's dropped count", async () => {
    const response = await app.inject({ url: "/devices/dev-g/gaps?from=2026-10-14T11:00:00Z&to=2026-10-14T12:00:00Z", headers: auth });
    expect(response.json()).toMatchObject({
      deviceId: "dev-g", gatewayId: "gw-2", droppedTelemetry: 3,
      gaps: [{ epoch: 1, fromSeq: 4, toSeq: 6, missing: 3, explained: true }],
    });
  });

  it("spends the lifetime dropped count on old gaps first: 300 drops, an old 300-gap, a new 50-gap in the window → unexplained", async () => {
    const response = await app.inject({ url: "/devices/dev-life/gaps?from=2026-10-14T11:00:00Z&to=2026-10-14T12:00:00Z", headers: auth });
    expect(response.json()).toMatchObject({ droppedTelemetry: 300, gaps: [{ epoch: 2, fromSeq: 51, toSeq: 100, missing: 50, explained: false }] });
  });

  it("reports no gaps for a complete device", async () => {
    const response = await app.inject({ url: "/devices/dev-1/gaps?from=2026-10-14T11:00:00Z&to=2026-10-14T12:00:00Z", headers: auth });
    expect(response.json().gaps).toEqual([]);
  });
});

describe("readings", () => {
  it("returns a device's state, and 404 for an unknown device", async () => {
    expect((await app.inject({ url: "/devices/dev-1/state", headers: auth })).json()).toMatchObject({ deviceId: "dev-1", epoch: 1, seq: 249, pvPowerW: 2490 });
    expect((await app.inject({ url: "/devices/nope/state", headers: auth })).statusCode).toBe(404);
  });

  it("pages through a window with cursors, without gaps or repeats", async () => {
    const seqs: number[] = [];
    let cursor: string | null = null;
    do {
      const query: string = `from=2026-10-14T11:00:00Z&to=2026-10-14T12:00:00Z&limit=100${cursor ? `&cursor=${cursor}` : ""}`;
      const page = (await app.inject({ url: `/devices/dev-1/readings?${query}`, headers: auth })).json();
      seqs.push(...page.items.map((r: { seq: number }) => r.seq));
      cursor = page.nextCursor;
    } while (cursor);
    expect(seqs).toEqual(Array.from({ length: 250 }, (_, i) => i));
  });

  it("rejects a window wider than 7 days", async () => {
    const response = await app.inject({ url: "/devices/dev-1/readings?from=2026-10-01T00:00:00Z&to=2026-10-14T00:00:00Z", headers: auth });
    expect(response.statusCode).toBe(400);
  });
});

describe("commands", () => {
  it("stores a valid mode for a known gateway, publishes it, answers 202, and shows it pending until reported", async () => {
    const response = await app.inject({ method: "PUT", url: "/gateways/gw-1/mode", headers: auth, payload: { mode: "max" } });
    expect(response.statusCode).toBe(202);
    expect(published).toContainEqual({ gatewayId: "gw-1", mode: "max" });
    const { rows } = await pool.query("SELECT desired_mode, desired_at FROM gateways WHERE gateway_id = 'gw-1'");
    expect(rows[0]).toEqual({ desired_mode: "max", desired_at: new Date("2026-10-14T12:00:00Z") });
    const gateway = (await app.inject({ url: "/gateways/gw-1", headers: auth })).json();
    expect(gateway.command).toEqual({ desiredMode: "max", reportedMode: "surplus", pending: true, pendingSince: "2026-10-14T12:00:00.000Z", stuck: false });
    expect((await app.inject({ url: "/fleet", headers: auth })).json().commands).toEqual({ pending: 1, stuckGateways: [] });
  });

  it("refuses an invalid mode (400) and an unknown gateway (404)", async () => {
    expect((await app.inject({ method: "PUT", url: "/gateways/gw-1/mode", headers: auth, payload: { mode: "turbo" } })).statusCode).toBe(400);
    expect((await app.inject({ method: "PUT", url: "/gateways/nope/mode", headers: auth, payload: { mode: "off" } })).statusCode).toBe(404);
  });

  it("while the broker is down, still stores the mode and answers 202; the reconciler publishes it once it's back", async () => {
    published.length = 0;
    brokerConnected = false;
    const response = await app.inject({ method: "PUT", url: "/gateways/gw-5/mode", headers: auth, payload: { mode: "off" } });
    expect(response.statusCode).toBe(202);
    expect(response.json().delivery).toMatch(/reconciler/);
    expect(published).toEqual([]);
    brokerConnected = true;
    expect(await reconciler.reconcile(true)).toBe(2);
    expect(published).toEqual([{ gatewayId: "gw-1", mode: "max" }, { gatewayId: "gw-5", mode: "off" }]);
  });
});
