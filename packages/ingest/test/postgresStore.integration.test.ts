/**
 * Integration tests for the batch store against a real, migrated Postgres 17.
 */
import { connect as connectTcp, createServer } from "node:net";
import type { AddressInfo, Socket } from "node:net";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { openIngestPool } from "../src/adapters/databasePool.ts";
import { assertPartitionsCover } from "../src/adapters/partitionCheck.ts";
import { createPostgresStore } from "../src/adapters/postgresStore.ts";
import type { IngestStore, ItemOutcome, StoreItem } from "../src/adapters/postgresStore.ts";
import { classifyDatabaseError } from "../src/core/errors.ts";
import type { ReadingRow, StatusRow } from "../src/core/rows.ts";
import { startMigratedPostgres } from "./support/containers.ts";

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let store: IngestStore;

beforeAll(async () => {
  container = await startMigratedPostgres();
  pool = new pg.Pool({ connectionString: container.getConnectionUri() });
  store = createPostgresStore(pool);
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

/** A reading row with sensible defaults. */
function row(overrides: Partial<ReadingRow> = {}): ReadingRow {
  return {
    gatewayId: "gw-1", deviceId: "dev-1", epoch: 1, seq: 1, kind: "Battery",
    measuredAt: new Date("2026-10-14T12:00:00Z"), receivedAt: new Date("2026-10-14T12:00:01Z"),
    pvPowerW: null, batteryPowerW: 250, gridPowerW: null, evPowerW: null, energyWh: null, socPercent: 55.5, chargeLimitA: null,
    ...overrides,
  };
}

/** Wraps a reading row as a store item. */
function reading(r: ReadingRow): StoreItem {
  return { routed: { type: "reading", row: r }, topic: `homes/${r.gatewayId}/devices/${r.deviceId}/telemetry`, payload: new Uint8Array([1]) };
}

/** Wraps a status row as a store item. */
function status(s: StatusRow): StoreItem {
  return { routed: { type: "status", row: s }, topic: `homes/${s.gatewayId}/status`, payload: new Uint8Array([2]) };
}

/** Writes one batch and returns its outcome kinds, for short assertions. */
async function write(...items: StoreItem[]): Promise<string[]> {
  return (await store.writeBatch(items)).map((o: ItemOutcome) => (o.kind === "dead_letter" ? `dead:${o.reason}` : o.kind));
}

describe("readings", () => {
  it("stores a reading once: a second delivery of the same (device, epoch, seq), in another batch or the same one, is a duplicate", async () => {
    expect(await write(reading(row()))).toEqual(["inserted"]);
    expect(await write(reading(row()))).toEqual(["duplicate"]);
    expect(await write(reading(row({ seq: 2 })), reading(row({ seq: 2 })))).toEqual(["inserted", "duplicate"]);
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM readings WHERE device_id = 'dev-1'");
    expect(rows[0].n).toBe(2);
  });

  it("registers the gateway and binds the device to it on first sight", async () => {
    await write(reading(row({ gatewayId: "gw-2", deviceId: "dev-9" })));
    const { rows } = await pool.query("SELECT gateway_id, kind FROM devices WHERE device_id = 'dev-9'");
    expect(rows).toEqual([{ gateway_id: "gw-2", kind: "Battery" }]);
  });

  it("treats the same seq in a new epoch as a new reading", async () => {
    expect(await write(reading(row({ deviceId: "dev-3", epoch: 1, seq: 10 })), reading(row({ deviceId: "dev-3", epoch: 2, seq: 10 })))).toEqual(["inserted", "inserted"]);
  });

  it("rejects the whole batch when one row is bad (a NUL in text, SQLSTATE class 22), so the pipeline can isolate it", async () => {
    await expect(store.writeBatch([reading(row({ deviceId: "dev-ok", seq: 1 })), reading(row({ deviceId: "dev-\u0000bad" }))]))
      .rejects.toMatchObject({ code: expect.stringMatching(/^22/) });
    const { rows } = await pool.query("SELECT count(*)::int AS n FROM readings WHERE device_id = 'dev-ok'");
    expect(rows[0].n).toBe(0);
  });
});

describe("device binding (D6)", () => {
  it("dead-letters a reading from a second gateway for a bound device, and registers nothing for the intruder", async () => {
    await write(reading(row({ gatewayId: "gw-home", deviceId: "dev-bound" })));
    expect(await write(reading(row({ gatewayId: "gw-intruder", deviceId: "dev-bound", seq: 2 })))).toEqual(["dead:foreign_device"]);
    const { rows } = await pool.query(`
      SELECT (SELECT count(*) FROM readings WHERE device_id = 'dev-bound')::int AS readings,
             (SELECT count(*) FROM gateways WHERE gateway_id = 'gw-intruder')::int AS intruder,
             (SELECT count(*) FROM dead_letters WHERE reason = 'foreign_device')::int AS dead`);
    expect(rows[0]).toEqual({ readings: 1, intruder: 0, dead: 1 });
  });

  it("gives a new device to the first gateway in the batch that sends it", async () => {
    expect(await write(
      reading(row({ gatewayId: "gw-a", deviceId: "dev-contested", seq: 1 })),
      reading(row({ gatewayId: "gw-b", deviceId: "dev-contested", seq: 2 })),
    )).toEqual(["inserted", "dead:foreign_device"]);
  });
});

describe("device_state is newer-only (D4)", () => {
  it("keeps the newest reading when an older backfill arrives after live data", async () => {
    await write(reading(row({ deviceId: "dev-s", seq: 50, measuredAt: new Date("2026-10-14T12:10:00Z"), batteryPowerW: 50 })));
    await write(reading(row({ deviceId: "dev-s", seq: 10, measuredAt: new Date("2026-10-14T11:00:00Z"), batteryPowerW: 10 })));
    const { rows } = await pool.query("SELECT seq::int, battery_power_w FROM device_state WHERE device_id = 'dev-s'");
    expect(rows[0]).toEqual({ seq: 50, battery_power_w: 50 });
  });

  it("takes the newest of a batch whatever its position, and breaks a timestamp tie on (epoch, seq)", async () => {
    const t = new Date("2026-10-14T12:20:00Z");
    await write(
      reading(row({ deviceId: "dev-t", seq: 8, measuredAt: t, batteryPowerW: 8 })),
      reading(row({ deviceId: "dev-t", seq: 9, measuredAt: new Date("2026-10-14T12:19:00Z"), batteryPowerW: 9 })),
      reading(row({ deviceId: "dev-t", seq: 7, measuredAt: t, batteryPowerW: 7 })),
    );
    const { rows } = await pool.query("SELECT seq::int FROM device_state WHERE device_id = 'dev-t'");
    expect(rows[0]).toEqual({ seq: 8 });
  });
});

describe("status", () => {
  const at = (iso: string) => new Date(iso);

  it("is newer-only: a Last Will stamped before the reboot's online can't mark a live gateway offline", async () => {
    await write(status({ gatewayId: "gw-reboot", state: "online", statusAt: at("2026-10-14T12:00:30Z"), receivedAt: at("2026-10-14T12:00:30Z"), buffered: 0, dropped: 0, mode: "surplus" }));
    await write(status({ gatewayId: "gw-reboot", state: "offline", statusAt: at("2026-10-14T12:00:00Z"), receivedAt: at("2026-10-14T12:00:00Z"), buffered: null, dropped: null, mode: null }));
    const { rows } = await pool.query("SELECT last_status, last_status_at FROM gateways WHERE gateway_id = 'gw-reboot'");
    expect(rows[0]).toEqual({ last_status: "online", last_status_at: at("2026-10-14T12:00:30Z") });
  });

  it("records when it last heard from the gateway even when the guard rejects an older status", async () => {
    await write(status({ gatewayId: "gw-heard", state: "online", statusAt: at("2026-10-14T12:00:30Z"), receivedAt: at("2026-10-14T12:00:30Z"), buffered: 0, dropped: 0, mode: null }));
    await write(status({ gatewayId: "gw-heard", state: "online", statusAt: at("2026-10-14T11:50:00Z"), receivedAt: at("2026-10-14T12:01:30Z"), buffered: 0, dropped: 0, mode: null }));
    const { rows } = await pool.query("SELECT last_status_at, last_status_received_at FROM gateways WHERE gateway_id = 'gw-heard'");
    expect(rows[0]).toEqual({ last_status_at: at("2026-10-14T12:00:30Z"), last_status_received_at: at("2026-10-14T12:01:30Z") });
  });

  it("records when it last received an offline status, and keeps it through later online statuses", async () => {
    await write(status({ gatewayId: "gw-will", state: "offline", statusAt: at("2026-10-14T12:00:00Z"), receivedAt: at("2026-10-14T12:00:00Z"), buffered: null, dropped: null, mode: null }));
    await write(status({ gatewayId: "gw-will", state: "online", statusAt: at("2026-10-14T12:01:00Z"), receivedAt: at("2026-10-14T12:01:00Z"), buffered: 0, dropped: 0, mode: null }));
    const { rows } = await pool.query("SELECT last_status, last_offline_received_at FROM gateways WHERE gateway_id = 'gw-will'");
    expect(rows[0]).toEqual({ last_status: "online", last_offline_received_at: at("2026-10-14T12:00:00Z") });
  });

  it("records when the gateway came back: the first online after the latest offline, not later heartbeats", async () => {
    const st = (state: "online" | "offline", iso: string) => status({ gatewayId: "gw-back", state, statusAt: at(iso), receivedAt: at(iso), buffered: 0, dropped: 0, mode: null });
    await write(st("online", "2026-10-14T12:00:00Z"));
    await write(st("offline", "2026-10-14T12:05:00Z"));
    await write(st("online", "2026-10-14T12:07:00Z"));
    await write(st("online", "2026-10-14T12:08:00Z"));
    const { rows } = await pool.query("SELECT came_back_at FROM gateways WHERE gateway_id = 'gw-back'");
    expect(rows[0]).toEqual({ came_back_at: at("2026-10-14T12:07:00Z") });
  });

  it("takes the first status for a gateway first seen through its readings", async () => {
    await write(reading(row({ gatewayId: "gw-new", deviceId: "dev-new" })));
    await write(status({ gatewayId: "gw-new", state: "online", statusAt: at("2026-10-14T12:00:00Z"), receivedAt: at("2026-10-14T12:00:00Z"), buffered: 0, dropped: 0, mode: null }));
    const { rows } = await pool.query("SELECT last_status FROM gateways WHERE gateway_id = 'gw-new'");
    expect(rows[0]).toEqual({ last_status: "online" });
  });

  it("keeps the counters when the Last Will arrives without them, even within one batch", async () => {
    await write(
      status({ gatewayId: "gw-5", state: "online", statusAt: at("2026-10-14T12:00:00Z"), receivedAt: at("2026-10-14T12:00:00Z"), buffered: 3, dropped: 1, mode: "surplus" }),
      status({ gatewayId: "gw-5", state: "offline", statusAt: at("2026-10-14T12:05:00Z"), receivedAt: at("2026-10-14T12:05:00Z"), buffered: null, dropped: null, mode: null }),
    );
    const { rows } = await pool.query("SELECT last_status, buffered, dropped, mode FROM gateways WHERE gateway_id = 'gw-5'");
    expect(rows[0]).toEqual({ last_status: "offline", buffered: "3", dropped: "1", mode: "surplus" });
  });
});

describe("dead letters", () => {
  it("keeps the payload byte for byte, in the same transaction as the batch's readings", async () => {
    const payload = new Uint8Array([0xff, 0x00, 0x7b]);
    const dead: StoreItem = { routed: { type: "dead_letter", row: { topic: "x", payload, reason: "invalid_json", detail: "bad", receivedAt: new Date() } }, topic: "x", payload };
    expect(await write(reading(row({ deviceId: "dev-dl" })), dead)).toEqual(["inserted", "dead:invalid_json"]);
    const { rows } = await pool.query("SELECT payload FROM dead_letters WHERE topic = 'x'");
    expect(new Uint8Array(rows[0].payload)).toEqual(payload);
  });
});

describe("readings partitioned by day on measured_at (S8-2)", () => {
  /** The partition a stored reading landed in. */
  async function partitionOf(deviceId: string, seq: number): Promise<string[]> {
    const { rows } = await pool.query<{ p: string }>("SELECT tableoid::regclass::text AS p FROM readings WHERE device_id = $1 AND seq = $2 ORDER BY p", [deviceId, seq]);
    return rows.map((r) => r.p);
  }

  it("a reading at 23:59:59.999 and its redelivery later are stored once, in that day's partition; 00:00:00.000 goes to the next day", async () => {
    const lastMs = row({ deviceId: "dev-boundary", seq: 1, measuredAt: new Date("2026-10-14T23:59:59.999Z") });
    const midnight = row({ deviceId: "dev-boundary", seq: 2, measuredAt: new Date("2026-10-15T00:00:00.000Z") });
    expect(await write(reading(lastMs))).toEqual(["inserted"]);
    expect(await write(reading(midnight), reading(lastMs))).toEqual(["inserted", "duplicate"]);
    expect(await partitionOf("dev-boundary", 1)).toEqual(["readings_p20261014"]);
    expect(await partitionOf("dev-boundary", 2)).toEqual(["readings_p20261015"]);
  });

  it("ingest's startup check passes when the partitions cover what it may accept, and names the hole when they don't", async () => {
    await expect(assertPartitionsCover(pool, 7, new Date())).resolves.toBeUndefined();
    await expect(assertPartitionsCover(pool, 60, new Date())).rejects.toThrow(/no partition for part of/);
  });

  it("a key reused with another timestamp is now stored twice (the D4 change): the detector, not the key, catches it", async () => {
    const first = row({ deviceId: "dev-reuse", seq: 9, measuredAt: new Date("2026-10-14T10:00:00Z") });
    const reused = row({ deviceId: "dev-reuse", seq: 9, measuredAt: new Date("2026-10-14T10:05:00Z") });
    expect(await write(reading(first))).toEqual(["inserted"]);
    expect(await write(reading(reused))).toEqual(["inserted"]);
    expect(await partitionOf("dev-reuse", 9)).toHaveLength(2);
  });
});

describe("a batch whose connection fails mid-write (S7-10)", () => {
  /** Holds an exclusive lock on `devices`, the first table a batch reads, so the next batch blocks inside its transaction. */
  async function lockDevices(): Promise<pg.Client> {
    const holder = new pg.Client({ connectionString: container.getConnectionUri() });
    await holder.connect();
    await holder.query("BEGIN");
    await holder.query("LOCK TABLE devices IN ACCESS EXCLUSIVE MODE");
    return holder;
  }

  /** Resolves with the pid of the backend waiting on a lock, once there is one. */
  async function blockedBackend(): Promise<number> {
    for (let attempt = 0; attempt < 100; attempt++) {
      const { rows } = await pool.query<{ pid: number }>("SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()");
      if (rows[0]) return rows[0].pid;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    throw new Error("no batch is waiting on the lock");
  }

  it("a backend killed mid-batch (the server says why, then closes) rejects the batch as retryable, and the next batch works", async () => {
    const holder = await lockDevices();
    const blocked = store.writeBatch([reading(row({ deviceId: "dev-killed" }))]);
    const outcome = blocked.then(() => undefined, (error: unknown) => error);
    await pool.query("SELECT pg_terminate_backend($1)", [await blockedBackend()]);
    const error = await outcome;
    await holder.query("ROLLBACK");
    await holder.end();

    expect(error, "the batch should have failed").toBeInstanceOf(Error);
    expect(classifyDatabaseError(error)).toBe("retryable");
    expect(await write(reading(row({ deviceId: "dev-killed" })))).toEqual(["inserted"]);
  });

  it("a connection dropped mid-batch with no error from the server (as toxiproxy does) rejects the batch as retryable instead of crashing the process", async () => {
    const sockets: Socket[] = [];
    const proxy = createServer((client) => {
      const upstream = connectTcp(container.getPort(), container.getHost());
      sockets.push(client, upstream);
      client.pipe(upstream).pipe(client);
      client.on("error", () => undefined);
      upstream.on("error", () => undefined);
    });
    await new Promise<void>((resolve) => proxy.listen(0, "127.0.0.1", resolve));
    const url = new URL(container.getConnectionUri());
    url.host = `127.0.0.1:${(proxy.address() as AddressInfo).port}`;
    const proxied = new pg.Pool({ connectionString: url.toString(), max: 1 });
    proxied.on("error", () => undefined);
    const proxiedStore = createPostgresStore(proxied);
    const holder = await lockDevices();
    const outcome = proxiedStore.writeBatch([reading(row({ deviceId: "dev-dropped" }))]).then(() => undefined, (error: unknown) => error);
    await blockedBackend();
    sockets.forEach((socket) => socket.destroy());
    const error = await outcome;
    await holder.query("ROLLBACK");
    await holder.end();
    await proxied.end().catch(() => undefined);
    await new Promise((resolve) => proxy.close(resolve));

    expect((error as Error).message).toMatch(/Connection terminated unexpectedly/);
    expect(classifyDatabaseError(error)).toBe("retryable");
  });

  it("transaction_timeout ends a batch transaction that runs too long as retryable 25P04, and the pool keeps working (S7-11)", async () => {
    const bounded = openIngestPool({ url: container.getConnectionUri(), size: 1, transactionTimeoutMs: 400 }, () => undefined);
    const boundedStore = createPostgresStore(bounded);
    const holder = await lockDevices();
    const error = await boundedStore.writeBatch([reading(row({ deviceId: "dev-txn" }))]).then(() => undefined, (e: unknown) => e);
    await holder.query("ROLLBACK");
    await holder.end();
    const after = await boundedStore.writeBatch([reading(row({ deviceId: "dev-txn" }))]);
    await bounded.end();

    expect((error as { code?: string }).code).toBe("25P04");
    expect(classifyDatabaseError(error)).toBe("retryable");
    expect(after.map((o) => o.kind)).toEqual(["inserted"]);
  });

  it("statement_timeout cancels a batch stuck behind a lock as retryable 57014, and the pool keeps working", async () => {
    const timed = openIngestPool({ url: container.getConnectionUri(), size: 1, statementTimeoutMs: 300 }, () => undefined);
    const timedStore = createPostgresStore(timed);
    const holder = await lockDevices();
    const error = await timedStore.writeBatch([reading(row({ deviceId: "dev-slow" }))]).then(() => undefined, (e: unknown) => e);
    await holder.query("ROLLBACK");
    await holder.end();
    const after = await timedStore.writeBatch([reading(row({ deviceId: "dev-slow" }))]);
    await timed.end();

    expect((error as { code?: string }).code).toBe("57014");
    expect(classifyDatabaseError(error)).toBe("retryable");
    expect(after.map((o) => o.kind)).toEqual(["inserted"]);
  });
});
