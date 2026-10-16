/**
 * The core property test: any stream of valid readings, duplicated and shuffled, leaves exactly the same
 * `readings` and `device_state` as the clean in-order stream. That's "exactly-once in Postgres" for any delivery
 * order and any way of cutting it into batches. It runs against a real Postgres, through the same batch store ingest
 * uses.
 *
 * The generated streams include what the wire really does:
 * - several devices;
 * - power losses (a new epoch with the seq rolled back);
 * - readings that share a timestamp;
 * - duplicates, and late backfills behind live data;
 * - batches of any size, including a copy of a reading in the same batch as the original;
 * - a stream that crosses midnight UTC, so it spans two daily partitions of `readings` (S8-2).
 */
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import fc from "fast-check";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createPostgresStore } from "../src/adapters/postgresStore.ts";
import type { IngestStore } from "../src/adapters/postgresStore.ts";
import type { ReadingRow } from "../src/core/rows.ts";
import { startMigratedPostgres } from "./support/containers.ts";

let container: StartedPostgreSqlContainer;
let pool: pg.Pool;
let store: IngestStore;

beforeAll(async () => {
  container = await startMigratedPostgres();
  pool = new pg.Pool({ connectionString: container.getConnectionUri(), max: 2 });
  store = createPostgresStore(pool);
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

/**
 * 15 s before midnight UTC: every generated stream crosses into the next day's partition (S8-2), so duplicates,
 * reorderings and backfills are exercised on both sides of a partition boundary.
 */
const START = Date.UTC(2026, 9, 14, 23, 59, 45);

/**
 * One device's history as the gateway produced it: a list of boots, each with how many readings it sent and how far
 * the power loss before it rolled the seq counter back (never within an epoch). Readings are 5 s apart, and every
 * `tieEvery`-th one repeats the previous timestamp.
 */
interface DeviceHistory {
  boots: Array<{ readings: number; rollback: number }>;
  tieEvery: number;
}

/** Arbitrary {@link DeviceHistory}: 1–3 boots of 1–6 readings, rolling seq back by up to 3 on each power loss. */
const deviceHistory: fc.Arbitrary<DeviceHistory> = fc.record({
  boots: fc.array(fc.record({ readings: fc.integer({ min: 1, max: 6 }), rollback: fc.integer({ min: 0, max: 3 }) }), { minLength: 1, maxLength: 3 }),
  tieEvery: fc.integer({ min: 2, max: 7 }),
});

/** Turns a device's history into its readings, in the order the gateway produced them. */
function readingsFor(deviceIndex: number, history: DeviceHistory): ReadingRow[] {
  const rows: ReadingRow[] = [];
  let seq = 1;
  let at = START;
  history.boots.forEach((boot, bootIndex) => {
    seq = Math.max(1, seq - boot.rollback);
    for (let i = 0; i < boot.readings; i++) {
      if (rows.length % history.tieEvery !== 0 || rows.length === 0) at += 5000;
      rows.push({
        gatewayId: "gw-prop", deviceId: `dev-${deviceIndex}`, epoch: bootIndex + 1, seq: seq++, kind: "Battery",
        measuredAt: new Date(at), receivedAt: new Date(at + 100),
        pvPowerW: null, batteryPowerW: deviceIndex * 1000 + rows.length, gridPowerW: null, evPowerW: null, energyWh: null,
        socPercent: 50, chargeLimitA: null,
      });
    }
  });
  return rows;
}

/** A clean stream for 1–3 devices, plus a delivery of it: every reading at least once, extra copies, any order. */
const streamAndDelivery = fc
  .array(deviceHistory, { minLength: 1, maxLength: 3 })
  .map((histories) => histories.flatMap((history, i) => readingsFor(i, history)))
  .chain((clean) =>
    fc
      .tuple(fc.array(fc.nat({ max: clean.length - 1 }), { maxLength: clean.length }), fc.infiniteStream(fc.integer()))
      .map(([extraCopies, randomKeys]) => {
        const keys = randomKeys.take(clean.length + extraCopies.length);
        const delivered = [...clean, ...extraCopies.map((i) => clean[i] as ReadingRow)];
        const order = delivered.map((row, i) => ({ row, key: keys.next().value as number, i }));
        order.sort((a, b) => a.key - b.key || a.i - b.i);
        return { clean, delivered: order.map((o) => o.row) };
      })
      .chain(({ clean, delivered }) =>
        fc.array(fc.integer({ min: 1, max: 12 }), { minLength: delivered.length, maxLength: delivered.length })
          .map((sizes) => ({ clean, batches: cutIntoBatches(delivered, sizes) }))),
  );

/** Cuts `rows` into consecutive batches, taking the next size from `sizes` for each batch. */
function cutIntoBatches(rows: ReadingRow[], sizes: number[]): ReadingRow[][] {
  const batches: ReadingRow[][] = [];
  for (let i = 0, b = 0; i < rows.length; b++) {
    const size = sizes[b] ?? 1;
    batches.push(rows.slice(i, i + size));
    i += size;
  }
  return batches;
}

/** Empties every table the store writes. */
async function reset(): Promise<void> {
  await pool.query("TRUNCATE readings, device_state, devices, gateways, dead_letters");
}

/** The stored result, without `received_at`: a duplicate keeps the first delivery's receive time, which may differ. */
async function snapshot(): Promise<{ readings: unknown[]; state: unknown[] }> {
  const readings = await pool.query(
    "SELECT device_id, epoch::int, seq::int, measured_at, battery_power_w FROM readings ORDER BY device_id, epoch, seq");
  const state = await pool.query("SELECT device_id, epoch::int, seq::int, measured_at, battery_power_w FROM device_state ORDER BY device_id");
  return { readings: readings.rows, state: state.rows };
}

/** Writes each batch in its own transaction, in order. */
async function deliver(batches: ReadingRow[][]): Promise<void> {
  for (const batch of batches) {
    await store.writeBatch(batch.map((row) => ({ routed: { type: "reading" as const, row }, topic: "t", payload: new Uint8Array() })));
  }
}

describe("exactly-once in Postgres", () => {
  it("any duplicated, shuffled, arbitrarily batched delivery leaves the same readings and device_state as the clean in-order stream", async () => {
    await fc.assert(
      fc.asyncProperty(streamAndDelivery, async ({ clean, batches }) => {
        await reset();
        await deliver(clean.map((row) => [row]));
        const expected = await snapshot();
        await reset();
        await deliver(batches);
        expect(await snapshot()).toEqual(expected);
      }),
      { numRuns: 80 },
    );
  });
});
