/**
 * The gap checkpoint against a real Postgres (S8-4). For generated histories, stored as real rows in the partitioned
 * `readings`:
 * - the SQL gap source returns exactly the model's gaps, for all history and from any T;
 * - every checkpointed pass, through the real `gap_checkpoints` table, equals explaining all stored history from
 *   scratch (the same oracle as the unit test), and equals the reference `gapsInWindow` that `/devices/:id/gaps` uses.
 * `stored_at` follows the test's own storage clock: a second per reading, and 100 s before each pass, beyond the
 * cursor overlap the SQL re-checks. With the database's clock instead, a whole history is stored within the overlap,
 * every pass re-checks every row and recomputes, and the invalidation rules are never what decides: the break checks
 * of the two SQL rules went unnoticed until this clock was added (S8-4).
 */
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import fc from "fast-check";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { applyMigrations, ensurePartitions, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";
import { compareGaps, explainGaps } from "../src/core/health.ts";
import type { Gap } from "../src/core/health.ts";
import { checkpointedGapsInWindow, sqlGapSource } from "../src/gapCheckpointStore.ts";
import { gapsInWindow } from "../src/healthReport.ts";
import { FIXED_HISTORIES, MINUTE, op, seriesGaps, WINDOW } from "./support/gapModel.ts";
import type { ModelRow, Op } from "./support/gapModel.ts";

const GATEWAY = "gw-cp";
const FAR_FUTURE = new Date(8.64e15);
let container: StartedPostgreSqlContainer;
let pool: pg.Pool;

const sorted = (gaps: Gap[]) => [...gaps].sort(compareGaps).map((g) => ({ ...g, before: new Date(g.before) }));

beforeAll(async () => {
  container = await new PostgreSqlContainer("postgres:17.11-alpine").start();
  pool = new pg.Pool({ connectionString: container.getConnectionUri(), max: 4 });
  const client = await pool.connect();
  await applyMigrations(client, loadMigrations(MIGRATIONS_DIR));
  // Generated histories start at the real clock and run up to a few days ahead of it.
  await ensurePartitions(client, new Date(Date.now() - 2 * 86_400_000), new Date(Date.now() + 10 * 86_400_000));
  client.release();
  await pool.query("INSERT INTO gateways (gateway_id, first_seen) VALUES ($1, now())", [GATEWAY]);
  await pool.query("INSERT INTO devices (device_id, gateway_id, kind, first_seen) VALUES ('d0', $1, 'Battery', now()), ('d1', $1, 'Battery', now())", [GATEWAY]);
});

afterAll(async () => {
  await pool?.end();
  await container?.stop();
});

/** Plays `ops` into the database and the model side by side, checking every pass. */
async function replay(ops: Op[]): Promise<void> {
  await pool.query("DELETE FROM readings");
  await pool.query("DELETE FROM gap_checkpoints");
  const rows: ModelRow[] = [];
  let now = Math.floor(Date.now() / MINUTE) * MINUTE;
  let storageClock = Date.now() - 86_400_000;
  let dropped = 0;
  let retainedFrom: number | null = null;
  for (const o of ops) {
    if (o.kind === "store" || o.kind === "next") {
      const deviceId = `d${o.device % 2}`;
      const top = Math.max(0, ...rows.filter((r) => r.deviceId === deviceId && r.epoch === o.epoch).map((r) => r.seq));
      const seq = o.kind === "store" ? o.seq : o.back > 0 ? Math.max(1, top - o.back) : top + 1 + o.skip;
      const row = { deviceId, epoch: o.epoch, seq, measuredAt: now - o.ageMinutes * MINUTE, storedAt: rows.length };
      if (retainedFrom !== null && row.measuredAt < retainedFrom) continue;
      const { rowCount } = await pool.query(
        `INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind, stored_at) VALUES ($1, $2, $3, $4, now(), 'Battery', $5)
         ON CONFLICT (device_id, epoch, seq, measured_at) DO NOTHING`, [deviceId, row.epoch, seq, new Date(row.measuredAt), new Date(storageClock += 1000)]);
      if (rowCount === 1) rows.push(row);
    } else if (o.kind === "drop") {
      dropped += o.more;
    } else if (o.kind === "retention") {
      const cut = now - o.keepMinutes * MINUTE;
      if (retainedFrom === null || cut > retainedFrom) {
        retainedFrom = cut;
        await pool.query("DELETE FROM readings WHERE measured_at < $1", [new Date(cut)]);
        for (let i = rows.length - 1; i >= 0; i--) if ((rows[i] as ModelRow).measuredAt < cut) rows.splice(i, 1);
      }
    } else {
      now += o.advanceMinutes * MINUTE;
      const windowFrom = new Date(now - WINDOW);
      const source = sqlGapSource(pool, GATEWAY);
      expect(sorted(await source.allGaps())).toEqual(sorted(seriesGaps(rows)));
      expect(sorted(await source.gapsFrom(windowFrom))).toEqual(sorted(seriesGaps(rows).filter((g) => g.before >= windowFrom)));
      const pass = { passStartedAt: new Date(storageClock += 100_000), retainedFrom: retainedFrom === null ? null : new Date(retainedFrom) };
      const { gaps } = await checkpointedGapsInWindow(pool, GATEWAY, dropped, windowFrom, FAR_FUTURE, pass);
      expect(gaps).toEqual(explainGaps(seriesGaps(rows), dropped).filter((g) => g.before >= windowFrom));
      // The reference reads readings measured up to its `now`, so ten minutes ahead covers those from the future
      // (2 min at most here); its window ends at the pass's now, so the comparison does too. With no drops it reads
      // only readings measured in the window, so after a clock stepped back it misses a pair whose second reading is
      // older (S8-4 records this); with drops it reads all history, and must agree exactly.
      if (dropped === 0) continue;
      const reference = await gapsInWindow(pool, GATEWAY, dropped, windowFrom, new Date(now), new Date(now + 10 * MINUTE));
      expect(gaps.filter((g) => g.before <= new Date(now))).toEqual(reference.map((g) => {
        const { gatewayId: _gateway, ...gap } = g as typeof g & { gatewayId?: string };
        return gap;
      }));
    }
  }
}

describe("the gap checkpoint on Postgres (S8-4)", () => {
  it.each(Object.entries(FIXED_HISTORIES))("equals the oracle on the fixed history %s", async (_name, history) => {
    await replay(history);
  });

  it("SQL gaps equal the model's, and every checkpointed pass equals explaining all stored history", async () => {
    await fc.assert(fc.asyncProperty(fc.array(op, { maxLength: 50 }), replay), { numRuns: 40 });
  }, 300_000);
});
