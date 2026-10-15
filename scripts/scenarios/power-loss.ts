/**
 * Scenario 8: a gateway loses power. It reboots into the next epoch, and its seq counters roll back by 5,
 * so the new readings reuse seq values the old epoch already stored.
 *
 * PASS means:
 * - the new epoch's readings are stored, not discarded as duplicates (the key includes the epoch);
 * - including the ones whose seq repeats an old-epoch seq;
 * - device_state moves to the new epoch;
 * - the API reports no gap for those devices over the window, because a rollback across epochs is not a gap.
 *
 * Usage: `node scripts/scenarios/power-loss.ts`. It brings the stack up itself if it isn't (S7-11), with
 * `GATEWAY_COUNT` gateways (500 by default, as in the demo).
 */
import { apiGet, ensureFleet, finish, getJson, openDatabase, progress, run, sleep } from "./lib.ts";

const GATEWAY = "sim-gw-0001";
const ROLLBACK = 5;

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  await ensureFleet(pool, Number(process.env.GATEWAY_COUNT ?? 500));
  const windowStart = new Date(Date.now() - 120_000);
  const before = await pool.query<{ device_id: string; epoch: string; max_seq: number }>(
    `SELECT r.device_id, r.epoch::text, max(r.seq)::int AS max_seq FROM readings r JOIN devices d USING (device_id)
     WHERE d.gateway_id = $1 AND r.epoch = (SELECT max(epoch) FROM readings r2 JOIN devices d2 USING (device_id) WHERE d2.gateway_id = $1)
     GROUP BY r.device_id, r.epoch`, [GATEWAY]);
  if (before.rows.length === 0) throw new Error(`${GATEWAY} has no readings yet; start the stack first`);
  const oldEpoch = Number(before.rows[0]?.epoch);

  progress(started, `power loss on ${GATEWAY} (epoch ${oldEpoch}, rollback ${ROLLBACK})`);
  const { epoch: newEpoch } = await getJson<{ epoch: number }>(`http://127.0.0.1:8090/chaos/power-loss?gateway=${GATEWAY}&rollback=${ROLLBACK}`, { method: "POST" });
  await sleep(30_000);

  const after = await pool.query<{ device_id: string; stored: number; overlapping: number; state_epoch: string }>(
    `SELECT d.device_id,
            (SELECT count(*) FROM readings r WHERE r.device_id = d.device_id AND r.epoch = $2)::int AS stored,
            (SELECT count(*) FROM readings r WHERE r.device_id = d.device_id AND r.epoch = $2
               AND r.seq <= (SELECT max(seq) FROM readings o WHERE o.device_id = d.device_id AND o.epoch = $3))::int AS overlapping,
            (SELECT epoch::text FROM device_state s WHERE s.device_id = d.device_id) AS state_epoch
     FROM devices d WHERE d.gateway_id = $1 ORDER BY d.device_id`, [GATEWAY, newEpoch, oldEpoch]);
  const gapCounts = await Promise.all(after.rows.map(async (row) => {
    const report = await apiGet<{ gaps: unknown[] }>(`/devices/${row.device_id}/gaps?from=${windowStart.toISOString()}&to=${new Date().toISOString()}`);
    return report.gaps.length;
  }));
  await pool.end();

  const devices = after.rows;
  finish(`scenario 8: power loss on ${GATEWAY}`, [
    { check: "new-epoch readings stored for every device", pass: devices.every((d) => d.stored > 0), detail: devices.map((d) => `${d.device_id}: ${d.stored}`).join(", ") },
    { check: "readings whose seq repeats an old-epoch seq were stored, not dropped as duplicates", pass: devices.every((d) => d.overlapping > 0), detail: devices.map((d) => `${d.device_id}: ${d.overlapping}`).join(", ") },
    { check: "device_state moved to the new epoch", pass: devices.every((d) => Number(d.state_epoch) === newEpoch), detail: `epoch ${oldEpoch} → ${newEpoch}` },
    { check: "no gap reported across the power loss", pass: gapCounts.every((n) => n === 0), detail: `gaps per device: ${gapCounts.join(", ")}` },
  ], { gateway: GATEWAY, oldEpoch, newEpoch, rollback: ROLLBACK });
}

await run(main);
