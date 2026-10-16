/**
 * Scenario 10: the health worker's leader is SIGKILLed while a standby waits (decisions.md S8-3).
 *
 * The leader's container is killed with `docker kill`, which Docker treats as a manual stop: it isn't restarted, so the
 * standby must take over. (A crash of the Node process alone restarts the container in under a second, and the
 * restarted leader then usually wins the free lock back before the standby's next try; that's correct, and not a
 * failover. The first run of this scenario found that, S8-3.) Then the killed worker is started again.
 *
 * PASS means:
 * - with two workers running, exactly one is leader before the kill, and the lock is never held by two sessions after
 *   it (two would double every fleet counter); Prometheus may show the dead worker's last sample for one scrape
 *   interval, and must show one leader again within 30 s;
 * - the standby takes the lock within 15 s of the kill: its 5 s retry plus margin, since a killed process's
 *   connection is closed by the kernel and Postgres releases the lock at once;
 * - `/fleet`'s open gaps advance again, written by the new leader, within 180 s: the new leader warms its window from
 *   the database first (about a minute at 500 gateways), and until then `/fleet` shows the old snapshot with its age;
 * - started again, the killed worker comes back on standby, not as a second leader.
 *
 * Usage: `node scripts/scenarios/worker-takeover.ts`. It scales `health-worker` to 2 and back to 1.
 */
import { spawnSync } from "node:child_process";
import type pg from "pg";
import { HEALTH_WORKER_LOCK_KEY } from "../../packages/api/src/workerLock.ts";
import { compose, ensureFleet, finish, openDatabase, progress, promQuery, run, sleep, waitForGapsWarm } from "./lib.ts";

const GATEWAYS = 500;
const LOCK_TAKEOVER_BUDGET_MS = 15_000;
const SNAPSHOT_BUDGET_MS = 180_000;
/**
 * A dead worker's last `leader = 1` sample stays current in Prometheus until a scrape of it fails (one 10 s interval,
 * then DNS discovery drops it), so for that long the sum can read 2 with only one live leader. Two intervals of margin.
 */
const STALE_SAMPLE_BUDGET_MS = 30_000;

/**
 * The leader gauge of workers scraped in the last 25 s (2.5 intervals). Prometheus keeps its data across `compose
 * down`, and after its own restart a previous run's worker series stay current for the 5-min lookback; without the
 * filter they count as live workers (found by this scenario's second run, S8-3).
 */
const FRESH_LEADER = "(fleet_health_worker_leader and (time() - timestamp(fleet_health_worker_leader)) < 25)";

/** Sessions holding the health worker's advisory lock now: the ground truth for "one leader". */
async function lockHolders(pool: pg.Pool): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(
    "SELECT count(*)::int AS n FROM pg_locks WHERE locktype = 'advisory' AND classid = 0 AND objid = $1 AND objsubid = 1 AND granted", [HEALTH_WORKER_LOCK_KEY]);
  return rows[0]?.n ?? 0;
}

/** Runs `docker` with `args` and returns its stdout. */
function docker(args: string[]): string {
  const result = spawnSync("docker", args, { encoding: "utf8" });
  if (result.status !== 0) throw new Error(`docker ${args.join(" ")}: ${result.stderr.trim()}`);
  return result.stdout;
}

/** The health-worker containers, by short ID (which is also each one's hostname, its WORKER_ID). */
function workers(): string[] {
  return compose(["ps", "-q", "health-worker"]).trim().split("\n").filter(Boolean).map((id) => id.slice(0, 12));
}

/** When `container` last logged `message` (pino's `time`, epoch ms), or null. */
function loggedAt(container: string, message: string): number | null {
  const lines = docker(["logs", container]).split("\n").filter((line) => line.includes(message));
  const last = lines.at(-1);
  if (!last) return null;
  try {
    return (JSON.parse(last) as { time: number }).time;
  } catch {
    return null;
  }
}

/** Kills the leading worker and judges the takeover. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  const warm = await waitForGapsWarm();
  progress(started, `gap window warm: ${warm.warm} after ${warm.waitedSeconds} s`);
  compose(["up", "-d", "--no-recreate", "--scale", "health-worker=2", "health-worker"]);
  await sleep(30_000);
  const ids = workers();
  const leaders = ids.filter((id) => loggedAt(id, "holding the health worker lock") !== null);
  const leadersBefore = await promQuery(`sum(${FRESH_LEADER})`);
  const targetsBefore = await promQuery(`count(${FRESH_LEADER})`);
  if (leaders.length !== 1) throw new Error(`expected one leader among ${ids.join(", ")}, found ${leaders.length}`);
  const leader = leaders[0] as string;
  const standby = ids.find((id) => id !== leader) as string;
  const before = (await pool.query<{ writer: string; written_at: Date }>("SELECT writer, written_at FROM health_snapshot")).rows[0];
  progress(started, `leader ${leader}, standby ${standby}; killing the leader`);
  const killedAt = Date.now();
  // A manual stop on purpose: Docker doesn't restart it, so only the standby can take the lock.
  docker(["kill", "-s", "KILL", leader]);

  let maxLeaders = 0;
  let maxLockHolders = 0;
  let lastDoubleAt: number | null = null;
  let snapshotAt: number | null = null;
  let snapshotWriter: string | null = null;
  while (Date.now() - killedAt < SNAPSHOT_BUDGET_MS + 30_000 && snapshotAt === null) {
    const leadersNow = (await promQuery(`sum(${FRESH_LEADER})`)) || 0;
    maxLeaders = Math.max(maxLeaders, leadersNow);
    if (leadersNow > 1) lastDoubleAt = Date.now();
    maxLockHolders = Math.max(maxLockHolders, await lockHolders(pool));
    const row = (await pool.query<{ writer: string; written_at: Date }>("SELECT writer, written_at FROM health_snapshot")).rows[0];
    if (row && row.writer !== leader && row.written_at.getTime() > killedAt) {
      snapshotAt = Date.now();
      snapshotWriter = row.writer;
    }
    await sleep(2000);
  }
  const takeoverAt = loggedAt(standby, "holding the health worker lock");
  progress(started, "starting the killed worker again");
  const restartedAt = Date.now();
  docker(["start", leader]);
  await sleep(30_000);
  const standbyAt = loggedAt(leader, "on standby");
  const killedBackOnStandby = standbyAt !== null && standbyAt > restartedAt ? standbyAt : null;
  maxLockHolders = Math.max(maxLockHolders, await lockHolders(pool));
  const leadersAfter = await promQuery(`sum(${FRESH_LEADER})`);
  compose(["up", "-d", "--no-recreate", "--scale", "health-worker=1", "health-worker"]);
  await pool.end();

  const lockMs = takeoverAt === null ? null : takeoverAt - killedAt;
  const snapshotMs = snapshotAt === null ? null : snapshotAt - killedAt;
  finish("health worker takeover", [
    { check: "two workers, one leader before the kill", pass: leadersBefore === 1 && targetsBefore === 2, detail: `${leadersBefore} leader(s) of ${targetsBefore} worker(s)` },
    { check: "never two sessions holding the lock (sampled every 2 s)", pass: maxLockHolders <= 1, detail: `at most ${maxLockHolders}` },
    {
      check: `Prometheus shows exactly one leader again within ${STALE_SAMPLE_BUDGET_MS / 1000} s of the kill`,
      pass: leadersAfter === 1 && (lastDoubleAt === null || lastDoubleAt - killedAt <= STALE_SAMPLE_BUDGET_MS),
      detail: `${leadersAfter} at the end; the dead worker's last sample summed to ${maxLeaders}${lastDoubleAt === null ? "" : ` until +${((lastDoubleAt - killedAt) / 1000).toFixed(0)} s`}`,
    },
    {
      check: `the standby takes the lock within ${LOCK_TAKEOVER_BUDGET_MS / 1000} s`,
      pass: lockMs !== null && lockMs <= LOCK_TAKEOVER_BUDGET_MS,
      detail: lockMs === null ? "it never logged taking the lock" : `${(lockMs / 1000).toFixed(1)} s`,
    },
    {
      check: `/fleet's open gaps advance again within ${SNAPSHOT_BUDGET_MS / 1000} s, written by the new leader`,
      pass: snapshotMs !== null && snapshotMs <= SNAPSHOT_BUDGET_MS && snapshotWriter === standby,
      detail: snapshotMs === null ? "no new snapshot" : `${(snapshotMs / 1000).toFixed(0)} s, by ${snapshotWriter}`,
    },
    { check: "started again, the killed worker comes back on standby", pass: killedBackOnStandby !== null, detail: killedBackOnStandby === null ? "no standby line in its log since it was started" : "on standby" },
  ], { maxLeadersSampled: maxLeaders, leader, standby, snapshotBefore: before, lockTakeoverSeconds: lockMs === null ? null : lockMs / 1000, snapshotSeconds: snapshotMs === null ? null : snapshotMs / 1000, warmUp: warm });
}

await run(main);
