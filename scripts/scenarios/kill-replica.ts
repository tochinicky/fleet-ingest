/**
 * Scenario 7: `kill -9` ingest-1's Node process mid-batch under steady load (500 gateways, ~400 msg/s).
 * `restart: always` brings it back with the same client ID, and it resumes its session. The shared subscription
 * doesn't hand its share to ingest-2 (D2): that share waits in ingest-1's session until it returns.
 *
 * PASS means:
 * - the broker dropped nothing;
 * - every reading the broker acknowledged is stored exactly once;
 * - redelivery happened: ingest-1 held unacknowledged messages when it was killed (its unacked gauge), and still nothing
 *   was lost, so the broker redelivered them. The DUP flag on those copies is recorded, not judged: it's advisory, and
 *   correctness rests on the key and ON CONFLICT, never on the flag (S7-7). Redelivered copies of uncommitted messages
 *   are plain inserts, so ON CONFLICT hits can't show redelivery either;
 * - ingest-2 kept storing its own share throughout;
 * - ingest lag recovered: readings measured after ingest-1 is back must be prompt. Lag is split by when a reading was
 *   measured (S7-7): the backlog measured before the kill is expected to be late. This used to also require lag to
 *   *rise* above 1 s, which a fast machine disproved: on the lab VM ingest-1 was back so quickly that lag peaked at
 *   0.9 s (S7-12). That the kill had an effect is what the restart and redelivery checks already prove; the downtime
 *   and the peak lag are recorded with this check.
 *
 * The last two are judged only after a quiet baseline. Before the kill, the scenario first waits (up to 5 min) for the
 * API to report its gap window warm, which is setup, not judgement (S7-11), then waits up to 3 min for 30 s of lag
 * p95 under 2 s. Without one, both are INVALID rather than FAIL, because the saturated VM stalls the whole fleet,
 * not one replica (S6-6, S7-7), and the measured baseline is recorded. A stall *during* the judged window makes them
 * INVALID only by the stall rule (stallRule.ts: CPU pressure, ingest-2 and the broker must all show it). The data
 * checks (dropped, stored, redelivered without loss) are always judged.
 *
 * Usage: `node scripts/scenarios/kill-replica.ts`
 */
import {
  brokerKeepaliveTimeouts, crashNode, ensureFleet, finish, gatedBy, ingestSummaries, lagByMeasured, lagSeconds,
  openDatabase, progress, PROMPT_LAG_P95_SECONDS, promQuery, readSys, restartCount, run, samplePsi, sleep,
  SYS_DROPPED, waitForApiWarm, waitForLedgerStored, waitForQuietBaseline,
} from "./lib.ts";
import { gatedByStall } from "./stallRule.ts";
import type { StallEvidence } from "./stallRule.ts";

const GATEWAYS = 500;

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  progress(started, `starting ${GATEWAYS} gateways`);
  await ensureFleet(pool, GATEWAYS);
  await sleep(20_000);
  const api = await waitForApiWarm();
  progress(started, api.warm ? `the API's gap window is warm after ${api.waitedSeconds} s` : `the API's gap window still isn't warm after ${api.waitedSeconds} s; going on`);
  progress(started, "waiting for a quiet baseline (30 s of lag p95 < 2 s)");
  const baseline = await waitForQuietBaseline(pool);
  progress(started, baseline.quiet ? `quiet after ${baseline.waitedSeconds} s (p95 ${baseline.p95.toFixed(2)} s)` : `no quiet baseline in ${baseline.waitedSeconds} s (p95 ${baseline.p95.toFixed(2)} s): recovery checks will be INVALID`);

  const before = await readSys([SYS_DROPPED]);
  const restartsBefore = restartCount("ingest-1");
  const unackedAtKill = await promQuery('max_over_time(ingest_unacked_messages{instance="ingest-1:9464"}[30s])');
  const killedAt = new Date().toISOString();
  const psi = samplePsi();
  progress(started, "SIGKILL ingest-1's node process");
  crashNode("ingest-1");
  const deadline = Date.now() + 60_000;
  while (restartCount("ingest-1") === restartsBefore && Date.now() < deadline) await sleep(500);
  progress(started, `ingest-1 restarted (restart count ${restartCount("ingest-1")})`);
  const backAt = new Date().toISOString();
  await sleep(40_000);

  const ledger = await waitForLedgerStored(pool, 60_000);
  const after = await readSys([SYS_DROPPED]);
  const windowEnd = new Date().toISOString();
  const recoveredFrom = new Date(Date.now() - 15_000).toISOString();
  const lagDuring = await lagSeconds(pool, killedAt, windowEnd);
  const lagAfter = await lagSeconds(pool, recoveredFrom, windowEnd);
  const backlogLag = await lagByMeasured(pool, "measured_at < $1 AND received_at > $1", [killedAt]);
  const freshLag = await lagByMeasured(pool, "measured_at > $1 AND measured_at < $2", [new Date(Date.parse(backAt) + 10_000).toISOString(), windowEnd]);
  const downSeconds = (Date.parse(backAt) - Date.parse(killedAt)) / 1000;
  const one = ingestSummaries("ingest-1", killedAt);
  const two = ingestSummaries("ingest-2", killedAt);
  const redelivered = one.reduce((s, x) => s + x.redelivered, 0);
  const duplicates = one.reduce((s, x) => s + x.duplicate, 0) + two.reduce((s, x) => s + x.duplicate, 0);
  const twoKeptGoing = two.length >= 3 && two.every((x) => x.inserted > 0);
  const droppedDelta = (after.get(SYS_DROPPED) ?? 0) - (before.get(SYS_DROPPED) ?? 0);
  const stall: StallEvidence = { psiAvg10Peak: psi.stop(), brokerTimeouts: brokerKeepaliveTimeouts(killedAt), untouched: { name: "ingest-2", samples: two.map((x) => x.inserted) } };
  await pool.end();

  finish("scenario 7: kill -9 an ingest replica mid-batch", [
    { check: "ingest-1 crashed and was restarted by Docker", pass: restartCount("ingest-1") > restartsBefore, detail: `restart count ${restartsBefore} → ${restartCount("ingest-1")}` },
    { check: "no message dropped by the broker", pass: droppedDelta === 0, detail: `$SYS dropped +${droppedDelta}` },
    { check: "every acknowledged reading stored exactly once", pass: ledger.stored === ledger.expected, detail: `${ledger.stored} of ${ledger.expected}` },
    {
      check: "redelivery happened: ingest-1 held unacked messages at the kill, and none was lost",
      pass: unackedAtKill > 0 && ledger.stored === ledger.expected,
      detail: `${unackedAtKill} unacked at the kill; ${redelivered} came back with DUP, ${duplicates} duplicates absorbed`,
      // A kill that landed with nothing in flight proves nothing about redelivery: not a pass, not a failure (S7-12).
      ...(unackedAtKill === 0 && ledger.stored === ledger.expected ? { invalid: "nothing was in flight at the kill, so redelivery couldn't be exercised" } : {}),
    },
    gatedBy(baseline, gatedByStall(stall, { check: "ingest-2 kept storing its own share throughout", pass: twoKeptGoing, detail: `inserted per 10 s: ${two.map((x) => x.inserted).join(", ")}` })),
    gatedBy(baseline, gatedByStall(stall, {
      check: "lag recovered: readings measured after ingest-1 was back are prompt",
      pass: freshLag.p95 < PROMPT_LAG_P95_SECONDS,
      detail: `p95 measured after +10 s ${freshLag.p95.toFixed(2)} s; down ${downSeconds.toFixed(1)} s, max lag during ${lagDuring.max.toFixed(1)} s (backlog ${backlogLag.p95.toFixed(2)} s, last 15 s received ${lagAfter.p95.toFixed(2)} s)`,
    })),
  ], { api, baseline, stall, downSeconds, unackedAtKill, redelivered, duplicates, droppedDelta, ledger, lagDuring, lagAfter, backlogLag, freshLag, killedAt, backAt });
}

await run(main);
