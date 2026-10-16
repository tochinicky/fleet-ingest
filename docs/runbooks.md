# Runbooks

What an on-call engineer does for each alert or known failure mode. It's a lab, so "on call" is whoever is running the demo. Slice 6 adds an entry for every alert; the entries below come from findings made while building.

---

## Cold start: a new broker, or an expired ingest session

**Alert:** IngestSessionExpired: the broker log says an `ingest-*` session expired, and whatever was queued for it is gone.

**What it means:** the broker queues messages only for sessions that already exist. On a brand-new broker (empty `mosquitto-data` volume), or after an ingest session expired (a replica gone for more than 7 days), readings published before ingest subscribes have no subscriber and are **discarded**. Nothing counts them.

**Symptoms:** the first readings after a deploy are missing. The gap check (slice 5) shows gaps at the start of each device's epoch, and `droppedTelemetry` doesn't explain them.

**First three things to check:**
1. Did the broker's volume just get created? `docker volume inspect fleet-ingest_mosquitto-data --format '{{.CreatedAt}}'`
2. Are both ingest sessions present and subscribed? `docker compose -f deploy/compose.yaml ps ingest-1 ingest-2` should show `healthy`, which means the ready file was written after subscribing.
3. Which devices are missing their first readings?
   ```sql
   SELECT device_id, epoch, min(seq) FROM readings GROUP BY 1, 2 HAVING min(seq) > 1;
   ```

**Also a cold start:** switching broker persistence between snapshots and persist-sqlite. Neither reads the other's files (decisions.md S6-14), so follow "Switch broker persistence" below.

**Prevention:** start ingest before the fleet. Compose does this (`fleet-sim` waits for both replicas to be `healthy`). Real gateways don't wait, so on a fresh broker, deploy ingest and wait for healthy before opening the gateway listener. Never delete the broker volume or rename an ingest client ID casually: either one throws the sessions away.

**Fleet-wide or single home?** Cold start is always fleet-wide. One home with missing first readings is something else, such as a gateway buffer overflow (`droppedTelemetry`).

---

## Gateway shows no status after an ingest cold start

**What it means:** shared subscriptions never receive retained messages (MQTT v5), so ingest doesn't see each gateway's retained `online` when it first subscribes. Live gateways reappear at their next 60 s heartbeat. A gateway that was **already offline** sends no heartbeat, so it stays "no status" until it reconnects.

**What to do:** nothing, for live gateways; wait one status interval. `/fleet` reports `gatewaysWithoutStatus` as its own count, and it is never folded into online or offline.

---

## Ingest connected but receiving nothing (after a broker restart)

**Alert:** IngestStalled: nothing received for 30 s while more than the services are connected to the broker.

**What it means:** ingest is connected but holds no subscription. With persist-sqlite (S6-15) a restart keeps the shared subscriptions, so a restart alone shouldn't cause this. With the snapshot fallback, Mosquitto 2.1.2 restores ingest's sessions **without their shared subscriptions** and still says "session present" (decisions.md S3-9). Either way, ingest subscribes again on every connect, so this should heal within a second. If it doesn't, something is stopping the re-subscribe.

**Symptoms:** readings stop arriving (`device_state.received_at` stops moving), and ingest's `summary` lines show `received: 0`. Yet ingest is connected, the gateways are online, and the broker's drop counter doesn't move. The readings go to no subscriber at all.

**First three things to check:**
1. Does the broker have any subscriptions? Read `$SYS/broker/subscriptions/count` with the `ops` certificate (see "Reading broker statistics" below). Your own `mosquitto_sub` counts as one subscription.
2. Did ingest log a re-subscribe failure? `docker compose -f deploy/compose.yaml logs ingest-1 ingest-2 | grep -i subscrib`
3. Restart the ingest replicas (`docker compose -f deploy/compose.yaml restart ingest-1 ingest-2`). They subscribe on connect.

**Fleet-wide or single home?** Always fleet-wide. Every gateway's readings stop at once.

---

## Switch broker persistence (snapshots ↔ persist-sqlite)

**When:** adopting persist-sqlite (decisions.md S6-15), or falling back to snapshots. Neither back end reads the other's files. The upstream migration script loses queued messages (#3763), so this is a fresh install of the broker's state.

1. **Empty the queue first.** Stop the fleet, or wait for a quiet moment, until both ingest replicas show `ingest_unacked_messages` 0 and `broker_store_messages` (Grafana, or `$SYS/broker/store/messages/count`) stops falling. Anything still queued is lost by the switch.
   - **Retained messages are lost too.** They are what's left in that count. Gateway statuses come back on their own (each gateway publishes one on connect). **Commands don't:** the desired mode lives only as a retained message (S5-4), and by design no certificate can read every gateway's command topic (D6 ACL: the API may only write them). Re-send the modes you need with `PUT /gateways/:id/mode` after the switch. A gateway that was offline with an unapplied command keeps its old mode until then.
2. Edit `deploy/mosquitto/mosquitto.conf` (and `mosquitto.small-queue.conf`): the persistence lines and the commented fallback block are next to each other.
3. Start the broker on the new back end. The old files can stay on the volume (`mosquitto.db` or `mosquitto.sqlite3`); the other back end ignores them.
   ```sh
   docker compose -f deploy/compose.yaml up -d --force-recreate mosquitto
   ```
4. **This is a cold start:** no sessions exist. Restart the ingest replicas and wait for `healthy` **before** the fleet sends (see "Cold start" above).
   ```sh
   docker compose -f deploy/compose.yaml restart ingest-1 ingest-2
   ```
5. Verify: the broker log shows `sqlite: Restored 0 clients` (persist-sqlite) on this first start, and the right number after the next restart; readings flow.

---

## Remove an ingest replica (end its session)

**When:** scaling down for good. A stopped replica keeps its session, and with it a round-robin share of every reading, queued until it comes back or for 7 days. Its session has to end.

1. Stop the replica: `docker compose -f deploy/compose.yaml stop ingest-2`. Its unacknowledged messages stay queued for its session.
2. Drain that session: start it again briefly with nothing else changed, wait for its `ingest_unacked_messages` to reach 0, then end the session by connecting once with its client ID, clean start and session expiry 0.
3. **With persist-sqlite, wait at least 5 s (one flush period) before any broker restart or crash.** A crash inside that window brings the ended session back with its shared subscription (#3761, reproduced in decisions.md S6-14): a ghost group member that takes a share of readings and never reads them.
4. Verify: the broker's subscription count dropped by one, and the remaining replica's received rate doubled. If BrokerBacklogHigh fires afterwards with one replica healthy, suspect a ghost: repeat step 2's clean-start connect.

---

## Revoke a gateway (stolen or retired certificate)

**What it means:** a gateway's private key may be in someone else's hands, or the gateway is decommissioned, and it must stop being able to publish.

**Steps:**
1. Revoke and regenerate the CRL: `scripts/pki.sh revoke deploy/certs <gateway-id>`
2. Reload it: `docker compose -f deploy/compose.yaml kill -s HUP mosquitto`. **New** connections with that certificate now fail the TLS handshake.
3. **Drop the gateway's open connection.** Mosquitto checks the CRL only at the handshake, so an open connection keeps working (decisions.md S4-3). Mosquitto 2.1.2 has no per-client disconnect without changing the auth backend, so restart the broker. Note the time first:
   ```sh
   date -u +%FT%TZ          # T_restart
   docker compose -f deploy/compose.yaml restart mosquitto
   ```
   **What this costs:**
   - **Every gateway reconnects.** Every queued message survives: persist-sqlite commits continuously (S6-15).
   - **Nothing published during the restart is lost:** the ingest sessions keep their shared subscriptions, so readings queue for them until they reconnect (S6-15; with snapshots this was the S3-9 window).
4. Verify the gateway is locked out, and stayed out:
   - A new connection is refused with "certificate revoked":
     ```sh
     mosquitto_sub -h 127.0.0.1 -p 8883 --cafile deploy/certs/ca.crt --cert deploy/certs/<id>.crt --key deploy/certs/<id>.key -t x -C 1 -W 5
     ```
   - The broker log shows its failed handshakes, not a connection: `docker compose -f deploy/compose.yaml logs mosquitto --since 5m | grep -i '<id>'`
   - Nothing from it is stored after the restart:
     ```sql
     SELECT count(*) FROM readings r JOIN devices d USING (device_id) WHERE d.gateway_id = '<id>' AND r.received_at > '<T_restart>';
     ```
     It should be 0.
   - `$SYS/broker/clients/connected` (with the `ops` certificate) returns to the fleet's usual count minus one.

**Fleet-wide or single home?** Single home. If many gateways fail the handshake at once, check the CA and CRL files instead: an expired CRL (`default_crl_days 30`) or a regenerated CA locks everyone out.

---

## Reading broker statistics ($SYS) with mTLS

There's no plain listener, so use the `ops` certificate, which the ACL allows to read `$SYS` only:
```sh
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile deploy/certs/ca.crt --cert deploy/certs/ops.crt --key deploy/certs/ops.key \
  -t '$SYS/broker/publish/messages/dropped' -t '$SYS/broker/store/messages/count' -C 2 -W 15
```

---

## A gateway is offline (alert GatewayOffline)

**What it means:** the gateway's last status is `offline` (usually its Last Will: the broker lost it for 1.5 × its 15 s keepalive), or nothing has been heard from it for 3 minutes. Either way, **and** no reading has arrived from it in the last minute. A reading always counts as proof of life (decisions.md S5-1).

**First three things to check:**
1. What the API says, and why: `curl -s localhost:8080/gateways/<id> -H "Authorization: Bearer $API_TOKEN" | jq '.health, .lastStatusAt, .lastStatusReceivedAt, .lastReadingReceivedAt'`
2. Whether the broker still sees it: the broker log for its ID (`docker compose -f deploy/compose.yaml logs mosquitto --since 10m | grep '<id>'`). "Client <id> has exceeded timeout" means the link died; "disconnected" means it said goodbye.
3. How much it's holding: when it returns, its status carries `bufferedTelemetry` and `droppedTelemetry`. A rising dropped count means its outage outlasted its 50,000-message buffer, and the gaps will show as **explained**.

**If IngestLagHigh is firing too,** offline detection is delayed by up to that lag. A backlogged ingest can process a dead gateway's queued readings after its Last Will (decisions.md S5-8). `/fleet` and `/gateways/<id>` return `healthAsOfLagSeconds`, so check it before trusting a recovery or an outage.

**Fleet-wide or single home?** Compare with `/fleet`.
- **Many gateways at once** (say 10% within a minute) is the broker, the cloud network, or an ISP. Check the broker and toxiproxy first, not 50 homes.
- **One gateway** is that home's internet or power.
- If `unknown` jumps instead of `offline`, ingest restarted and hasn't heard their statuses yet (S1-2).

---

## A device is stale (alert DeviceStale)

**What it means:** the gateway is online, but one of its devices has sent nothing for 60 s. That's a device or LAN problem inside the home, not connectivity. It's a different team from GatewayOffline.

**First three things to check:**
1. Which devices: `curl -s localhost:8080/gateways/<id> -H "Authorization: Bearer $API_TOKEN" | jq '.devices[] | select(.stale)'`
2. When it last reported: `SELECT * FROM device_state WHERE device_id = '<device>';`
3. Whether readings for it are being dead-lettered instead of stored (wrong gateway, bad clock): `SELECT reason, count(*) FROM dead_letters WHERE topic LIKE '%/<device>/%' AND received_at > now() - interval '1 hour' GROUP BY 1;`

---

## Command not applied (alert CommandNotApplied)

**Alert:** CommandNotApplied: at least one online gateway still reports a different mode than its desired mode, 5 minutes after it could have applied it (decisions.md S7-1).

**What it means:** the cloud asked for a mode (`PUT /gateways/:id/mode`), the desired mode is in Postgres, and the reconciler publishes it retained. The gateway is online, but its status still reports the old mode. Either it never got the command, or it got it and refused or failed to apply it.

**Symptoms:** `GET /fleet` lists the gateways under `commands.stuckGateways`. `GET /gateways/<id>` shows `command.pending: true`, with `desiredMode`, `reportedMode` and `pendingSince`.

**First three things to check:**
1. Is the API connected to the broker, and is the reconciler publishing? `curl -s localhost:8080/health/ready` (broker must be `true`), and `docker compose -f deploy/compose.yaml logs api --since 10m | grep -i reconcile` for failures.
2. Does the gateway report its mode at all? Its status carries `mode`. If `reportedMode` is null, it's a gateway that doesn't report modes, not a lost command.
   ```sql
   SELECT gateway_id, mode, desired_mode, desired_at, came_back_at, last_status_received_at FROM gateways WHERE desired_mode IS DISTINCT FROM mode AND desired_mode IS NOT NULL;
   ```
3. Did the gateway receive it? The gateway's own log shows commands it received and applied, or why it ignored one (an invalid command is ignored).

**Fleet-wide or single home?** Many gateways at once points to the cloud side: the API isn't publishing, or the broker lost retained messages and the API hasn't reconnected. One gateway points to that gateway: it refused the mode, or its device can't do it.

---

## Gaps in a device's readings

**What it means:** within one epoch, some seqs never arrived.
- **Explained gaps** are covered by the gateway's `droppedTelemetry`: its buffer overflowed and it dropped the oldest readings. That's accepted loss.
- **Unexplained gaps** were never stored, and nothing says why.

**First three things to check:**
1. `curl -s "localhost:8080/devices/<device>/gaps?from=…&to=…" -H "Authorization: Bearer $API_TOKEN"`. Are they explained?
2. Unexplained gaps around a broker **crash** (SIGKILL, OOM) point to the persist-sqlite flush window: up to 5 s of acknowledged queue (S7-7). A clean restart loses nothing, unless the broker runs on the snapshot fallback, where gaps around a restart point to the S3-9 window. Around a database outage longer than the queue bound, to counted drops (`$SYS/broker/publish/messages/dropped`).
3. Gaps at the start of an epoch on a new broker point to the cold start (see that entry).

A gap that a later replay fills disappears by itself, so recheck after the gateway has drained its backlog.

---

## Ingest lag high (alert IngestLagHigh)

**What it means:** the p95 time from ingest receiving a message to its batch committing has been over 60 s for 5 minutes. Ingest is slow, or it's retrying a failing database. Time spent waiting in the *broker* queue before ingest receives a message isn't in this number. BrokerBacklogHigh covers that (decisions.md S6-1).

**First three things to check:**
1. Retries: is DBWriteErrors firing too? On the dashboard, "Database retries" shows how long the current batch has been failing.
2. Database health and the toxiproxy route: `docker compose -f deploy/compose.yaml exec postgres pg_isready -U fleet` and `curl -s localhost:8474/proxies/postgres` (enabled, any toxics?).
3. Ingest's own load: event-loop lag and RSS on the dashboard, and `waiting` / `unacked` in its summary lines (`docker compose … logs ingest-1 | grep summary | tail -3`).

**While it fires,** GatewayOffline is delayed by up to the lag (S5-8).

---

## Broker backlog high (alert BrokerBacklogHigh)

**What it means:** over 24,000 messages (10% of the 2 × 120,000 bound) have been waiting at the broker for 5 minutes. Ingest isn't keeping up. Its Receive Maximum is full, so the broker queues (D3).

**First three things to check:**
1. Is ingest acknowledging at all? `ingest_unacked_messages` near 1,000 per replica means it's stuck on writes. Look at IngestLagHigh and DBWriteErrors.
2. Are both replicas up and subscribed? `ingest_broker_connected`. A dead replica's share waits in its own session (D2), so the queue grows even though the other replica is healthy.
3. How fast is it growing? `deriv(broker_store_messages[5m])` against the 240k bound tells you how long until BrokerDroppingMessages.

---

## Broker dropping messages (alert BrokerDroppingMessages)

**What it means:** a session's queue hit its bound (`max_queued_messages` 120,000 or `max_queued_bytes`), and the broker is discarding new messages for it. The gateways were acknowledged, so this data is **gone**. It's the counted loss in D3's guarantee.

**First three things to check:**
1. Which client: `sum by (client) (increase(broker_log_events_total{kind="dropping"}[10m]))`. An `ingest-N` means that replica's queue is full (it's down or stalled).
2. How many: `increase(broker_publish_messages_dropped[10m])`. This counter resets when the broker restarts.
3. Fix the cause upstream: the database (IngestLagHigh, DBWriteErrors) or a dead replica (D2). Raising the bound only buys time, and costs broker memory (844 B per message, S0-6).

---

## Database write errors (alerts DBWriteErrors, BatchRetryingTooLong)

**What it means:**
- **DBWriteErrors:** ingest has retried batch writes at least 3 times in 2 minutes, for a minute: a sustained problem, not one blip. It retries with full-jitter backoff up to 30 s (S3-2), and nothing is acknowledged while it retries, so messages wait in the broker. That's safe, but the queue grows.
  - A single retry (one connect timeout while the stack cold-starts, say) no longer fires it (S7-11). It still shows in `ingest_db_retries_total` and in the "batch write failed, retrying" log line.
- **BatchRetryingTooLong:** the same batch has been failing for over 5 minutes.

**First three things to check:**
1. The error: `docker compose -f deploy/compose.yaml logs ingest-1 | grep "retrying" | tail -3` (connection refused? deadlock? too many connections?).
2. Postgres itself: `pg_isready`, disk space (`df` in the container), `SELECT count(*) FROM pg_stat_activity`.
3. The toxiproxy route, in the lab: `curl -s localhost:8474/proxies/postgres`.

**If writes can't recover,** watch BrokerBacklogHigh and the time left before BrokerDroppingMessages.

---

## Dead letters appearing (alert DeadLettersAppearing)

**What it means:** messages are being rejected for a reason (`reason` label): bad JSON or schema, device mismatch, a clock over 5 minutes ahead, a device claimed by another gateway, or a row Postgres refused. The pipeline keeps going (D7). This is informational unless the rate is high.

**First three things to check:**
1. Which reason and which gateway: `SELECT reason, topic, count(*) FROM dead_letters WHERE received_at > now() - interval '1 hour' GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 10;`
2. Look at one: `SELECT convert_from(payload, 'UTF8'), detail FROM dead_letters WHERE reason = '<reason>' ORDER BY id DESC LIMIT 1;`
3. **Fleet-wide or single home?** One gateway means its firmware or clock (`future_timestamp`) or an impersonation attempt (`foreign_device`). Many gateways at once means a contract change: check the gateway firmware version against `docs/contract.md`.

---

## SLO burn (alerts SLOBurnFast, SLOBurnSlow)

**What it means:** fewer than 99% of readings (measured while their gateway was connected) are queryable within 30 s of being measured (S6-1), and the error budget is going fast. **Fast** is 14.4× the sustainable rate over 1 h; **slow** is 6× over 6 h.

**First three things to check:**
1. Is it ingest or the broker? IngestLagHigh (inside ingest) vs BrokerBacklogHigh (waiting at the broker).
2. Is it one gateway's clock? Look at the dashboard's clock-skew panel. A gateway skewed by over 30 s is already excluded and counted in `fleet_slo_readings_excluded_skew_total`. A skew just under 30 s still distorts its own readings.
3. Did gateways just come back from an outage? Their buffered readings are backfill and excluded (measured before `came_back_at`). If they're counted, check that ingest is writing `came_back_at` (`SELECT gateway_id, came_back_at FROM gateways ORDER BY came_back_at DESC NULLS LAST LIMIT 5;`).

---

## Health job stale (alert HealthJobStale)

**What it means:** one part of the API's background health job (`health`, `clock_skew`, `slo` or `gaps`, in the alert's `part` label) hasn't succeeded for 2 minutes, so its metrics are frozen. A failing part also stops the parts after it in the same pass. For `health`, that's GatewayOffline, DeviceStale and the command gauges; for `slo`, the SLO counters; for `gaps`, the gap totals.

**First three things to check:**
1. Is the API up? `curl -s localhost:8080/health/ready`.
2. Its log: `docker compose -f deploy/compose.yaml logs api | grep "health job" | tail`.
3. Which part is slow: `fleet_health_job_pass_seconds{part}`. Each part takes well under 2 s (S7-11, measured at 4.3M readings); for gaps, `fleet_health_gaps_step_seconds{step}` splits it into read, window and explain. After an API start, gaps waits for its warm-up (`fleet_health_gaps_warm` = 1 after about a minute for 500 gateways).

---

## Health job failing (alert HealthJobFailing)

**What it means:** one part of the health job failed at least twice in 5 minutes. The `reason` label says how:
- `timeout`: the database didn't answer within the API's 10 s statement timeout, or its 15 s query timeout (S7-10). The pass stopped there instead of holding a pool slot, and that part's metrics are as old as `fleet_health_job_part_last_success_timestamp_seconds{part}` says.
- `error`: anything else, usually a lost connection or a schema problem.

**First three things to check:**
1. The API's log: `docker compose -f deploy/compose.yaml logs api | grep "health job pass failed" | tail`. The error is next to each line.
2. For `timeout`, what Postgres is doing: `SELECT pid, now() - query_start AS running, wait_event_type, left(query, 80) FROM pg_stat_activity WHERE state <> 'idle' ORDER BY running DESC;`. A lock wait points to a long transaction elsewhere, and a long `gaps` scan to a large readings table (gaps scan all of a device's history).
3. Whether ingest is affected too: DBWriteErrors and BatchRetryingTooLong. If both are firing, it's the database, not the API.

**Right after an API deploy:** HealthJobFailing can fire from the old process's failures plus a cold-cache timeout on the new one (seen at the S7-11 deploy). It resolves within 5 minutes if the new process's parts succeed; check `fleet_health_job_part_last_success_timestamp_seconds`.

---

## Running the demo (`scripts/demo.sh`)

**What it is:** every fault scenario in order, each PASS, FAIL or INVALID, with a summary table in `demo-results/<timestamp>/summary.txt` and an exit status of 0 only if everything passed.

**Where it runs: the lab VM, never a laptop's Docker.** Demos, scenarios and integration runs go to the lab machine through `scripts/lab-vm.sh` (`LAB_HOST`, `LAB_SSH_KEY`): `sync`, then `run scripts/demo.sh` (or a scenario, or `corepack pnpm test:integration`), then `pull-results`. A laptop's 4 GB Docker VM is saturated by the 500-gateway stack and distorted every timing measured on it; on a quiet 8 vCPU / 32 GB machine the same code passes everything (decisions.md S6-6, S7-12). Typecheck, lint and unit tests run anywhere. The machine is started and stopped outside this repo, and it's billed while running, so say when a run ends.

**Before starting:**
1. **One runner, no leftovers.** Nothing else may touch the stack or toxiproxy while it runs. Check for scenario scripts, runner loops and stray `curl` or `docker` loops left by an earlier test run: `ps -axo pid,etime,command | grep -E 'scenarios/|run-|toxiproxy|8474|docker logs -f' | grep -v grep`, and stop them. A task launched hours earlier once sat polling the alert log through a whole demo; it turned out to be read-only, but proving that meant recovering the full command it was started with (S7-11).
2. **A quiet machine.** Record what else is running (`docker ps`) and the CPU pressure (`head -1 /proc/pressure/cpu` on the lab VM; inside a container on Docker Desktop). On the lab VM it stays under 1% for the whole demo (S7-12).
3. **Logs outside the containers.** Scenarios recreate containers, and `docker compose up` loses their logs. Stream the ones you may need (`docker logs -f fleet-ingest-<service>-1 >> file`) for the whole run.

**Reading the result:**
- **FAIL** comes with the failing check and its numbers. A scenario that couldn't finish prints `FAIL  the scenario could not complete (<reason>)`; an API answer other than 2xx carries its status and body.
- **INVALID** means a check couldn't be judged because the VM wasn't healthy *before* the fault (no 30 s of lag p95 under 2 s within 3 minutes, after the API reports its gap window warm). It isn't a pass, and demo.sh exits non-zero for it. The line gives the baseline it measured.
- **KNOWN LIMIT** lines show a documented limit as it happened (scenario 6: an open connection keeps publishing until the broker restart drops it, S4-3). They never count as PASS or FAIL.
