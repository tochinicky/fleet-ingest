# Decisions

This file covers every decision behind fleet-ingest, each with its trade-off. D1–D8 were fixed before the build started (the table below); each gets its full write-up in the slice that implements it. Decisions made during the build are added below, newest slice last.

This is a lab with a fictional domain. Nothing here describes production traffic.

## Design decisions D1–D8

Fixed before the build started. Later entries refine them (the S-numbers say where).

| | Decision | Why |
|---|---|---|
| **D1** | TypeScript on Node 24, strict, ESM, a pnpm workspace. `mqtt` (mqtt.js 5), `pg` with hand-written SQL and no ORM, `zod`, `fastify`, `prom-client`, `pino`; tests with `vitest`, `testcontainers`, `fast-check`. Migrations are numbered `.sql` files with a small runner. | Visible SQL, and a small set of well-known libraries. |
| **D2** | MQTT stays the transport, on Mosquitto 2.1.2. Ingest uses MQTT v5 persistent sessions and a shared subscription (`$share/ingest/...`), with two replicas, `ingest-1` and `ingest-2`, as named services with fixed client IDs and `restart: always`. | The gateway already speaks MQTT. Mosquitto doesn't move a dead member's share to the live one, so a fast restart under the same ID is the takeover: a shared subscription isn't a work queue. |
| **D3** | Acknowledge after commit; the broker queue is the bounded buffer. PUBACK only after the batch commits, through an `AckGate` (S0-1); Receive Maximum 1,000 per connection; a per-session queue bound with counted drops; persistence on disk (persist-sqlite since S7-7). | A slow database stops acks, the broker queues up to its bound, and past it drops are counted and alerted. That chain is the README guarantee. |
| **D4** | Plain Postgres 17. `readings` keyed by `(device_id, epoch, seq)` and inserted with `ON CONFLICT DO NOTHING`; `device_state` updated only by a newer reading. | Deduplication by construction, and current state can't go backwards when a backfill arrives after live data. |
| **D5** | Batching: flush at 500 rows or 200 ms, one transaction per batch. | Throughput against latency, measured in slice 6b. |
| **D6** | Identity is mutual TLS. The certificate's CN is the MQTT username, an ACL limits each gateway to its own topics, and a CRL handles revocation. Ingest also binds each device to the first gateway it appeared under. | The broker enforces identity per gateway; the device binding is defence in depth. |
| **D7** | Poison messages never block the pipeline: invalid JSON, schema failures, mismatched or foreign device IDs, and timestamps more than 5 minutes in the future go to `dead_letters` and are acknowledged. | A bad clock must not freeze `device_state` (D4 only accepts newer readings). |
| **D8** | Deployment: one small VM running the same Compose stack, created by Terraform, with Caddy in front. `terraform apply` only once the spend is approved; until then, `plan` plus a local production profile. | One real deployment path, and the only part that costs money. |

---

## Slice 0: spike findings (2026-10-14)

Evidence: [`spikes/mqtt-acks/`](../spikes/mqtt-acks/README.md), run against `eclipse-mosquitto:2.1.2-alpine` with mqtt.js 5.16.0.

### S0-1. mqtt.js's documented manual ack serializes delivery. **Stop condition, resolved: hold PUBACK packets behind an `AckGate` (decided 2026-10-14).**

- **Found:** mqtt.js handles incoming packets one at a time. It moves on to the next packet only after the current PUBACK has been written. Holding the `customHandleAcks` callback until a database commit therefore blocks every later message. Measured: 1 of 20 arrived while acks were held.
- **Why it matters:**
  - Batches can't fill, so D5's 500 rows / 200 ms can't happen.
  - Receive Maximum 1,000 is pointless, because only one message is ever in flight.
  - A commit slower than the keepalive also blocks PINGRESP, so the client kills a healthy connection (measured: `Keepalive timeout` after 2 s).
- **Workaround that passed every stop-tier check:** acknowledge the callback straight away, intercept the outgoing `puback` packet in the client's private `_sendPacket`, and write the held PUBACKs after the commit, in arrival order. The cost is a dependency on a private method. Mitigations: pin mqtt.js exactly, and add an integration test that fails if the behaviour changes.
- **Options considered:**
  - (a) the interception above;
  - (b) a small MQTT v5 client of our own on `mqtt-packet`;
  - (c) another client library;
  - (d) batch size 1.
- **Decision: (a), behind an `AckGate` adapter interface.** Conditions:
  1. **Pin mqtt.js to exactly `5.16.0`, with no caret.** A canary integration test fails if an upgrade stops PUBACKs being held, or if delivery serialises again.
  2. **Held acks belong to one connection generation.** On disconnect, every held PUBACK from the old connection is discarded and never written on the new one. After a resume, a packet ID can be reused for a *different* message, so a stale PUBACK could acknowledge a message that was never committed. That would be silent loss. Dropping those acks is safe: the broker redelivers the messages with DUP, and the idempotent key absorbs them. A deliberate-break test proves this rule.
  3. **Keepalive still works under the interception.** Proven in the spike (`deferred-hold-keeps-keepalive`): one ack held for 7.5 s with a 2 s keepalive, 3 PINGRESPs, still connected, and messages still arriving.
- **Why touch a private method:** it's the only place the PUBACK can be held without also holding up the receive loop.
  - **Risk:** a mqtt.js upgrade can rename or change `_sendPacket` silently. The exact pin and the canary test turn that into a loud failure.
  - **Exit path:** replace the adapter with (b), our own subscriber on `mqtt-packet`, behind the same `AckGate`, so nothing in the core changes.
- **Why the others were rejected:**
  - (b) means owning reconnect, keepalive and mTLS, which is too much scope now.
  - (c) hasn't been evaluated.
  - (d) changes D5 and kills throughput.

### S0-2. In-flight to ingest is set by ingest's Receive Maximum, not by `max_inflight_messages`.

The expectation was min(RM, broker setting). Measured: with Mosquitto's default `max_inflight_messages`, the broker still sent ingest its full RM of 1,000, and RM 10 gave exactly 10. **So ingest's RM is the only knob for the in-flight window, and the batch can be as large as RM.** The broker's `max_inflight_messages` stays at its default (D3), so the Receive Maximum it advertises to gateways is unchanged.

### S0-3. The queue bound, as measured.

- **Offline persistent session:** the broker keeps exactly `max_queued_messages`, then drops the **newest**.
- **Connected session that has stopped acking:** about `max_queued_messages` + 2 × RM before dropping (1020 with bound 1000 and RM 10).
- **For D3, effective bound per ingest session ≈ 120,000 + 2 × 1,000.**
- Every drop is counted in `$SYS/broker/publish/messages/dropped`. The log line ("Outgoing messages are being dropped for client X") appears once per episode, not once per message. So **the `$SYS` counter is the primary drop signal**, and the log-watch only names the client.

### S0-4. `$SYS` on 2.1.2-alpine: heap and store gauges exist.

`$SYS/broker/heap/current` and `heap/maximum` are published. The research expected them to be missing on Alpine. So broker memory can come from `$SYS`, and cAdvisor becomes a cross-check instead of the only source. `$SYS/broker/store/messages/count` and `/bytes` give scenario 3a its queue-depth gauge directly.

### S0-5. Persistence: D3's premise holds.

- A clean restart keeps the queue (500 of 500).
- A SIGKILL keeps everything up to the last snapshot and loses everything after it (500 kept, 300 lost, with autosave at 5 s).

The README guarantee in D3 is accurate as written.

### S0-6. Sizing numbers for D3.

- **Memory:**
  - RSS is about **844 B per queued message** (299-byte payload), so **209 MB at 240k queued**.
  - Peak RSS while a snapshot of that queue is written (VmHWM) is also **209.3 MB**, so the snapshot adds no memory.
  - **Container memory limit = 209 × 1.25 ≈ 262 MB.**
- **`max_queued_bytes`:** it counts **payload bytes only**, so it is sized from payload, not RSS (agreed in review): **p95 payload size of real messages × 120,000 × 1.25 per session**.
  - The p95 comes from the real gateway's telemetry kinds and from fleet-sim. It's captured in slice 1 and recorded in S1.
  - The 299-byte spike sample gives about 45 MB as a first estimate.
- **Snapshot cost:** a full snapshot of 240k messages is 104 MB on disk and **freezes the broker for about 3.2 s**. With `autosave_interval 60` and a full backlog, that's about a 5% stall: gateways pause on their 16-message window, and ingest pauses with them. Accepted. The README states it next to the guarantee. The 120 s fallback in D3 would halve it. `persist-sqlite` (incremental writes) is the documented upgrade.

### S0-7. Shared subscriptions behave as the research read them (D2).

On a live broker:
- (a) Members are served in strict rotation, including a dead one.
- (b) The dead member's share queues up to its bound, then drops. The live member never gets it: 30 of 60, with 20 dropped and counted.
- (c) The dead member's in-flight messages come back with DUP=1 when it resumes with the same client ID.

D2's same-ID fast restart is the right takeover for Mosquitto.

---

## Slice 1: contract, schema, happy path (2026-10-14)

### S1-1. One image, TypeScript run directly by Node 24.

Node 24 strips types natively, so the services run their `.ts` sources directly with `node` and there's no build step or `dist/`. `tsc --noEmit` (TypeScript 7.0.2) does the type checking.
- **Constraints this imposes:** only erasable syntax (`erasableSyntaxOnly`: no enums, namespaces or parameter properties), and imports must use `.ts` extensions.
- **Trade-off:** startup pays a small strip cost, and a Node or TypeScript upgrade has to keep the two compatible. In exchange, what runs is exactly what you read.
- **Workspace packages:** they're linked by pnpm and resolve to their real path outside `node_modules`, so Node strips them too.
- **The image:** one `node:24.21.0-slim` image, non-root, installed with `--prod`. Compose picks the entry point for each service (ingest, api, fleet-sim, migrate).

### S1-2. Shared subscriptions never receive retained messages.

MQTT v5 doesn't deliver retained messages to a shared subscription, and Mosquitto follows the spec. So on first start, ingest doesn't see each gateway's retained `online` status. A gateway shows as "no status yet" until its next 60 s heartbeat. Observed on the live stack: 10/10 gateways had no status at 20 s, and all were online after the first heartbeat.
- **Effect:** after a cold start, gateway status lags by up to one status interval. Readings are unaffected.
- **Not a correctness problem for slice 5:** "offline" also means "no status for 3 × 60 s", and the Last Will is published live, so a session that exists receives it.
- **If it matters later:** a non-shared, status-only subscription on one replica would get the retained messages. The cost is that the replica has to coordinate who owns status.

### S1-3. Cold start: nothing is queued before ingest's sessions exist.

The broker queues messages for a session only once that session has subscribed. On a brand-new broker, every reading published before ingest's first subscribe has no subscriber and is discarded. Observed: fleet-sim started 6 s before ingest, and seq 1 was missing for some devices.
- **Fix:** ingest writes `/tmp/ingest-ready` after subscribing, its healthcheck tests for that file, and Compose starts fleet-sim only when both replicas are healthy.
- **Steady state is safe:** after the sessions exist (expiry 7 days), ingest downtime is covered by the broker queue.
- **The runbook will need this:** a brand-new broker, or an expired session, has a loss window until ingest has subscribed.

### S1-4. Slice 1 behaviour on a database failure: stop, and let the restart redeliver.

Slice 1 processes one message per transaction, strictly in order.
- **When a write fails as retryable or fatal:** the processor stops taking work, logs, and exits with the message unacknowledged. `restart: always` brings the replica back with the same client ID, and the broker redelivers.
- **When Postgres rejects a row as poison (SQLSTATE 22/23):** the message goes to `dead_letters` as `rejected_by_database`, and the pipeline moves on.
- **Why not retry in-process now:** retries with backoff and full jitter, batching and batch splitting are slice 3. Exit-and-redeliver is the simplest behaviour that never loses or double-stores.

### S1-5. Duplicates are already absorbed in slice 1; the state guard comes in slice 2.

- **Duplicates:** QoS 1 duplicates are normal, so `INSERT … ON CONFLICT (device_id, epoch, seq) DO NOTHING` is in place from slice 1. Without it, a redelivery would hit the primary key, and the pipeline would dead-letter a good reading.
- **Current state:** `device_state` is still last-write-wins. Slice 2 adds the newer-only guard (D4), the property test, and the deliberate break that shows why it's needed.

### S1-6. No foreign key from `readings` to `devices`.

The hot insert path checks only the primary key. Ingest registers the gateway and the device (`ON CONFLICT DO NOTHING`) in the same transaction as the reading, so the binding exists without a per-row FK lookup. `devices → gateways` does have an FK, because those rows are written once.

### S1-7. Contract decoding.

- **Validated against the real gateway:** zod schemas tested against 114 captured messages (all four kinds, two epochs, the Last Will). .NET writes 7 fractional digits and a `+00:00` offset, and both are accepted.
- **Extra fields are stripped, not rejected**, so a newer gateway can add fields.
- **`null` measurements are rejected,** because the gateway omits them instead.
- **Status `mode` is a free string,** so an unknown mode can't get a status dead-lettered.
- **Dead-letter reasons in slice 1:** `unknown_topic`, `invalid_json`, `schema_mismatch`, `device_id_mismatch`, `rejected_by_database`. Slice 2 adds `future_timestamp` and `foreign_device` (D7).

### S1-8. `max_queued_bytes 24450000`.

That's p95 payload 163 B (real gateway, the larger of the two measured) × 120,000 × 1.25. fleet-sim's p95 is 159 B. Both are in `docs/contract.md`.

### S1-9. The AckGate in production code (S0-1 conditions met).

- **Pinned:** mqtt.js is `5.16.0` exactly in both packages.
- **Canary tests:** a batch of 20 arrives while all PUBACKs are held, and the broker stops at Receive Maximum until acks are released.
- **Connection generations:** a token from a closed connection is discarded and never written.
- **Deliberate break:** with the rule off (`GateWithoutGenerations`), the stale acks acknowledge the redeliveries, and all 5 messages are lost. The same scenario with the real gate gets all 5 back. Both are in `packages/ingest/test/ackGate.integration.test.ts`.

### S1-10. API reading history uses keyset pagination.

- **The cursor:** `(measured_at, epoch, seq)` is a total order, so the cursor resumes exactly after the last row, without OFFSET.
- **Limits:** a window is at most 7 days and a page at most 1,000 rows.
- **Auth:** a static bearer token, compared in constant time. The health endpoints are open.
- **Readiness:** `/health/ready` checks the database only, because the API has no broker connection until the commands endpoint arrives in slice 5.

### S1-11. Deliberate-break checks run in slice 1.

| Rule | Break | Caught by |
|---|---|---|
| Acknowledge only after the write commits (D3) | Moved `delivery.ack()` before the store call in `processOne` | 2 of 3 tests in `messageProcessor.test.ts` fail: the ack arrives before the write, and the database-down case acknowledges a message that was never stored |
| Discard acks from a closed connection (S0-1, condition 2) | `GateWithoutGenerations` (stale check off) | `ackGate.integration.test.ts`: the same scenario loses all 5 messages with the broken gate and keeps all 5 with the real one |

The remaining planned breaks (insert without `ON CONFLICT`, `device_state` without the newer-only guard, gaps compared across epochs) belong to slices 2 and 5.

---

## Slice 2: idempotency and ordering (2026-10-14)

### D4 in full: stored once, and state never goes backwards.

- **Dedup** is the primary key `(device_id, epoch, seq)` with `INSERT … ON CONFLICT DO NOTHING RETURNING 1`. The returned row count says whether a reading was new or a duplicate (a metric in slice 6).
- **Newer-only state:** `device_state` is updated `WHERE (excluded.measured_at, excluded.epoch, excluded.seq) > (device_state.measured_at, device_state.epoch, device_state.seq)`.
  - It's the tuple rather than `measured_at` alone, so two readings with the same timestamp still have one deterministic winner. Without that, the end state would depend on arrival order, and the property test would rightly fail.
  - A late backfill is stored in `readings` but can't drag `device_state` back.
- **Epochs:** a power cut rolls `seq` back, but only together with a new `epoch`, so `(epoch, seq)` never repeats. Nothing compares seq across epochs.
- **Alternative not taken:** the design names TimescaleDB, partitioning and rollups as the next step and leaves them out of this build. At this scale, a plain table with the PK and `(device_id, measured_at)` is enough.

### S2-1. The property test: exactly-once for any delivery order.

`exactlyOnce.property.integration.test.ts` uses fast-check, 60 runs per execution, against real Postgres through the production store.
- **Input:** a clean stream for 1–3 devices, with 1–3 boots each, seq rollbacks of up to 3, and repeated timestamps.
- **Delivery:** the same stream with extra duplicates and shuffled into any order.
- **Requirement:** `readings` and `device_state` end up identical to the clean in-order run. `received_at` is excluded, because a duplicate keeps the first delivery's receive time.
- **Batches:** slice 3 adds arbitrary batch splits once ingest batches.

### S2-2. Gateway status is newer-only (fix from the slice 1 review).

- **The bug:** the two replicas each get some of a gateway's status messages, so a last-write-wins upsert could apply them out of order. Example: the Last Will `offline` reaches ingest-2, and the reboot's `online` reaches ingest-1 and is written first. The gateway would then show offline while it's online.
- **The fix:** `UPSERT_STATUS … WHERE gateways.last_status_at IS NULL OR excluded.last_status_at > gateways.last_status_at`. A gateway first registered through its readings has no status yet, so its first status always applies.
- **Caveat:** `last_status_at` mixes two clocks. It's the gateway's clock for `online`, and ingest's receive time for the Last Will, because the broker publishes the Will without a timestamp. Skew between them can misorder statuses only seconds apart. The next 60 s heartbeat corrects any such residue. The gateway is not changed.
- **Test:** `online(T2)` then `offline(T1 < T2)` leaves the gateway online. Removing the guard makes the test fail (break check `status-not-newer-only`).
- **The guard has blind spots, which slice 5 closes (found in review, 2026-10-14):**
  - **A gateway clock that's behind.** Example: 10 minutes slow. The Last Will is stamped at ingest's receive time T. The reboot's `online` 30 s later carries T + 30 s − 10 min, which is earlier than T, so it's ignored, along with every heartbeat for about 10 minutes.
  - **A late Last Will.** If a Will waits in a down replica's session, it's stamped with that replica's receive time when the replica returns, so it can overwrite a newer `online`.
  - **Not clamping clocks:** every clamping scheme has its own edge cases.
  - **Slice 5 makes "online" evidence-based.** A gateway is online if its stored status is `online` **or** ingest has received any reading from it in the last 60 s, measured by `received_at` on ingest's clock. It's offline only if the status says offline **and** nothing has arrived recently. So neither clock problem can make a working gateway look dead.
  - **Tests to add in slice 5:** a slow gateway clock, and a late queued Last Will.
  - **Slice 6** adds a per-gateway clock-skew gauge: the median of `received_at − measured_at` over live, non-backfill readings. That makes a slow clock visible instead of inferred.

### S2-3. D7 checks that need no database run in the pure core; the device binding runs in the transaction.

- **`future_timestamp`:** checked in `routeMessage`. A reading more than 5 minutes ahead of ingest's receive time is dead-lettered.
  - The same check applies to **status** messages, because status is now newer-only too. One far-future status would hide every real one after it.
  - That gateway's readings still flow, so its broken clock shows up in `dead_letters` while the data keeps coming.
- **`foreign_device` (D6):** a device is bound to the gateway it first appeared under.
  - `BIND_DEVICE` is a single statement: an `INSERT … ON CONFLICT DO NOTHING RETURNING` CTE `UNION ALL` a `SELECT` of the existing binding. If a concurrent transaction binds the device at the same moment, neither half returns a row, and a second `SELECT` reads the binding.
  - A mismatch rolls back the whole transaction (including the impostor gateway's registration) and dead-letters the message.
- **Measured live:** malformed JSON, a timestamp 1 h ahead, another gateway claiming a bound device, a topic/payload device mismatch and a negative seq each landed in `dead_letters` with the right reason. The pipeline kept running, and `device_state` didn't move.

### S2-4. Deliberate breaks are a script now.

`node scripts/break-checks.ts` breaks each rule in the source on purpose, runs the test that should catch it, requires that test to **fail**, and restores the file. Result on 2026-10-14: 7/7 caught.

| Break | Rule | Caught by |
|---|---|---|
| `ack-before-write` | ack only after commit (D3) | `messageProcessor.test.ts` |
| `stale-acks-sent` | discard acks from a closed connection (S0-1) | `ackGate.integration.test.ts` |
| `insert-without-on-conflict` | stored once per key (D4) | the property test |
| `device-state-not-newer-only` | state moves forward only (D4) | the property test |
| `status-not-newer-only` | status moves forward only (S2-2) | `postgresStore.integration.test.ts` |
| `future-timestamp-accepted` | broken clocks are dead-lettered (D7) | `core.test.ts` |
| `foreign-device-accepted` | device binding (D6) | `postgresStore.integration.test.ts` |

Still to come: "gaps compared across epochs" in slice 5, and "batch splitting" in slice 3.

### S2-5. Code-documentation gaps closed.

- Every `IngestStore` method has TSDoc.
- The local closures are now named, documented functions:
  - `stopAndExit` and `shutDownGracefully` (ingest);
  - `shutDown` (API and fleet-sim);
  - `recordReadings`, `scheduleReadings` and `logUplink` (fleet-sim);
  - `publishOnline` (virtual gateway);
  - `deadLetter`, `futureSkew` and `routeStatus` (routing).
- Function-typed interface members are documented too.
- A scan confirms every declaration in `packages/*/src` has a TSDoc block, and there are no inline comments in function bodies.

### S2-6. fleet-sim takes a new epoch on every start (simulator bug found live).

- **The bug:** fleet-sim always started at epoch 1, seq 1. After a restart against an existing database, its new readings reused `(device, 1, seq)` keys, and ingest correctly dropped them as duplicates. 0 rows were stored for a device after the restart.
- **Why the real gateway is fine:** it increments `epoch` on every boot.
- **The fix:** fleet-sim's default `EPOCH` is now its start time in whole seconds, so every start gets a new, larger epoch and seq starts again at 1, just like a real reboot.
- **The lesson:** the dedup key is only as good as the producer's promise never to reuse it.

---

## Slice 3: backpressure and retries (2026-10-14)

### D3 and D5 in full: batches, acknowledged after commit.

- **Batching:** the pipeline routes each delivery as it arrives and collects it in a `Batcher`. A batch is written at **500 rows or 200 ms after its oldest message**, whichever comes first, in **one transaction**. Only then are its messages acknowledged, in arrival order, through the AckGate.
- **One batch at a time:** while a batch is being written, new deliveries keep collecting. That keeps acknowledgements in arrival order, and lets the next batch fill while the current one commits.
- **The bound:** the batcher has no limit of its own and needs none. Everything in it is unacknowledged, and the broker never sends more than Receive Maximum (1,000) unacknowledged messages. So ingest's memory is bounded by Receive Maximum × message size, whatever the database is doing.
- **The backpressure chain:** DB slow or down → no COMMIT → no PUBACK → Receive Maximum reached → the broker stops sending and queues (up to 120,000 per session) → past the bound, drops are counted.
- **Persistence (since 2026-10-15, S7-7):** the persist-sqlite plugin, committing every 5 s. That 5 s is the crash window for acknowledged queue. Snapshots (`autosave_interval 60`) are the commented fallback in `mosquitto.conf`.
- **Config:** `BATCH_MAX_ROWS` may not exceed `RECEIVE_MAXIMUM`, and startup refuses it otherwise. A batch bigger than the in-flight window could never fill, and every batch would wait the full 200 ms.

### S3-1. Set-based SQL for a batch.

A batch of 500 readings takes a fixed handful of statements, not 2,000 round trips: `unnest` turns parameter arrays into rows.
- Existing device bindings, one `SELECT … = ANY`.
- New gateways and devices.
- `INSERT readings … SELECT FROM unnest(…) ON CONFLICT DO NOTHING RETURNING` the keys.
- `INSERT device_state … SELECT DISTINCT ON (device_id) … ORDER BY measured_at DESC, epoch DESC, seq DESC … WHERE newer`.
  - Within a batch, only each device's newest reading competes.
  - Against the stored row, the same tuple decides.
  - So the end state doesn't depend on how the stream was cut into batches. The property test now also cuts the delivery into random batches of 1–12 and requires the same result.

Statuses and dead letters are rare, so they're written one by one, in arrival order, in the same transaction. Within a batch, the first copy of a reading reports `inserted` and later copies `duplicate`.

**Device binding inside a batch:**
- **A device new to the database is claimed by the first gateway in the batch that sends it.** Later readings from other gateways become `foreign_device` dead letters.
- **If another transaction binds the device first,** `INSERT … ON CONFLICT DO NOTHING RETURNING` leaves it out of the returned rows. The binding is then re-read, and the concurrent claim wins.

### S3-2. Retries with full-jitter backoff; a poison row is isolated by halving.

- **Retryable errors** are connection loss, shutdown, overload, serialization failures and deadlocks (SQLSTATE 08, 53, 57P0x, 40001, 40P01, and Node socket errors). The **same batch** is retried after `random() × min(30 s, 200 ms × 2ⁿ)`, without limit. Nothing is acknowledged while it retries.
  - The unit test checks that two replicas' delays for the same attempt are uncorrelated (|r| < 0.1 over 2,000 pairs) and rarely coincide.
- **Poison errors** are data and constraint errors (22, 23). `writeIsolatingPoison` halves the batch until the bad row is alone. That row becomes a `rejected_by_database` dead letter, and the rest commit.
  - One bad row among 500 costs at most about 2·log₂ 500 ≈ 18 extra writes.
  - Halves that already committed may be written again when a retryable error interrupts the search. That's harmless: every write is idempotent.
- **Fatal errors** (anything else) stop the pipeline. The batch stays unacknowledged, the process exits, `restart: always` brings it back, and the broker redelivers.
- **A realistic poison row:** a NUL character (`\u0000`) in a JSON string passes the schema but Postgres can't store it in `text` (SQLSTATE 22021). It's used in the store test.
- **After a failed transaction** the connection is destroyed instead of returned to the pool, because it may be broken.

### S3-3. No circuit breaker in front of the single database.

- **Why not:** a circuit breaker protects a struggling dependency from callers that would otherwise pile on, and lets them fail fast and do something else. Ingest has nothing else to do and nobody waiting: the messages it can't store wait safely in the broker queue. Capped exponential backoff with full jitter already sheds the load, at most one attempt per replica every 30 s. So retrying *is* the breaker here.
- **When one would earn its place:**
  - several consumers of the database, where one should give way to others;
  - a caller that can fail fast to a fallback, such as an API answering from a cache;
  - or a shared database that must not see even capped retries from many replicas.

### S3-4. Graceful shutdown with a deadline.

- **On SIGTERM:** ingest stops taking deliveries and writes what's in hand, including a partial batch. It acknowledges it, then disconnects.
- **Bounded:** this is limited by `SHUTDOWN_GRACE_MS` (8 s, under Docker's default 10 s stop timeout).
- **If the database is down at that moment:** the pipeline gives up at the deadline and leaves the batch unacknowledged. That's safe: the broker redelivers it to the same client ID on restart.

### S3-5. Toxiproxy between ingest and Postgres.

Ingest connects through `toxiproxy:15432`. The API, the migration job and the scenario checks connect to Postgres directly. A scenario can then cut or slow the database for ingest only, and still verify what was stored.

### S3-6. fleet-sim keeps a PUBACK ledger.

- **What it is:** every reading the broker acknowledges is recorded as per-(device, epoch) seq ranges (`AckLedger`), served on `GET :8090/ledger`.
- **Why it's the ground truth:** once acknowledged, the gateway has deleted its copy. So every ledger entry must be stored exactly once (3a, 7). In 3b, what's acknowledged but not stored must match the broker's drop counter.
- **Memory:** ranges keep it small, about one entry per device per epoch while delivery is in order.

### S3-7. Ingest logs a summary line every 10 s until slice 6.

The line carries received, redelivered, inserted, duplicate, statuses, dead letters by reason, batches, retries, discarded stale acks, waiting and unacknowledged. The scenarios read it (for example "redeliveries > 0" in scenario 7). Slice 6 replaces it with Prometheus metrics.

### S3-8. Consistent lock order across replicas (deadlock found in scenario 7).

- **What happened:** during the 500-gateway run, the two replicas deadlocked inserting `gateways` rows (`40P01`, "while inserting index tuple … in relation gateways"). Their multi-row statements reached the same keys in different orders.
- **Was it harmful:** no. `40P01` is classified as retryable, so Postgres killed one transaction, the batch was retried, and nothing was lost.
- **The fix, which prevents it:** every multi-row write now takes its locks in one global order.
  - gateway IDs are sorted;
  - new devices are sorted by device ID;
  - readings are inserted `ORDER BY device_id, epoch, seq`;
  - `device_state` was already `DISTINCT ON (device_id) ORDER BY device_id`;
  - status upserts are sorted by gateway ID, with a stable sort, so one gateway's statuses keep their arrival order for the newer-only guard.
- **The lesson:** retrying makes a deadlock harmless, but ordering makes it not happen.

### S3-9. ⚑ Mosquitto 2.1.2 doesn't restore shared subscriptions after a restart, but says "session present".

- **Found live:** after scenario 3b restarted the broker, both ingest replicas reconnected and received **nothing**. `$SYS/broker/subscriptions/count` was 0, there were no errors, and no drops were counted. That's a silent, total outage.
- **Reproduced in the spike** (`subscriptions-survive-restart`): after a clean restart, a persistent session's **plain** subscription is restored (5/5 delivered), and its **shared** subscription is not (0/5). CONNACK reports `sessionPresent: true` in both cases.
- **Why ingest didn't notice:** mqtt.js re-subscribes only when the session is *not* present.
- **Fix:** the subscriber now subscribes on **every** connect (`resubscribe: false`, plus its own handler). Subscribing again to an existing subscription just replaces it.
  - If the re-subscribe fails, ingest exits, and `restart: always` brings it back.
  - Integration test: `brokerRestart.integration.test.ts`. Break check: `subscribe-only-once`, caught.
- **What's left: a loss window.** Between the broker coming back and ingest re-subscribing, readings have no subscriber. The broker acknowledges them to the gateway, discards them, and doesn't count them.
  - **Measured** (`scripts/scenarios/broker-restart.ts`, 500 gateways, one clean restart): **0 of 38,222** acknowledged readings lost. Ingest reconnects every 1 s and fleet-sim every 2 s, so ingest won the race.
  - **Why it's still real:** the real gateway's first reconnect is also after 1 s.
- **For a decision (D2/D3):** the README guarantee needs one more line: "a broker restart has a loss window until ingest re-subscribes (measured: 0 at 1 s vs 2 s reconnects)." The options are:
  - keep ingest's reconnect faster than the gateways', and document the window;
  - add a single non-shared subscription, which is restored, as a catch-all on one replica;
  - test whether `persist-sqlite` restores shared subscriptions (if it does, that's an argument for the upgrade).
- **Not seen in 3b's first restart:** I haven't established why. That restart went from the normal to the small-queue config, and data kept flowing.

### S3-10. Scenario results (2026-10-14, Apple Silicon, Docker Desktop 8 CPU / 4 GB, 500 gateways ≈ 400 msg/s)

| Scenario | Result | Measured |
|---|---|---|
| **3a**: Postgres down 120 s | **PASS 6/6** | broker queue peak 43,410 (bound 240,000), broker memory peak 38 MB (limit 262 MB), ingest 118 → 119 MB, 0 dropped, 75,428/75,428 acknowledged readings stored exactly once, drain 34.6 s, 28 retries |
| **3b**: Postgres down 60 s, `max_queued_messages 5000` | **PASS 4/4** | 18,620 drops counted, 18,370 acknowledged-but-not-stored (the difference is status messages the broker also dropped, which the reading ledger doesn't track), no OOM, no restart |
| **7**: `kill -9` ingest-1 | **PASS 6/6** | restart about 3 s, 0 dropped, 27,738/27,738 stored exactly once, 25 redelivered, ingest-2 steady at about 2,000 per 10 s, lag max 3.0 s then p95 0.04 s |
| broker restart (measurement) | 0 lost | 38,222/38,222 after the re-subscribe fix (S3-9) |

- **Scenario 7's first run:** ingest-1 took **14 s** from container start to connecting (lag max 22.8 s). A second crash took 2 s, and the scenario re-run took 3 s. I couldn't reproduce or explain it. It's recorded so that slice 6's startup metric can watch for it.
- **The alert checks** (IngestLagHigh in 3a, BrokerDroppingMessages in 3b) arrive with Prometheus in slice 6.
- **Drain rate:** about 1,250 msg/s while also taking the live 400 msg/s, with two replicas.

### S3-11. Slice 3 review follow-ups: the restart window is narrowed, and a queued backlog survives a clean restart.

- **The decision on S3-9:** (a) now, and (c) (does `persist-sqlite` restore shared subscriptions?) as a slice 6 measurement. (b) doesn't fix the shared group: a plain catch-all subscription would just be a third consumer of everything. The new, uncounted loss window goes into the README guarantee.
- **Ingest's reconnect interval is now 250 ms** (`RECONNECT_PERIOD_MS`), firmly below the gateway's 1 s first retry.
- **Coming in slice 6:**
  - a metric for the time from broker connect to SUBACK;
  - an **IngestStalled** alert: nothing received for 30 s while `$SYS` shows gateways connected;
  - a "batch retrying > 5 min" signal alongside IngestLagHigh;
  - fleet-sim's ledger also tracking status messages, so 3b becomes an exact equality.
- **Does a backlog queued in the shared sessions survive a clean restart? Yes.**
  - Spike (`shared-backlog-survives-restart`): 500 messages were queued for an offline shared session, the broker was cleanly restarted, and the session re-subscribed. 500/500 delivered.
  - Scenario (`scripts/scenarios/broker-restart-with-backlog.ts`, 500 gateways): Postgres was cut until **41,056** messages were queued, then a clean broker restart (**41,802** stored after loading the snapshot), then Postgres was brought back. **54,416/54,416** acknowledged readings were stored exactly once, and live traffic resumed from all 500 gateways. **PASS.**
  - **So the restart only loses its subscriptions, not its queues.** The D3 guarantee holds for a clean restart. The only loss window left is S3-9's: readings published after the restart, before ingest re-subscribes.

---

## Slice 4: identity and authorisation (2026-10-14)

### D6 in full: mutual TLS, the CN as the username, and a per-gateway ACL.

- **One broker listener, 8883, mutual TLS only.** The listener settings:
  - `require_certificate true`, so a client with no certificate fails the TLS handshake;
  - `use_identity_as_username true`, so the certificate's CN *is* the MQTT username;
  - `allow_anonymous false`;
  - `crlfile`, so revoked certificates fail the handshake.
- **There is no plain listener.** Even the operators' `$SYS` reads go through 8883 with the `ops` certificate. That leaves the broker no "inside the network, so trusted" door.
- **Who holds certificates:** every gateway (CN = gateway ID), each ingest replica (`ingest-1`, `ingest-2`), the API (`api`, used for commands in slice 5) and `ops`.
- **Certificates are mounted, never baked into an image** (`deploy/certs`, gitignored). `scripts/dev-certs.sh <n>` makes the CA, the broker certificate, the service certificates and `sim-gw-0001…n`: 500 gateways in about 14 s.
- **Alternatives considered:**
  - **API keys or passwords per gateway:** a shared secret sent on every connect, easy to copy off a device, and the broker needs a password database.
  - **JWTs:** good for short-lived tokens issued by an identity service, but the gateway would need to fetch and refresh them over the same link it uses for data.
  - **Why certificates win:** the private key never leaves the gateway, the broker verifies identity with nothing but the CA, and revocation is a list.
  - **Costs:** running a CA (issuing, rotating, revoking), and the CRL-reload gap below (S4-3).

### S4-1. The ACL is tighter than the planned `pattern write homes/%u/#`.

| Who | May publish | May read |
|---|---|---|
| gateway (CN = ID) | `homes/<ID>/status`, `homes/<ID>/devices/+/telemetry` | `homes/<ID>/commands` |
| `ingest-1`, `ingest-2` | nothing | `homes/+/devices/+/telemetry`, `homes/+/status` |
| `api` | `homes/+/commands` | nothing |
| `ops` | nothing | `$SYS/#` |

- **Why tighter:** `write homes/%u/#` would let a gateway publish a retained message on `homes/<ID>/commands`, overwriting the desired mode the cloud set for it. Tested: refused.
- **The Last Will** is checked against the same ACL when the gateway connects. `homes/<ID>/status` is allowed.

### S4-2. Mosquitto grants an unauthorised SUBSCRIBE, then filters at delivery.

**Monitoring rule** (accepted in review, applied in slice 6): to check an ACL, *verify delivery*, never SUBACK codes. A client that's being filtered can't tell from its SUBACK.

A gateway subscribing to another gateway's readings, or to `$SYS/#`, gets a SUBACK with the requested QoS, not 0x87. The read ACL is enforced per message when the broker delivers. Tested: nothing is delivered. **Consequence for tests and monitoring:** check what is *delivered*, not the SUBACK codes. A client can't tell from the SUBACK that it's being filtered.

### S4-3. ⚑ Revocation: SIGHUP reloads the CRL for new handshakes, but an open connection stays up.

- **The procedure:** `scripts/pki.sh revoke deploy/certs <ID>` regenerates `crl.pem`, then `docker compose kill -s HUP mosquitto` reloads it. **New** connections with that certificate then fail the handshake ("certificate revoked").
- **The gap:** the gateway's **already-open connection keeps working.** Mosquitto checks the CRL at the handshake only.
  - Measured in scenario 6: after revocation and SIGHUP, the open connection still published, and the reading was stored.
  - The broker test asserts both halves: new handshake refused, open connection still publishing.
- **Until the connection drops,** a revoked gateway can keep publishing under its own ID. Its readings are still bound to its own devices (D6 binding), so it can't write as anyone else.
- **Options:**
  - (a) The runbook restarts the broker after the SIGHUP, which drops every connection. The revoked gateway can't come back, and the rest reconnect within seconds. A clean restart loses nothing (S3-11), apart from the S3-9 window, which ingest's 250 ms reconnect covers.
  - (b) An ingest-side `revoked_gateways` list that dead-letters anything from a revoked ID (`revoked_gateway`). This is defence in depth that works without touching the broker, but it's one more list to keep in sync with the CRL.
  - (c) A broker with an admin "disconnect client" API. Mosquitto's dynamic-security plugin may offer this; not verified, and it's a bigger change.
  - **My recommendation:** (a) now, as a runbook step; (b) if a broker restart is considered too disruptive.
- **Decided 2026-10-14: (a), with a timeboxed check of (c), and no (b).**
  - (b) is out: a second revocation list kept in sync with the CRL is a new failure mode, and ingest sees topic IDs, not certificate serials.
  - The runbook ("Revoke a gateway") now states the cost of the restart and ends with checks that the gateway is locked out.
- **The (c) check (well inside its 30-minute timebox).** Mosquitto 2.1.2 ships `mosquitto_ctrl` with two modules:
  - `broker`, which offers only `listPlugins` and `listListeners`. There's no per-client disconnect on the broker control API.
  - `dynsec`, the dynamic-security plugin, which can `disableClient` or `deleteClient`, and that ends a client's session. But only for clients that dynsec itself manages: it must be the authentication and ACL backend instead of the CRL plus `acl_file`.
  - Moving D6 onto dynsec is a D6-level change, so per the rule, it stops here and (a) stays.
- **Stage 3 note:** real fleets need a per-client disconnect at revocation time. Managed IoT brokers (and dynsec-style admin APIs) provide one. Revocation that takes effect only at the next handshake, or a broker restart to force it, is a Stage 2 limit, not a design ideal.

### S4-4. Threat list.

| Threat | What stops it, or what's accepted |
|---|---|
| **A stolen gateway certificate and key** | The thief can publish only as that gateway, for that gateway's own devices (ACL plus device binding). Fix: revoke, SIGHUP, and drop the connection (S4-3). Accepted: readings sent before revocation are stored, and that gateway's own data may be wrong until it's noticed. Detection comes in slice 6: a gateway's clock skew, rate or device set changing suddenly. |
| **Impersonating another gateway** | It needs a certificate with that gateway's CN from the lab CA. A certificate from another CA, or none, fails the handshake. Publishing on another gateway's topic with your own certificate gets PUBACK 0x87 (scenario 6, broker test). |
| **Claiming another gateway's device under your own topic** | The broker allows it, because the topic is yours. Ingest's device binding dead-letters it as `foreign_device` (scenario 6). |
| **Replaying old messages** | A replay with the same (device, epoch, seq) is a duplicate (D4) and changes nothing. A replay with a *new* key is new data, which the protocol can't tell apart from the real thing. Accepted. The only actors who can publish under an ID are its certificate holder and the broker. Mitigation: a timestamp more than 5 min in the future is rejected (D7). Old timestamps are accepted as backfill by design. |
| **A malicious payload** | zod validates every field (types, ranges, enum values) before any SQL is built. All SQL is parameterised; there's no string concatenation of values. A NUL or an out-of-range value reaches Postgres at worst as a rejected row, which is dead-lettered (S3-2). Payloads are stored as `bytea` in `dead_letters`, never evaluated. Ingest never logs payloads at info level. |
| **An exposed API** | It has a bearer token compared in constant time, health endpoints only without it, and binds to `127.0.0.1` in Compose. Accepted for a lab: one static token, no per-user auth (a non-goal). Slice 7 puts Caddy (TLS) in front. A real service would add OAuth, rate limits and audit logs. |
| **A compromised CA key** | Everything falls. Accepted for a lab: the key sits unencrypted in `deploy/certs`. A real fleet keeps it in an HSM or a managed CA, issues from an intermediate, and enrols devices at manufacturing. |
| **A broker container exposed to the host** | Only 8883 (mTLS), on `127.0.0.1`. There's no anonymous listener. |
| **A forged or revoked broker certificate presented to a gateway** | The gateway trusts only the lab CA (`WithTrustChain`), so a certificate from another CA fails. **Accepted:** the gateway doesn't check whether the *broker's* certificate was revoked, because the lab CA publishes no CRL or OCSP endpoint. A stolen broker key would be trusted until the CA itself is replaced. A real deployment publishes revocation (CRL distribution point or OCSP) and turns the check on. |

### S4-5. The gateway TLS change (home-energy-gateway, its own commit).

- **What changed:** `UplinkOptions` gains `TlsCaFile`, `TlsCertificateFile`, `TlsKeyFile` and `TlsServerName`. Setting the CA file turns TLS on. `UplinkTls` builds MQTTnet's options:
  - trust only the configured CA (`WithTrustChain`, not the system store);
  - present the gateway's certificate;
  - TLS 1.2 or 1.3;
  - no revocation check of the broker certificate, because the lab CA publishes no CRL endpoint.
- **Bug found by the test:** `X509Certificate2.CreateFromPemFile(caPath)` also looks for a private key in the CA file and throws. The CA is loaded with `CreateFromPem` instead.
- **Plain 1883 stays the default,** so the gateway repo's own demo compose is unchanged.
- **Tests:** in the gateway repo, `UplinkTlsTests` uses certificates made in .NET:
  - the right certificate connects and publishes under its ID;
  - another gateway's topic gets NotAuthorized;
  - a client certificate from a foreign CA is refused;
  - a broker certificate from a foreign CA is refused.

### S4-6. The slow restart (S3-10), traced: it's CPU contention during module loading.

- **Recurred:** the 14 s restart from slice 3 came back in slice 4's scenario 7 over mTLS, at about 10 s from container start to connect. Review asked for timestamped connect logs if it did.
- **Instrumentation now in ingest:** a `starting` log with `sinceProcessStartMs`, `connecting to broker`, a `broker connection event` for every connect, close, offline, error and CONNECT sent, and `ingest running` with the totals.
- **Measured:** three SIGKILLs under 500 gateways, with ingest, fleet-sim and Postgres each at about 50% CPU.

  | Phase | Time |
  |---|---|
  | node start → first log (loading modules) | 1.3–3.4 s (0.5 s on an idle machine) |
  | CONNECT sent → CONNACK | about 0.5 s |
  | session resumed → subscribed and running | 0.4–1.3 s |
  | **total** | **3.0–5.7 s** |

- **Cause:** time goes into loading and type-stripping the modules while the Docker VM's CPU is saturated, not into the broker or TLS.
- **The 10–14 s outliers** coincided with the heaviest load (the 500-gateway start-up) and didn't recur once instrumented.
- **Tried and reverted:** Node's compile cache (`NODE_COMPILE_CACHE`) showed no measurable gain (1.5–2.4 s), and I'm not keeping an unproven optimisation.
- **Impact:** the restarted replica's share waits in its session (D2), so lag rises by the restart time and nothing is lost (scenario 7 PASS every time).
- **Slice 6:** the startup and connect timings become metrics.
- **Slice 4 scenario 7 over mTLS:** PASS 6/6 at 500 gateways. 29,664/29,664 stored, 0 dropped, 0 broker denials or errors.

### S4-7. Docker's disk filled up during this slice.

- **What happened:** Postgres test containers failed with "No space left on device", and 22 integration tests were skipped.
- **The fix:** `docker builder prune` (10.8 GB of build cache), approved by the maintainer.
- **Break checks are now guarded:** a "caught" break was meaningless while the tests failed for this other reason. `scripts/break-checks.ts` now first requires every involved test file to **pass on the unbroken code**, and stops if one doesn't.
- **Result:** 11/11 breaks caught with healthy baselines.

---

## Slice 5: health, gaps and commands (2026-10-14)

### S5-1. "Online" is evidence-based, and every age is measured on ingest's clock.

The rules live in `packages/api/src/core/health.ts` (pure). They're applied in order:
1. **A reading received in the last 60 s, after the latest `offline` status → online**, whatever the stored status says. "After the latest offline" was added from scenario 2's measurements; see S5-6.
2. **No status ever → unknown.** It's never folded into online or offline (S1-2). A gateway that was offline when ingest's sessions were created sends no heartbeat, so it stays unknown until it reconnects.
3. **Last status `offline`** (usually the Last Will) **→ offline.**
4. **Last status `online`, but nothing heard for 3 × 60 s → offline.** This covers a Will lost in a broker restart.
5. **Otherwise → online.**

- **Why rule 1 comes first:** it makes the two clock problems from the slice 2 review harmless for any gateway with devices. Both are unit-tested:
  - **A gateway clock 10 minutes slow:** its reboot `online` loses to the Will's receive time under the newer-only guard, so the stored status stays offline. Readings keep it online.
  - **A late, queued Will:** a replica that was down writes an old `offline` after the reboot's `online`. Readings keep it online.
- **Migration 002** adds `gateways.last_status_received_at`, ingest's receive time of the latest status of any kind. It's updated on every status, **even one the newer-only guard rejects**, because it records "when did we last hear it", not "what does it say".
  - The status upsert is now one statement, with `CASE WHEN newer` per column, instead of a `WHERE` guard that skipped the whole update.
  - The break check `status-not-newer-only` now targets the shared `STATUS_IS_NEWER` condition.
- **Remaining edge case:** a gateway with **no devices** *and* a clock that runs behind would show offline until its clock is fixed. Nothing contradicts the stored status. Accepted. Slice 6's per-gateway clock-skew gauge makes the slow clock visible.

### S5-2. Device stale means: the gateway is online, but the device sent nothing for 60 s.

That's a different alert from GatewayOffline, because a different team acts on it: a device problem inside the home, not connectivity. A device behind an offline or unknown gateway is never stale. Its silence is the gateway's.

### S5-3. Gaps: a window function per (device, epoch), explained by `droppedTelemetry`.

- **The query:** `lag(seq) OVER (PARTITION BY device_id, epoch ORDER BY seq)` over readings measured in the window. A gap is `seq − prev > 1`.
- **Why epoch is in the partition:** seq starts again after a power loss, so comparing across epochs would turn every reboot into a false gap.
  - The break check `gaps-across-epochs` (epoch removed from the partition) is caught by the API test.
  - Scenario 8 measures it live.
- **A gap that a later replay fills disappears by itself,** because the query reads only what's stored.
- **Explanation (`explainGaps`, pure):** when the gateway's buffer overflows, it drops the **oldest** readings and counts them in `droppedTelemetry`. So a gateway's gaps, oldest first, are marked `explained` while their missing counts fit within that counter.
  - This is a heuristic: the counter says how many were dropped, not which.
  - It's computed across all the gateway's devices, because the counter covers all of them.
- **`/fleet`** reports open gaps over the **last hour**: the gaps, the missing readings, and how many are explained. Over a longer window the query would get heavy. Slice 6's background job keeps a running total for the metric.
  - **Since S7-11** it reports the health job's last gaps pass (at most about 60 s old, with `asOf`) instead of running the query per request: at 4.3M rows the query took 12.6 s.

### S5-4. Commands are desired state, published retained with the `api` certificate.

- **The endpoint:** `PUT /gateways/:id/mode {"mode": "surplus"|"max"|"off"}`.
  - The body is validated against the contract's command schema (400 otherwise).
  - The gateway must exist (404).
  - The broker must be connected (503).
  - It then publishes **retained, QoS 1** on `homes/<id>/commands` and answers **202 Accepted**: the gateway applies it when it's connected.
- **Authorisation:** the API connects with its own certificate (CN `api`). The ACL lets it write only command topics (S4-1).
- **Readiness:** `/health/ready` now needs both the database and the broker.
- **fleet-sim** subscribes to its command topic and reports the applied mode in its status, like the real gateway.
  - Verified live: `PUT …/sim-gw-0003/mode {"mode":"max"}` led to `gateways.mode = max` for sim-gw-0003 only.

### S5-5. fleet-sim: the gateway's keepalive, a cuttable route, and power-loss chaos.

- **Keepalive 15 s,** as the real gateway's. The broker publishes the Last Will 1.5 × keepalive ≈ 22.5 s after a dead link.
- **The fleet reaches the broker through toxiproxy** (`wan`). Every 10th gateway uses its own route (`wan-cuttable`), so scenario 2 cuts 10% of the fleet with a `timeout` toxic: packets stop, with no FIN, like a real dead link.
  - TLS passes through the proxy unchanged.
  - fleet-sim sets `servername: mosquitto`, so the broker's certificate still matches.
- **`POST :8090/chaos/power-loss?gateway=<id>&rollback=<n>`** gives that gateway the next epoch, with seq rolled back by n. It never goes below 1, and only together with a new epoch.

### S5-6. A reading proves life only if it arrived after the latest `offline` (found by scenario 2).

- **What scenario 2 measured first:** the cut gateways were reported offline after **60.8 s**, not ~25 s. The rule "any reading in the last 60 s means online" kept a dead gateway online for a minute after its last reading, even though its Last Will arrived at about 22 s.
- **The refinement:** a reading counts as proof of life only if ingest received it **after the latest `offline` status**.
  - Migration 003 adds `gateways.last_offline_received_at`, ingest's receive time of the latest `offline`. It's set on every offline status, even one the newer-only guard rejects, and kept through later `online`s.
- **How each case comes out:**

  | Case | Before the refinement | After |
  |---|---|---|
  | Dead link (scenario 2) | offline after 60 s | **offline as soon as the Will arrives (23.5 s measured)** |
  | Gateway clock 10 min slow, reboot | online (readings) | online: its readings arrive after the Will |
  | Will processed late by a replica that was down | online | offline for at most one reading interval (≤ 5 s), then online |
  | No status ever, recent reading (cold start) | online | online |

- **The cost** is that ≤ 5 s flicker in the late-Will case. In exchange, the offline alert can fire within the ~25 s target.
- **Status:** this refines the rule set in the slice 2 review, and was confirmed in review.

### S5-7. Scenario results (2026-10-14, 500 gateways)

| Scenario | Result | Measured |
|---|---|---|
| **2**: 50 of 500 gateways lose their internet for 120 s (toxiproxy `timeout` toxic, no FIN) | **PASS 5/5** | offline detected in **23.5 s**; all 500 online again 12.5 s after the link returned; 50,761/50,761 acknowledged readings stored exactly once; 0 gaps; 379 readings backfilled (lag > 60 s) |
| **8**: power loss on sim-gw-0001 (next epoch, seq rolled back by 5) | **PASS 4/4** | every device stored new-epoch readings, including 5 that repeat an old-epoch seq; `device_state` moved to the new epoch; 0 gaps reported across the power loss |

- The first scenario 2 run measured 60.8 s, which led to S5-6.
- GatewayOffline as a firing alert comes with Prometheus in slice 6. Here, detection is measured by polling `/fleet`.

### S5-8. Health is only as fresh as ingest (found in the slice 5 review).

- **The edge:** if a replica is backlogged (a slow database), a dead gateway's queued readings can arrive *after* its Last Will, so rule 1 keeps it "online" until the backlog drains and 60 s pass. The unit test pins this down: offline detection is delayed by at most the backlog plus 60 s.
- **Not fixed** by requiring a recent `measured_at`: that would bring back the slow-clock problem.
- **Made visible instead:** `/fleet` and `/gateways/:id` return `healthAsOfLagSeconds`, the p95 of (received − measured) over readings received in the last 30 s. A consumer can see when health is stale.
- **Slice 6:** the GatewayOffline runbook says "if IngestLagHigh is firing, offline detection is delayed by up to the lag", and the two alerts are linked by annotation, with no inhibition.

### S5-9. `droppedTelemetry` is a lifetime counter, so gaps are explained against all of a gateway's history.

- **The flaw the review found:** slice 5 first compared the counter with only the gaps in the requested window, which over-explains. 300 lifetime drops would "explain" every gap in any window.
- **Now:**
  1. `gapsInWindow` takes **all** of the gateway's gaps since the counter started (all time, all devices, all epochs);
  2. `explainGaps` assigns the counter to them oldest first;
  3. only then is the result filtered to the window for display. `/fleet` does this only for gateways that have a gap in its window.
- **Stopping rule:** stop at the first gap that doesn't fit. That gap and all later ones are unexplained, even a later, smaller one that would fit. Drops take the oldest buffered readings, so an explained gap can't follow one the counter couldn't cover.
  - So for explain-back Q4 (300 dropped; gaps of 200, 150 and 50, oldest first), **only the 200 is explained**.
- **Tests:**
  - unit tests for the stopping rule and the lifetime spend;
  - an integration test: 300 lifetime drops, an old 300-gap three days ago, a new 50-gap in today's window → the 50-gap is unexplained;
  - break check `gaps-explained-in-window`.
- **Accepted:** if a gateway's store is wiped, its counter restarts, and old gaps lose their explanation. Ingest can't tell. The status only carries the current count.

---

## Slice 6a: metrics, alerts, SLO, dashboard (2026-10-14)

### S6-1. What "lag", "backfill" and the SLO measure (decided in review).

- **The original spec contradicted itself.** It defined backfill as "lag > 60 s", then asked for "IngestLagHigh: p95 lag > 60 s, excluding backfill", which can never fire. And `received_at` is set when ingest *receives* a message, before any retries, so it can't say when a reading became queryable.
- **IngestLagHigh** = p95 of `ingest_commit_delay_seconds` (ingest receipt → batch commit) > 60 s for 5 min.
  - It's clock-free and catches stuck retries.
  - Blind spot: time spent waiting in the *broker* queue before receipt, which is exactly where backpressure parks messages.
- **BrokerBacklogHigh** therefore covers the broker side: `$SYS` stored messages > 24,000 (10% of the 2 × 120k bound) for 5 min. Together the two cover "slow inside ingest" and "waiting at the broker".
- **Backfill** = a reading measured before its gateway came back: `measured_at < gateways.came_back_at`.
  - `came_back_at` is ingest's receive time of the first `online` after the latest `offline`, set in the status upsert (migration 004).
  - Counted by the API's health job (`fleet_backfill_readings_total`), the only place that knows `came_back_at`. Ingest's lag histogram no longer labels backfill.
- **SLO:** 99% of readings measured while their gateway was connected (not backfill) are **stored within 30 s of being measured**, over 7 days.
  - `stored_at` (migration 004) is the database's transaction time: when the reading became queryable.
  - The SLO compares the gateway's clock (`measured_at`) with the database's, so a skewed gateway distorts its own readings. The fleet-wide ratio resists one bad clock, and the skew gauge shows which one it is.
  - Gateways skewed by more than 30 s are left out and counted in `fleet_slo_readings_excluded_skew_total`.
- **Burn-rate alerts** on a 1% budget: SLOBurnFast (14.4× over 1 h, confirmed over 5 min) and SLOBurnSlow (6× over 6 h, confirmed over 30 min).
  - Seen live: cutting the database for about 100 s fired SLOBurnFast, and it resolved after the drain.

### S6-2. Where each metric comes from.

| Source | Metrics |
|---|---|
| **ingest** (`:9464`) | received (redelivered or not), readings stored by outcome, statuses, dead letters by reason, batch size and write time, lag (received − measured), commit delay, DB retries, current batch retrying seconds, unacked, waiting, broker connected, connect → SUBACK, startup seconds, last-message timestamp, plus Node's defaults (RSS, event-loop lag, GC) |
| **api** (`:9464`): the background health job, every 15 s, gaps every 60 s | per-gateway offline, gateways by state, per-device stale, stale count, per-gateway clock skew (median received − measured of live readings, 5 min), open-gap readings explained or not (last hour, explained against all-time gaps, S5-9), SLO total, good and excluded-skew counters, backfill counter, pass time per part |
| **broker-exporter** (`:9464`) | 13 `$SYS` values (dropped, stored count and bytes, heap, clients, subscriptions, message counters, uptime) read with the `ops` certificate; log-watch counters per kind and per client (dropping, expiring) |
| **cAdvisor** | container working set, RSS and CPU, as a cross-check of `$SYS` heap |

- **Alerts are evaluated by Prometheus from these, never by the API.** That's why the health rules export per-gateway and per-device gauges, which Prometheus turns into GatewayOffline and DeviceStale with a 2-minute hold.
- **The log-watch "sidecar"** of D3 is folded into the broker exporter. It follows the broker's log file on a shared volume.
- **The exporter runs as Mosquitto's uid (1883), read-only,** because Mosquitto creates its log `0600`.

### S6-3. Alerts.

| Alert | Expression (short) | Runbook |
|---|---|---|
| GatewayOffline | `fleet_gateway_offline == 1` for 2m; annotated: delayed by IngestLagHigh (S5-8) | ✓ |
| DeviceStale | `fleet_device_stale == 1` for 2m | ✓ |
| IngestLagHigh | p95 commit delay > 60 s for 5m | ✓ |
| BrokerBacklogHigh | `broker_store_messages > 24000` for 5m | ✓ |
| IngestStalled | nothing received for 30 s while > 6 clients are connected (S3-9 made loud) | ✓ |
| BatchRetryingTooLong | the current batch failing > 300 s | ✓ |
| DBWriteErrors | retries in the last 2 min, for 1m | ✓ |
| DeadLettersAppearing | dead letters by reason in the last 5 min | ✓ |
| BrokerDroppingMessages | `$SYS` dropped rose, or the log reports dropping | ✓ |
| IngestSessionExpired | the log reports an `ingest-*` session expired | ✓ (cold start) |
| SLOBurnFast, SLOBurnSlow | multi-window burn rate | ✓ |
| HealthJobStale, BrokerExporterStale | the fleet or broker metrics stopped updating | ✓ |

- **Related alerts are linked with a `related` annotation, with no inhibition:** IngestLagHigh ↔ GatewayOffline ↔ BrokerBacklogHigh.
- **Alertmanager** groups by alert name and sends to `alert-logger`, which logs each alert and lists them on `GET :9095/alerts` for the demo. There's no real paging.
- **`pnpm test:alerts`** runs `promtool check`, then `promtool test` (rules.test.yml: IngestStalled, BrokerBacklogHigh, BrokerDroppingMessages across a counter reset, and the never-set-gauge cases), then checks that every alert's runbook link resolves to a heading.
- **Seen live:**
  - a database cut fired DBWriteErrors, which then resolved;
  - SLOBurnFast fired, then resolved;
  - a malformed message fired DeadLettersAppearing.

### S6-4. Three Prometheus traps found while wiring this up.

- **A counter series that first appears already at 1 is invisible to `increase()`.** There's no earlier sample, so DeadLettersAppearing never fired.
  - Fix: every known label combination is created at 0 at startup. In prom-client 15, `labels(x)` alone exports nothing, so it's `labels(x).inc(0)`.
  - The broker log counter's `client` label can't be known in advance, so a per-kind counter (created at 0) carries the alerting. The per-client one says *who*.
- **A gauge that was never set exports 0, i.e. 1970.** So "time since last run" alerts fire right after a restart: HealthJobStale fired falsely after an API restart.
  - Fix: every staleness rule requires the gauge to be > 0, and holds for 1 min.
  - promtool tests cover this.
- **cAdvisor's page is about 1 MB and takes about 3 s on Docker Desktop.** The default 10 s scrape timed out once under load. It now scrapes every 30 s with a 25 s timeout, and `--docker_only`.

### S6-5. Observations.

- **The health job is cheap once warm:** health 0.6 s, clock skew 0.2 s, SLO 0.01 s, gaps 2.8 s once a minute. Its first pass after a restart took about 12 s (cold caches).
- **One transient stall:** batch-write p95 reached 45 s for 1–2 minutes while cAdvisor and the observability stack started. It went back to 0.28 s with no retries. The suspect is I/O from cAdvisor's first scan of Docker's storage on the 4 GB Docker Desktop VM. To watch during the 6b load runs.

---

## Slice 6b: load, storms and the persistence experiment (2026-10-14)

### S6-6. The stalls are the saturated Docker VM, not cAdvisor, `docker stats` or checkpoints.

- **The request from review:** turn S6-5's suspicion into evidence. `scripts/scenarios/stall-probe.ts` times, once a second, a full mTLS connect to the broker (it accepts connections on its single main loop) and a `SELECT 1`, under the 500-gateway load.

| Setup | Broker connect p50 / p99 / max | Postgres p50 / p99 / max | Stalls over 2 s |
|---|---|---|---|
| cAdvisor on, `docker stats` polled every 5 s **in the probe's own process** | 12.5 s / 24.7 s / 24.7 s | 6.2 s / 66 s / 66 s | 6 (an artefact, see below) |
| cAdvisor on, no polling | 144 / 821 / 1,192 ms | 24 / 246 / 369 ms | 0 |
| cAdvisor on, polled from a **separate** process | 128 / 666 / 1,121 ms | 22 / 152 / 844 ms | 0 |
| cAdvisor on, no polling (second window) | 102 / 322 / 393 ms | 16 / 124 / 196 ms | 0 |
| cAdvisor **off**, no polling | 131 / 465 / 10,788 ms | 24 / 267 / 2,155 ms | 1 (broker and Postgres slow together) |

- **The first row is an artefact.** The scenario helper ran `docker stats` *synchronously*, which froze the probe's own event loop, and with it its timings. It's fixed: `containerMemoryMb` is asynchronous now, which also makes every scenario's timing honest. Polling from a separate process causes no stalls.
- **cAdvisor isn't the cause.** 0 stalls in 11 minutes with it on, 1 in 4 minutes with it off.
- **Postgres checkpoints aren't the cause.**
  - They run every 5 minutes, each spread over about 270 s (`checkpoint_completion_target` 0.9), so one is always in progress. The sync phase is 0.1–0.3 s.
  - The stalls don't line up with syncs: 18:41:41 fell mid-checkpoint, and the sync was at 18:43:31.
  - `pg_stat_checkpointer` shows 24 timed and 2 requested checkpoints, with 2.2 s of sync in total.
- **The cause is the Docker VM, at its limit.** Pressure-stall information inside the 4 GB / 8 CPU Docker Desktop VM under 500 gateways:
  - **CPU** "some" is 53%: tasks wait for a CPU over half the time.
  - **Memory** "full" is 2.5%: every task is stalled on reclaim 2.5% of the time, with 90 MB free.
  - **I/O** "some" is 12–14%.
- **How the stalls show up:** rare, multi-second, and hitting the broker and Postgres together.
- **The largest one:** 31 s with no broker activity at all during 3b's first run, 18:16:40 → 18:17:11. Every gateway then hit its 22.5 s keepalive limit at once.
- **Production relevance:** none of this is an application fault. What matters is that the system handled it:
  - the gateways reconnected and resent;
  - the duplicates were absorbed;
  - the only visible effect was on the drop counter's accounting (S6-7).
- **The load numbers below were measured in this constrained VM,** and say so.

### S6-7. Scenario 3b is now exact.

fleet-sim's ledger also counts the status messages the broker acknowledged. 3b checks the broker's drop counter for **exact equality**:
- readings acknowledged but never stored (ledger vs Postgres);
- plus statuses acknowledged but never received (ledger vs `ingest_statuses_stored_total`).

| Run | Drops counted | Readings lost + statuses lost | Equal? |
|---|---|---|---|
| 1 | 12,722 | 12,222 + (−6) = 12,216 | no |
| 2 | 10,793 | 10,574 + 219 = 10,793 | **yes, PASS** |

- **Why run 1 missed:** it hit the 31 s VM stall (S6-6) mid-outage. All 500 gateways timed out, reconnected, and resent their unacknowledged messages. A resent copy that the full queue dropped again is counted, even though the original was stored (+506). A status resent and delivered twice shows up as "−6 lost".
- **Spike checks rule out a broker counting bug:**
  - two stalled shared-subscription members: 2,960 dropped, 2,960 lost;
  - retained statuses: 2,960 dropped, 2,960 lost.
- **Lesson:** the `$SYS` drop counter counts drop *events*, not lost *messages*. Without resends they're equal. With resends after a reconnect, the counter is an upper bound, and the ledger is the truth.

### S6-8. Stage 3 note: metric cardinality.

- **Stage 2:** per-device stale gauges (about 2,000 series) and per-gateway offline and skew gauges (about 500 each) are fine.
- **At 100k gateways** those per-entity series go: alerts move to counts per state, and the per-entity view becomes a `/fleet` drill-down (and an alert annotation that links to it).

### S6-9. fleet-sim's jittered reconnect: let mqtt.js reconnect, and randomise its period.

- **The first version broke the uplink.** It turned mqtt.js's own reconnect off (`reconnectPeriod: 0`) and called `client.reconnect()` after a random delay.
  - With reconnect off and a clean session, mqtt.js *flushes* its outgoing queue when a connection is cleaned up.
  - The uplink loop then waited forever on publishes that never resolved.
  - In the first scenario 4 run, all 504 clients reconnected and the broker was idle, but fleet-sim's outbox kept growing (430k). The gateways were connected and sending nothing.
- **The fix:** keep mqtt.js reconnecting by itself. It reads `options.reconnectPeriod` each time it schedules an attempt, so fleet-sim sets a freshly drawn delay on every `close`, `reconnect` and `connect`.
  - `RECONNECT_JITTER_MAX_MS > 0`: uniform in [0, max), full jitter.
  - `0`: a fixed 2 s, as before.
- **Verified:** with a 60 s outage, 11,981 buffered readings drained in 60 s, and 47,156/47,156 acknowledged readings were stored.

### S6-10. Load numbers (500 gateways ≈ 400 msg/s; Apple M1, 8 cores, 8 GiB; Docker Desktop VM 8 CPU / 4 GB, saturated, S6-6)

| Configuration | Steady msg/s | Lag p50 / p95 | Receipt → commit p50 / p95 | Ingest RSS steady → flood | Capacity (backlog drain) |
|---|---|---|---|---|---|
| **2 replicas, batch 500** (D5) | 395 | 35 ms / 0.58 s | 0.18 s / 0.45 s | 113 → 135 MB | 21,370 backlog drained in **18 s**, peak 704 msg/s total (the backlog was gone before either replica ran flat out) |
| **1 replica, batch 500** | 401 | 46 ms / 6.9 s | 0.22 s / 0.81 s | 154 → 175 MB | 20,000 drained in **33 s**, peak **2,756 msg/s** in a 20 s window: one replica flat out |
| **2 replicas, batch 1** | **175** (of 400 offered) | 167 s / 287 s, growing | 10.2 s / 28.0 s | 202 → 187 MB | never drained within 20 min: about 200 msg/s per replica, the queue passed its 240k bound, and the broker dropped 68,223 |

- **Batching is what makes the target reachable** (D5's trade-off, measured).
  - With one row per transaction, a replica manages about **200 msg/s**: two replicas can't keep up with 400 msg/s, and the backpressure chain ends in counted drops.
  - With batches of up to 500, **one replica sustains about 2,750 msg/s**, about 7× the fleet's load. The original assumption of "300 msg/s per replica" is very conservative.
- **Ingest memory stays bounded under a flood** (+20 MB). Receive Maximum caps what's in memory (D3).
- **One replica's steady lag p95 of 6.9 s** is higher than with two replicas. Commit p95 is 0.81 s, so the time is spent before receipt, in the broker queue and on the saturated VM (S6-6), not in ingest.
- **Scenario 4's assumptions should be revisited:** its jitter sizing assumed ingest drains at about 600 msg/s for two replicas. The measured drain rate is far higher (S6-11).

### S6-12. ⚑ persist-sqlite, measured (D3 experiment; switching is a maintainer decision).

Spike checks in `spikes/mqtt-acks/src/checks/persistSqlite.ts` (`plugin_opt_flush_period 5`), against the snapshot numbers in S0-5/S0-6 and S3-9:

| Question | Snapshots (current, autosave 60 s) | persist-sqlite (flush 5 s) |
|---|---|---|
| Shared subscription restored after a clean restart? | **No**: session present, 0/5 delivered (S3-9) | **Yes**: session present, 50/50 queued before the restart and 10/10 published after it delivered |
| Acknowledged queue lost on SIGKILL | everything since the last snapshot (up to 60 s) | everything since the last flush: 500/500 from before a flush kept, **0/300** published just before the kill kept (up to 5 s) |
| Longest broker stall with 240k queued | **about 3.2 s** per full snapshot, once a minute | **1.13 s** longest gap in a 5 ms echo probe while 240k were queued at 17,230 msg/s |
| Broker RSS at 240k queued | 209 MB | 213.5 MB (no change to the 262 MB limit) |

- **What it would buy:**
  - It closes the S3-9 loss window at its root: the shared group survives a restart, so the re-subscribe race no longer matters (the every-connect re-subscribe stays as a belt-and-braces measure).
  - It shrinks the SIGKILL loss window from up to 60 s to up to 5 s.
  - It removes the once-a-minute 3.2 s freeze under a full backlog.
- **Costs and caveats:**
  - It's a plugin new in 2.1.0, so it's less battle-tested than snapshots. It needs `persistence false` and a writable SQLite file on the broker volume.
  - The 1.13 s stall was measured with the full compose stack still running on the saturated VM (S6-6), so it's an upper bound, not a clean number.
  - The guarantee wording changes ("up to 5 s of acknowledged readings on a broker crash").
- **Recommendation:** switch. It's a config change and three lines of the README guarantee, and it removes a silent loss window rather than narrowing it. Without the switch, nothing changes: snapshots, the 250 ms ingest reconnect and the documented window stay as they are.

### S6-11. ⚑ Scenario 4, reconnect storm: passes, but one of two jittered runs lost 4 readings.

500 gateways, broker stopped cleanly for 600 s, then started (`scripts/scenarios/reconnect-storm.ts`):

| Run | Buffered at restart | Drain | Peak broker queue | Dropped | Acknowledged readings stored |
|---|---|---|---|---|---|
| jitter 0–300 s, run 1 | 236,751 | 452 s | 1,535 | 0 | **401,280 of 401,284: FAIL, 4 lost** |
| no jitter (every 2 s) | 209,844 | 682 s | 2,344 | 0 | 399,315 of 399,315 |
| jitter 0–300 s, run 2 (with diagnostics) | 239,613 | 636 s | 1,350 | 0 | 456,153 of 456,153: **PASS 4/4** |

- **The drain is far faster than the scenario assumed.** The jitter sizing assumed ingest drains at about 600 msg/s. In fact the broker queue never built up (peak 2,344 with no jitter at all), because ingest takes about 2,750 msg/s per replica (S6-10). The drain time is set by fleet-sim's replay rate (about 700 msg/s in total from one process on the saturated VM), not by ingest.
- **The 4 lost readings in run 1** were acknowledged by the broker and never stored, and the broker counted no drop.
  - **Most likely the S3-9 window:** with delays uniform in 0–300 s, about 2 of 500 gateways draw a delay under 1 s, so they may publish before ingest has re-subscribed its shared group after the broker loads its snapshot.
  - **Not confirmed:** the next run wiped the database before I could look. I added `missingReadings()` (which device, epoch and seqs, and when they were measured, against `brokerRestartedAt`), but run 2 lost nothing, so there was nothing to look at.
  - The no-jitter run lost nothing either. All gateways retry at 2 s there, so ingest at 250 ms always wins the race.
- **For a decision:** this is the same uncounted window as S3-9, now seen once at 4 readings in about 400k. The options are:
  - (a) accept it, and keep it documented in the guarantee (current state);
  - (b) give fleet-sim, and recommend for the real gateway, a minimum reconnect delay of 1 s after a disconnect;
  - (c) switch to persist-sqlite, which restores the shared subscription and closes the window at its root (S6-12).
  
  My recommendation is (c). (b) only narrows a race.

### S6-13. Scenario 4's drain assumption was wrong, in the safe direction: the drain time measures fleet-sim.

- **The assumption:** the jitter sizing assumed two replicas drain a reconnect storm at about 600 msg/s, so a 240k replay would queue at the broker for minutes.
- **Measured:** one replica alone peaked at **2,756 msg/s** (S6-10), and the broker queue never passed 2,344 in any scenario 4 run (S6-11). Ingest was never the bottleneck.
- **The bottleneck is fleet-sim:** one Node process holding 500 TLS clients on the saturated VM (S6-6) replays at about 700 msg/s in total. The 452–682 s drain times in S6-11 are the simulator's replay time, not ingest's capacity.
- **Why it's the safe direction:** the real risk in a storm is a queue that grows towards the 240k bound. With ingest about 4× faster than the replay, it doesn't. A real fleet replays faster than the simulator, and the 2,756 msg/s per replica is the headroom for that, still unmeasured from the sending side.
- **Not done:** a multi-process fleet-sim that could replay faster than ingest drains. It's what would actually load the broker queue in a storm, and it belongs to a bigger machine than this VM.

### S6-14. persist-sqlite canaries for the open upstream bugs (prepared for the switch, S6-12).

`packages/ingest/test/persistSqlite.integration.test.ts` runs in the integration suite against the pinned 2.1.2 image. Each test pins a bug as it affects fleet-ingest, so an image bump that changes the behaviour fails there first:

| Bug | Canary | Seen on 2.1.2 | Effect on fleet-ingest |
|---|---|---|---|
| [#3758](https://github.com/eclipse-mosquitto/mosquitto/issues/3758): restored in-flight messages lose DUP | 3 readings held unacknowledged, broker restarted cleanly | all 3 redelivered, **DUP = 0** | none for storage: the primary key absorbs the copy. `ingest_messages_received_total{redelivered}` undercounts redeliveries after a broker restart. |
| [#3761](https://github.com/eclipse-mosquitto/mosquitto/issues/3761): a crash brings back an ended session | ingest-2 ends its session (expiry 0) **just before** a SIGKILL | **reproduced:** ingest-2 comes back with its shared subscription and takes a round-robin share; ingest-1 gets fewer than 20 of 20 | a ghost group member queues readings that nobody reads, until its expiry or the queue bound. BrokerBacklogHigh catches it. |
| #3761, the safe path | ingest-2 ends its session, **one flush period (5 s) passes**, then SIGKILL | ingest-2 stays gone; ingest-1 gets 20 of 20 | none |
| No migration ([#3763](https://github.com/eclipse-mosquitto/mosquitto/issues/3763) breaks the script) | 50 queued under snapshots, clean stop, start with persist-sqlite on the same volume | no session, nothing queued; readings after ingest subscribes arrive | the switch is a fresh install: it must be made with the broker queue at 0 |

- **Ingest never ends its own session** (shutdown disconnects and keeps it), so #3761 needs an operator action, such as removing a replica, and a crash within 5 s of it. The switch adds that wait to the runbook.

### S6-15. ⚑ persist-sqlite re-runs (on the `persist-sqlite` branch; a maintainer decision).

The lab stack was switched following the new runbook ("Switch broker persistence"). Then the scenarios the review asked for ran in one batch (2026-10-14, 500 gateways, the same saturated VM as S6-6). **Every acknowledged reading was stored in every run: 0 lost anywhere.** Three checks that aren't about loss failed, and are reported as found:

| Scenario | Verdict | Acknowledged readings stored | Notes |
|---|---|---|---|
| 3a: Postgres down 120 s | PASS | 81,768 of 81,768 | 0 dropped, peak queue 43,320, broker 63 MB |
| 3b: bound 5,000, Postgres down 60 s | FAIL (1 of 4): accounting not exact | lost by design, all counted | counter 17,597 vs readings 17,097 + statuses 302 = 17,399. The counter is **higher**, which is S6-7's upper-bound direction (resent copies dropped again). No uncounted loss. |
| 7: kill -9 ingest-1 | FAIL (1 of 6): lag recovery | 29,088 of 29,088 | 49 DUP redeliveries absorbed, 0 dropped. Lag p95 after recovery was 4.99 s, against the 2 s bar. |
| broker-restart (run 1) | FAIL: 32 of 500 gateways delivering 60 s later | 0 of 18,411 lost | about 500 reconnected by +18 s, then went silent and were timed out by the broker ("exceeded timeout") about 40–60 s later. fleet-sim's log was lost to the next scenario's recreate, so this isn't explained. |
| broker-restart (runs 2, 3, in the A/B batch) | PASS | 0 of 31,313; 0 of 33,233 lost | 500 of 500 gateways delivering |
| broker-restart with a 43k backlog | PASS | 57,410 of 57,410 | backlog restored (43,440 before, 46,971 after) |
| 4: storm, jitter 0–300 s, run 1 | PASS | 383,287 of 383,287 | 243,936 buffered, drain 367 s, peak queue 10,165 (at restart) |
| 4: run 2 | PASS | 475,164 of 475,164 | 240,000 buffered, drain 838 s, peak queue 2,331 |
| 4: run 3 | PASS | 459,900 of 459,900 | 236,204 buffered, drain 734 s, peak queue 2,535 |

- **S3-9 is closed for restarts, as predicted:** 0 lost in all three restart-under-load runs (snapshots: 0 of 38,222, but only because ingest won the race; S6-11's run 1 lost 4). Every storm run stored every acknowledged reading.
- **Not settled, and why:** the A/B batch meant to compare scenario 7's lag and 3b's accounting with snapshots was aborted. By then the Docker VM was far past S6-6's levels: CPU pressure "some" 79% (S6-6: 53%), memory "full" 8.6% (2.5%), I/O "some" 37% (12–14%), with 686 MB in swap and 60 MB free. A forgotten probe container from slice 4 had been spinning at about 50% CPU for 7 hours. The sqlite scenario 7 run in that batch showed lag around 70 s and fleet throughput collapsing (ingest-2 storing 31 per 10 s), so the machine, not the broker, was being measured.
- **What it means for the decision:** the safety question (does anything acknowledged get lost?) has its answer: no, in 9 runs. The cost question (does persist-sqlite add steady lag?) is still open. Scenario 7's 4.99 s could be the VM or the plugin's write load, and only a snapshot-vs-sqlite pair on a quiet machine can tell.

---

## Slice 7: ship (2026-10-14)

### S7-1. Commands: desired state in Postgres, reported state from the gateway, and a reconciler between them.

- **The gap (found while preparing the persist-sqlite switch):** a gateway's desired mode lived only as a retained message on the broker (S5-4). A fresh broker, a persistence switch (S6-14) or a lost volume dropped every command a gateway hadn't applied yet. Nothing could rebuild them, because by design (D6) no certificate can read every command topic.
- **The pattern: desired vs reported state.**
  - **Desired** is what the cloud asked for: `gateways.desired_mode` and `desired_at` (migration 005). `PUT /gateways/:id/mode` writes it **first**.
  - **Reported** is what the gateway says it runs: `gateways.mode`, from its status (unchanged).
  - **The retained command is a copy of desired state,** not the record. The API's reconciler keeps it in line.
  - The Stage 3 equivalent is a device twin or shadow, as managed IoT platforms provide: the same two documents, plus a computed delta.
- **The endpoint:** 400 / 404 as before, then store, then publish. If the broker is down or the publish fails (5 s timeout), it **still answers 202**, saying the reconciler will publish it. The old 503 is gone: the command is no longer lost if the broker is away.
- **The reconciler** (`commandReconciler.ts`):
  - on **every broker connection**, it publishes **every** desired mode, because a new connection may mean a broker that lost all of them;
  - **every 60 s**, it publishes only the **pending** ones (desired ≠ reported). Re-publishing a retained message delivers it again to subscribed gateways, so publishing all 500 every minute would send every gateway a command a minute for nothing;
  - a publish that times out (5 s) has already been handed to mqtt.js, which sends in order, so a late one can't land after a newer publish queued behind it. The timeout only frees the queue (and the HTTP request);
  - the endpoint's publish and the reconcile passes share **one queue**. Without it, a pass that read the old mode could publish it just after the endpoint published the new one, and the retained copy would go backwards until the next pass.
- **Drift:** a pure rule, `commandStatus` in `core/health.ts`.
  - Pending = a desired mode the gateway doesn't report yet, since `desired_at`.
  - **Stuck** = pending on an **online** gateway for more than 5 minutes, counted from the later of `desired_at` and `came_back_at`. A gateway that was away isn't blamed for the time it was away, and an offline gateway never counts.
  - Exposed as `command` on `GET /gateways/:id`, `commands` (pending count, stuck gateway IDs) on `GET /fleet`, and the gauges `fleet_commands_pending{connected}` and `fleet_commands_stuck`. Counts, not per-gateway series (S6-8): `/fleet` is the drill-down.
  - **Alert CommandNotApplied:** `fleet_commands_stuck > 0` for 1 min, with a runbook entry.
- **D6 is unchanged:** the `api` certificate already writes `homes/+/commands`, and still reads nothing.
- **Proof:**
  - `commandReconciler.integration.test.ts`: a broker without persistence is restarted, which loses every retained message. The reconciler publishes again on reconnect, and a gateway subscribing afterwards gets its mode, parsed with the contract's schema as both gateways do.
  - The deliberate break `reconciler-not-on-connect` removes the reconnect pass, and that test must fail.
  - Unit tests cover the drift rule, and fast-check properties pin it: pending ⇔ desired ≠ reported; never stuck unless pending and online; once stuck, stays stuck; never stuck within 5 min of coming back. A promtool test covers the alert.
  - **The property test found an edge:** with `desired_at` NULL (only a manual SQL edit can do that), the rule ignored `came_back_at` and called a gateway stuck the moment it came back. It now counts from the later of whichever times are known.
  - The publish queue is a pure core module (`core/serialQueue.ts`), with a property: the last write added is the last one applied, whatever each takes.
- **The real gateway matches:** it reports `mode` lowercased in its status and parses commands case-insensitively (`MqttUplinkService.cs`), so desired and reported compare as plain strings.

### S7-2. Lint: ESLint parses with its own TypeScript 5.9, because TypeScript 7 has no classic compiler API.

- **The problem:** typescript-eslint parses with TypeScript's JavaScript compiler API. TypeScript 7 (the native port, which this repo type-checks with) no longer exports it, only an `unstable/` client API. typescript-eslint 8.71 declares `typescript >= 4.8.4 < 6.1`.
- **The choice:** a workspace package `tools/lint` with ESLint, typescript-eslint and **TypeScript 5.9.3** of its own. pnpm resolves typescript-eslint's peer from that package, so it gets 5.9 while the root keeps 7.
  - Rules: `@eslint/js` and typescript-eslint's `recommended`, **without type-aware rules**. Types are `tsc`'s job (TypeScript 7), and type-aware lint would type-check twice with two compilers that could disagree.
  - `prefer-const` allows a `let` that closures read before its single assignment (`ignoreReadBeforeAssign`). That's how the composition roots make forward references.
- **What lint found on its first run** (3 findings): one real one, fixed (a `this` alias in the metrics adapter, now an arrow function), and the two forward references above.
- **When to revisit:** when typescript-eslint supports TypeScript 7's API, drop `tools/lint`'s own TypeScript.
- Versions are pinned to releases at least two weeks old (eslint 10.11.0, typescript-eslint 8.71.0, globals 17.12.0).

### S7-3. CI (GitHub Actions), written and checked locally; it runs only once the repo is pushed.

| Job | Needs | What |
|---|---|---|
| lint | | `pnpm lint` (ESLint), `pnpm typecheck` (tsc 7) |
| unit | lint | `pnpm test` |
| alerts | lint | `pnpm test:alerts`: promtool check and test, runbook links |
| integration | unit | `pnpm test:integration`: Testcontainers, Postgres and Mosquitto with mutual TLS (the tests make their own PKI, so CI needs no secrets) |
| break-checks | integration | `node scripts/break-checks.ts`: every deliberate break must make its test fail |
| images | unit | `docker build`, then: Node 24 runs, as a non-root user. Not pushed anywhere. |
| terraform | | `fmt -check`, `init -backend=false`, `validate`. `plan` and `apply` stay manual (D8). |

- **Actions are pinned to commit SHAs,** with the release in a comment. The workflow token is read-only (`permissions: contents: read`).
- **`actionlint` is clean** (run locally from the `rhysd/actionlint:1.7.7` image, which includes shellcheck).
- **Not verified on GitHub:** nothing is pushed (D8, lab guardrail). The first push is when it really runs. The likeliest first failures are runner Docker limits for Testcontainers and the break-check job's length (it runs one integration test file per integration-tier break).

### S7-4. Terraform for one VM, `plan` only, and a local production profile (D8).

- **`deploy/terraform`:** the hcloud provider (pinned at 1.69.0) creates three resources: an SSH key, a firewall and the server.
  - The firewall allows 22 from `admin_cidrs` only (validated: never `0.0.0.0/0`), 80/443 for Caddy, and 8883 for gateways (mutual TLS).
  - cloud-init installs Docker and Compose from Ubuntu 24.04's own archive and creates a `fleet` user.
  - **`server_type` has no default,** because size and price are what the approval decides. 4 vCPU / 8 GB is the floor to ask for (S6-6).
  - State is local; the lock file covers linux_amd64 and darwin (CI and the laptop).
- **`plan`:** 3 to add. It ran with a placeholder token: this provider makes no API calls to plan new resources. A real-token plan at approval time is the first one that checks the server type exists.
- **`deploy/compose.production.yaml`:**
  - Caddy serves the API and Grafana over HTTPS; only it and 8883 bind to `PUBLIC_BIND` (0.0.0.0 on the VM, loopback locally).
  - fleet-sim moves to a `sim` profile.
  - `API_TOKEN` and `GRAFANA_ADMIN_PASSWORD` must be set (Compose refuses to start without them), and Grafana has no anonymous viewer.
  - Hostnames are sslip.io names for the VM's address, so Let's Encrypt works without buying a domain. Locally they're `*.localhost`, which Caddy serves with its internal CA.
- **`scripts/deploy.sh`:** rsync plus `compose up`, after an approved apply.
  - It copies **only** the service certificates: the CA's private key and the simulated gateways' keys stay on the laptop.
  - Secrets travel over stdin into a 0600 env file, never on a command line.
- **Accepted for the lab:** the Postgres password stays the dev default in the production profile. Postgres binds to loopback on the VM and the firewall doesn't open 5432, so it's reachable only from the VM itself.

### S7-5. fleet-sim's chaos knobs: duplicates, reordering and poison (scenarios 1 and 5).

- **Pure core** (`packages/fleet-sim/src/core/chaos.ts`), driven at runtime through `POST /chaos/settings {duplicateRate, reorderWindow, poisonRate}`:
  - **Duplicates:** a reading goes into the outbox twice, so the broker acknowledges two copies with one key.
  - **Reordering:** a per-gateway window releases readings in random order. A property test checks that every reading comes out exactly once, and that no more than `window − 1` are ever held. Lowering the window releases what was held.
  - **Poison:** an *extra* message, never a real reading, so the reading stream keeps no gaps. There is one kind per dead-letter reason (`invalid_json`, `schema_mismatch`, `device_id_mismatch`, `future_timestamp`, `foreign_device`).
    - Each parseable poison carries `seq = 900,000,000 + seq`, so one that ingest wrongly accepted would stand out, never be absorbed as a duplicate.
    - Poison bypasses the outbox and the ledger, and is skipped while disconnected: it isn't data.
- **`foreign_device` needs a bound neighbour.** It uses the next gateway's device. Ingest binds a device on its first reading (D6), so scenario 5 turns poison on only once every gateway is delivering.
- The other planned knobs already exist elsewhere: power loss (`/chaos/power-loss`, S5-5), network cuts (toxiproxy), impersonation (scenario 6's own client). A future-clock *gateway* is covered by the `future_timestamp` poison kind, which is what ingest sees from one.

### S7-6. The full demo: `scripts/demo.sh`, and scenarios 1, 5 and 7b (written; not yet run).

- **`scripts/demo.sh`** issues certificates for 500 gateways, starts the stack, then runs every fault scenario in order: 1, 2, 3a, 3b, 4 (jitter), 5, 6, 7, 7b, 8, 9.
  - Each scenario is PASS or FAIL. Its full log, and a summary table with the failing checks, go to `demo-results/<timestamp>/`.
  - It exits 0 only if everything passed. `SKIP="4 9"` leaves scenarios out.
- **New scenarios:**
  - **1, `duplicate-storm.ts`:** 20% duplicates in a reorder window of 50, for 120 s. Checks: ledger exactly stored, duplicates absorbed (the `duplicate` outcome rose), no device_state behind its newest reading, no gap.
  - **5, `poison.ts`:** 5% of readings each followed by one poison message, for 90 s, starting only once every gateway is delivering. Checks: all five reasons dead-lettered, no poison stored, ledger stored, inserted/s within 20% of before, DeadLettersAppearing fired.
  - **7b, `replica-down-overflow.ts`:** ingest-1 stopped for 90 s with the bound at 5,000. Checks: drops counted, counted ≥ lost (S6-7), BrokerDroppingMessages fired, ingest-2 kept its share, ingest-1 receives again once back.
- Scenarios now check alerts automatically through alert-logger (`waitForAlert`), not only by eye.
- **Not yet run.** They need Docker, and run after the persist-sqlite merge, on the merged master.

### S7-7. ⚑ Broker persistence is persist-sqlite (decided 2026-10-15), and a second comparison against snapshots.

- **The decision:** the switch was approved on 2026-10-15 and merged. Snapshots stay as a commented fallback in both mosquitto configs, and the runbook "Switch broker persistence" covers going either way. The case for it is S6-12 to S6-15: persist-sqlite restores the shared subscription and the queue after a restart, and its crash window is the 5 s flush period, not 60 s of snapshot.
- **The rule for after the merge** (agreed in review): the merge stands unless a sqlite run loses an acknowledged reading. A cost that reproduces gets documented here with its size, and doesn't trigger a revert.
- **A second comparison, after the merge** (2026-10-15, 500 gateways, the same saturated 4 GB VM as S6-6; CPU pressure "some" 25–65% with 250–540 MB in swap on both back ends; Evently.Redis and Bookify.Redis also running):

| Scenario | sqlite | snapshot |
|---|---|---|
| broker-restart, run 1 | FAIL: 411 of 500 delivering at +60 s; 0 of 10,891 lost | setup failure: not 500 delivering within 3 min of the cold switch |
| broker-restart, runs 2 and 3 | PASS, 0 lost | PASS, 0 of 38,156 and 0 of 34,305 lost |
| broker start → "running" → first CONNACK | 0 s → 29, 9, 13 s | 0 s → 10, 7 s |
| CONNACK timeouts in fleet-sim's log | 267 | 276 |
| 7 (one run) | FAIL 2 of 6: 0 of 27,385 lost, 0 DUP, lag p95 after 6.48 s | PASS: 0 of 28,844 lost, 486 DUP, lag p95 after 0.53 s |
| 3b | readings 12,850 ≤ 13,350 counted; "statuses lost" 1,666 | readings 14,930 ≤ 15,430 counted; statuses 340 |

- **Every acknowledged reading was stored in every run, on both back ends.** So by the rule, the merge stands.
- **Each difference, classified:**
  1. **broker-restart's 411 of 500 was the check, not the broker.** Toxiproxy accepts TCP while the broker is down, so attempts waited out mqtt.js's 30 s connect timeout. Fixed in S7-8 (fleet-sim's connect timeout is now 10 s, like the real gateway, and the check is judged on a budget).
  2. **3b's 1,666 "statuses lost" was a measurement artefact.** ingest-1 restarted 8 s into the cut, and its status counter reset. 3b now counts statuses with `increase()`, which survives a reset.
  3. **That ingest-1 restart was a real defect.** S7-9 first blamed it on two unclassified driver messages, and S7-10 found the real cause: an unhandled `error` on a checked-out client.
  4. **Scenario 7's 0 DUP and 6.48 s lag were noise on a saturated VM, not a sqlite cost.** It was rerun three times per back end (2026-10-15, logs streamed to files, `compose down` between runs):

| Run | lost | unacked at kill | DUP | fresh lag p95 | max lag | broker "exceeded timeout" | ingest-2's lowest 10 s | CPU pressure (60 s) |
|---|---|---|---|---|---|---|---|---|
| sqlite 1 | 0 of 25,966 | 669 | 0 | 13.66 s | 31.8 s | 189 | 110 | 23% |
| sqlite 2 | 0 of 25,726 | 145 | 36 | 5.13 s | 13.4 s | 0 | 1,015 | 48% |
| sqlite 3 | 0 of 15,877 | 956 | 15 | 42.64 s | 48.9 s | 8 | 0 | 44% |
| snapshot 1 | 0 of 25,725 | 426 | 222 | 2.85 s | 22.1 s | 0 | 288 | 48% |
| snapshot 2 | 0 of 23,792 | 657 | 192 | 9.18 s | 33.5 s | 0 | 502 | 34% |
| snapshot 3 | 0 of 21,939 | 247 | 12 | 6.05 s | 30.4 s | 213 | 113 | 42% |

  - **0 lost in all six**, and every run passed redelivery as it is judged now (unacked at the kill + 0 lost).
  - **DUP shows up on both back ends.** sqlite's earlier 0 doesn't reproduce: it was 0, 36 and 15. The key and `ON CONFLICT` absorb copies whether or not they carry the flag, and the live-reconnect canary shows DUP set on both back ends.
  - **Open observation, not a conclusion:** DUP was lower on sqlite in two of the three pairs (0 vs 222 and 36 vs 192; the third was 15 vs 12). It's unexplained, and it has no effect on correctness.
  - **The lag check failed in all six runs, on both back ends, and the lag wasn't caused by the kill.** Grouped by measurement time in 15 s buckets, p95 was already above 2 s in the 15 s *before* the kill in five of the six runs: 12.4, 15.3, 2.9, 29.3 and 29.0 s, against 1.2 s in sqlite run 2. Fleet-wide stalls also hit the replica that wasn't killed: ingest-2 stored 0 readings for 20 s in sqlite run 3, and the broker dropped hundreds of simulated gateways for missed keepalives in two runs. That's S6-6, the VM, and not something persistence changes.
  - **No reproducible lag cost for sqlite:** the two ranges overlap (sqlite 5.1–42.6 s, snapshot 2.9–9.2 s), and sqlite's worst run is the one where the whole VM stalled.
  - **So scenario 7 now waits for a quiet baseline** before the kill. It first waits (up to 5 min) for the API to report its gap window warm: waiting for the system to report itself ready is part of setup, not part of the judgement (S7-11). Then it waits up to 3 min for 30 s of p95 lag under 2 s. Without one, its recovery checks ("lag recovered" and "the other replica kept storing") are INVALID, and the p95 that was measured is recorded. 3a-hang's "prompt again" check uses the same gate. The gate measures recovery against a VM that was healthy before the fault. A stall *during* the judged window is the second, narrower rule (stallRule.ts, S7-11): a recovery or throughput check is INVALID only if the VM's CPU pressure reached 60%, the untouched replica stalled, and the broker timed out gateways, all three; data checks (lost, stored, dropped, counted ≥ lost) are never gated, because a stall can excuse slowness, never loss. The gate isn't there to hide slowness, and INVALID runs are reported along with how many there were: demo.sh shows INVALID as its own state ("N PASS / M FAIL / K INVALID"), and exits non-zero unless everything passed.
- **One note on the evidence:** the sqlite broker log from the first comparison was overwritten by a log watcher. The start-up numbers above (start → running → first CONNACK) were extracted before that happened; the rest of that log is gone. The reruns stream every container log to files outside the containers.

### S7-8. A restart costs each gateway up to its connect timeout, behind anything that accepts TCP for the broker.

- **Found in the comparison (S7-7):** in one sqlite broker-restart run, only 411 of 500 gateways were delivering at the fixed check. fleet-sim's log showed 267 "connack timeout" and 477 EPIPE. The broker was "running" in the same second it started, but the first gateway's CONNACK came 29 s later.
- **The mechanism:** fleet-sim reaches the broker through toxiproxy, which accepts the TCP connection even while the broker is down. An attempt made during the restart doesn't get "connection refused" and retry. It waits out mqtt.js's default 30 s `connectTimeout` for a CONNACK that never comes.
  - That isn't a lab artefact: a cloud load balancer in front of a broker does the same.
  - So the cost of a restart, per gateway, is up to one connect timeout plus its reconnect delay.
- **The real gateway's numbers** (home-energy-gateway, `MqttUplinkService.cs`):
  - MQTTnet `WithTimeout(TimeSpan.FromSeconds(10))` per attempt;
  - a reconnect delay doubling from 1 s, capped by `MaxReconnectDelay` = 30 s (`UplinkOptions.cs`).
- **Changes:**
  - fleet-sim gets `CONNECT_TIMEOUT_MS`, default **10 s** like the real gateway (it was mqtt.js's 30 s).
  - `broker-restart.ts` no longer checks "delivering at +60 s". Its two checks are now judged:
    - no acknowledged reading lost (S3-9 is closed by persist-sqlite; this fails on the snapshot fallback, as it should);
    - every gateway delivering again within **connect timeout + reconnect delay + 30 s** (42 s by default). The 30 s margin is for 500 TLS handshakes on the saturated VM (S6-6).
  - The time until all are back is recorded. It's measured from when `compose restart` returns, i.e. from the broker being up again.
- **Not yet run** with the new timeout; the next broker-restart runs, on the merged master, will.

### S7-9. Ingest exited on a database that hangs: two driver messages weren't classified as retryable.

- **Corrected by S7-10:** the hang runs showed the restart came from an unhandled `error` event on a checked-out client, not from these messages. The two messages stay retryable, and their canary stays; the diagnosis below was inferred from the code and was wrong.
- **Found by the comparison (S7-7):** in the sqlite 3b run, ingest-1's process started again at 13:14:29, 8 s after Postgres was cut at 13:14:21. Prometheus showed its status counter resetting. Its container was recreated afterwards, so its log was lost; what follows comes from the code, not from that log.
- **The defect:** `classifyDatabaseError` called a failure retryable only for a network code on the error itself, a SQLSTATE, or a message matching `Connection terminated|connection timeout|Client has encountered a connection error`. Two messages the driver uses for an unreachable database fell through to **fatal**, so the process exited and `restart: always` restarted it:
  - `timeout exceeded when trying to connect`: pg-pool, when no connection is ready within `connectionTimeoutMillis` (5 s in ingest), `pg-pool/index.js:224`;
  - `timeout expired`: pg, a connect that doesn't complete, `pg/lib/client.js:170`.
  - The 5 s connect timeout plus a backoff fits the 8 s.
- **Why it rarely showed:** a plain cut (toxiproxy disabled) refuses connections at once (ECONNREFUSED, always retryable). The timeouts happen when connects *hang*, which the saturated VM (S6-6) makes likely.
- **What it cost:** no lost data. A fatal exit leaves the batch unacknowledged and the broker redelivers it. It did cost a restart in the middle of an outage, a reset metric (which is what corrupted 3b's status count), and a restart loop for as long as the database hangs.
- **The fix:**
  - both messages are listed in `CONNECTION_LOSS_MESSAGE`, each with its source;
  - network codes are found anywhere in the error's `cause` chain, since pg-pool wraps one error in another;
  - one unit case per error shape: ECONNRESET, ETIMEDOUT, EPIPE, an AggregateError carrying ECONNREFUSED, both new messages, and a wrapped cause.
- **The pool's idle-client `error` event is handled** (`main.ts`: logged, not crashed). That was the other suspect.
- **Proof:**
  - the deliberate break `pool-timeout-is-fatal` removes the pool-timeout message, and the unit test catches it;
  - scenario 3 gained `DB_FAILURE=hang` (a toxiproxy `timeout` toxic: connections are held, nothing passes), and 3a/3b now fail if any ingest replica restarts during the outage;
  - the hang runs, with the fix removed and then in place, were started but haven't run: Docker Desktop stopped at about 15:57, before the first one got going.
- **Enforced, not just documented:** the fix matches driver text, so a driver release that rewords a message would bring the restart loop back.
  - pg stays pinned exactly in packages/ingest (8.23.0), and pg-pool, which pg takes as `^3`, is pinned to 3.14.0 by a pnpm override.
  - `driverMessages.test.ts` (unit, no Docker, no mocks) makes the real driver produce each message against a local TCP server that accepts and never answers, or drops the socket. Each must classify as retryable: `timeout expired`, `timeout exceeded when trying to connect` (the waiting connect of a one-client pool), `Connection terminated due to connection timeout` (that pool's first connect), and `Connection terminated unexpectedly`.
  - The break `driver-message-reworded` rewords one alternative, and the canary catches it.
- **Also fixed:** the scenario helper's own pool had no `error` handler, so a database shutdown crashed the script instead of failing it cleanly.

### S7-10. Ingest crashed when a database connection dropped mid-batch: nobody listened for a checked-out client's `error`.

- **Found by the hang runs (2026-10-15, 3a with `DB_FAILURE=hang`):** both runs failed "no ingest replica restarted", the one with the S7-9 fix in place exactly like the one without it. This time the ingest logs were streamed to files, and both replicas, in both runs, died with `Unhandled 'error' event` / `Connection terminated unexpectedly` at the moment the toxic was removed.
- **The defect:**
  - when a connected pg client loses its socket, pg rejects the query in flight and also emits `error` on the client (`pg/lib/client.js:204`, `_handleErrorEvent` at :422);
  - pg-pool listens for that only while a client is idle: it removes its listener at checkout (`pg-pool/index.js:344`) and puts it back on release (:385);
  - `writeBatch` held its client from `pool.connect()` with no listener of its own, and an `error` event with no listener throws, which kills the process. `restart: always` brought it back.
  - The `pool.on("error")` handler in `main.ts` (S7-9) only covers idle clients.
- **Why S7-9 got it wrong:** the 3b restart's log was lost, so its cause was inferred from the code. This explains it better. A plain cut (disabling the toxiproxy route) also closes live connections, so a replica crashes only if a batch is in flight at that instant, which is random and matches the restart showing up only sometimes. The 8 s is a container restart, not a 5 s connect timeout plus backoff.
- **What the hang runs also showed:** during the hang ingest made **0 retries**. The INSERT in flight just waited (waiting 970, unacked 1000, for the whole 120 s), because nothing bounded a query. A database that never closes the connection (a blackholed network) would have held that batch forever.
- **What it cost:** no lost data. Every acknowledged reading was stored exactly once in both runs (83,312 and 84,381). The cost was a restart per replica per outage, and a reset of its metrics.
- **The fix:**
  - **one way to hold a connection:** `withClient(pool, fn)` (`packages/ingest/src/adapters/databasePool.ts`). It adds an `error` listener for as long as `fn` holds the client and swallows the event: the rejected query already carries the same error to the retry path. On a failure it releases with the error, so the connection is **destroyed and replaced**, never reused: pg-pool removes a client released with an error and ends it (`pg-pool/index.js:392`, `_remove` at :172). A reused client would still have the stuck query queued ahead of the next one. `databasePool.test.ts` proves it against a server that never answers: after a query timeout the socket is closed and the pool is empty, and the next use opens a new connection. The break check `failed-client-reused` releases without the error and is caught. That also aborts the open transaction on the server, so there's no ROLLBACK: after a query timeout the connection is still waiting on the old query, and a ROLLBACK would only queue behind it.
  - **slow but alive:** `statement_timeout` = 30 s on ingest's connections (a connection option). Postgres cancels the statement cleanly (SQLSTATE 57014, now retryable), and the connection stays usable.
  - **silent:** `query_timeout` = 35 s on the client, for a database that can never answer. The driver gives up with `Query read timeout`, now retryable. It's longer than the statement timeout, so a live server cancels first.
  - **dead peer while idle:** `keepAlive: true` on the pool.
- **Why the timeouts are safe:** a timed-out INSERT may still commit on the server. The retry then meets `ON CONFLICT DO NOTHING` and counts as a duplicate, and no PUBACK goes out until a commit is confirmed, so idempotency (D4) covers it.
- **Why 30 s:** a 500-row batch commits in milliseconds, so 30 s fires only on something really wrong. IngestLagHigh (60 s) still alerts first, well before anyone would notice otherwise.
- **Audit of every connection in all packages:**
  - ingest `writeBatch`: the one `pool.connect()` in production code; now through `withClient`;
  - the api (queries, health job, command reconciler) uses only `pool.query()`, and pg-pool 3.14.0 guards that call's temporary client itself (`client.once('error', onError)`, `pg-pool/index.js:464`, removed at :469 when the query returns). Idle clients are covered by `pool.on("error")` in `packages/api/src/main.ts`;
  - the migration job (`packages/db/src/main.ts`) uses one `pg.Client` and now has a listener. A lost socket still fails the job, through its query, rather than as a stack trace from an unhandled event. Each migration is one transaction, and the advisory lock goes with the session;
  - the scenario helper uses only `pool.query()` and has a pool `error` handler (S7-9);
  - test setup code is the only other `pool.connect()`, against a container it owns.
- **The api's pool gets bounds too, lower than ingest's, and that deliberately changes the api's behaviour:** `statement_timeout` 10 s, `query_timeout` 15 s, `keepAlive`, and a 5 s connect timeout (`packages/api/src/databasePool.ts`).
  - **Why:** the health job runs every 15 s on a pool of five, and gaps scan all of a device's history (S5-9). A hung or runaway query held a slot forever. A few of them would freeze the fleet metrics, and the alerts built on them would go quiet, which is the worst way for monitoring to fail.
  - **The health job:** a part that times out stops the pass and is counted in `fleet_health_job_failures_total{part, reason}` (`timeout` or `error`). Its metrics keep their last values, and `fleet_health_job_part_last_success_timestamp_seconds{part}` says how old they are.
  - **Alerts:** HealthJobStale is now per part. This came out of this review: a gaps part that failed every time could hide behind the passes that skip it, which still stamp "last run", so stale values would pose as fresh. HealthJobFailing fires on two failures of a part in 5 minutes. Both have promtool tests and a runbook.
  - **HTTP:** a timeout answers 503, not 500 (`setErrorHandler` in `app.ts`); everything else still goes to Fastify's default handler.
  - **Proof:** `packages/api/test/databasePool.test.ts` (the options, the timeout classifier, the health job counting a 57014 as `timeout` and not stamping the part), an integration test for the 503 behind a held lock, and the break check `api-timeouts-removed`.
- **Proof:**
  - `postgresStore.integration.test.ts`: another session locks `devices`, so the batch blocks inside its transaction, and then its connection goes:
    - **dropped with no word from the server**, through a small TCP proxy in the test whose sockets are destroyed, which is what toxiproxy did. The batch must reject as retryable with no unhandled error. This is the test the break check needs;
    - **killed by `pg_terminate_backend`**: the batch rejects as retryable and the next batch works. This one passes *without* the listener too: the server sends its error before closing, so the query rejects and the client is released (with the error, so the pool destroys it). On release the pool attaches its own listener again, and that listener gets the later `error`. The crash needs a socket that ends with no error first.
    - A third test lowers `statement_timeout` to 300 ms behind the same lock and expects a retryable 57014, and a pool that still works;
  - `driverMessages.test.ts` gains a server that completes the startup handshake and then never answers a query: `Query read timeout`, retryable;
  - `databasePool.test.ts` pins the pool options;
  - break checks: `checked-out-client-unguarded` (remove the listener; the integration test catches it) and `database-timeouts-removed` (remove both timeouts; the options test catches it);
  - 3a-hang now also requires retries *during* the hang, and readings measured from 60 s after the toxic is removed with p95 < 2 s (behind the quiet-baseline gate, S7-7).
- **The hang runs** (2026-10-15, 3a with `DB_FAILURE=hang`, same tree, logs streamed to files):

| Build | Restarts | Retries during the hang | Stored | Prompt 60 s after (p95) | Verdict |
|---|---|---|---|---|---|
| all of S7-10 removed (no listener, no timeouts) | 2 (an unhandled `error` in each replica) | 0 | 94,088 of 94,088 | 0.31 s | FAIL 2 of 8 |
| only the listener removed | 0 | 16 | 73,456 of 73,456 | 2.34 s (run before the gate existed) | FAIL 1 of 8, lag only |
| S7-10 | 0 | 18 | 83,709 of 83,709 | 0.17 s | PASS |

  - **What each part proves:** under a hang, the query timeout alone prevents the crash: the stuck query gives up, the connection is destroyed, and no checked-out client is left holding a socket when the hang ends. The listener is for a connection that drops while a batch holds it (a plain cut, or a network drop), and the socket-drop test and `checked-out-client-unguarded` prove that part, not the scenario.
  - **Destroyed and replaced, in the run too:** in the S7-10 run, a `Query read timeout` was followed 5.4 s later by `Connection terminated due to connection timeout`. That's pg-pool opening a new connection through the still-hung proxy, not reusing the old one.

### S7-11. ⚑ One large table, on day one: fleet-wide reads of `readings` scanned the whole table.

- **Found by the first full demo (2026-10-15, after S7-10):** scenarios 2 and 8 failed because `/fleet` and the device-gaps endpoint answered 503, and the health job failed every pass. S7-10's 10 s API statement timeout cancelled them.
  - The timeout didn't make anything slower: before it, the same queries simply took 10–40 s, and nothing failed visibly. It made the slowness visible, which is what it was for.
  - The cause: `readings` had grown to 4.33M rows (1.1 GB) over one day of lab runs, and every fleet-wide read filtered on a time column with no index that starts with it. Each one was a full scan, and each got slower every hour. The HLD's Stage 2 "what breaks next: one large table" arrived in a lab, within a day.
- **Measured** (EXPLAIN ANALYZE, BUFFERS; 2,005 devices, 500 gateways delivering, CPU pressure 36–50%):

| query | stale statistics | after ANALYZE | with the covering index |
|---|---|---|---|
| fleet gap query, last hour (whole-table join) | 19.5 s, parallel seq scan | 11.7 s | 25.6 s (planner switched to 2,005 index-only loops) |
| one gateway, all time (explanation, S5-9) | 29.5 s, bitmap heap | 2.9 s | 54 ms, index-only |
| fleet, last hour, per device (lateral) | 60.5 s | **76.3 s** | 8.7 s, index-only |
| one gateway, all time, per device | 11.2 s | 3.2 s | **46 ms** |
| gap detection by aggregate (`max − min + 1 > count`), per device | | | 6.4 s |
| the same, fleet-wide | | 13.2 s, seq scan | |

  - **Statistics were stale:** the last autoanalyze was hours old with 233,294 rows changed, under the default 10% threshold of about 433k. For an append-only table whose timestamps only move forward, the histogram's last bucket falls behind the newest rows. ANALYZE alone halved the fleet query, and didn't fix it.
  - **A per-device rewrite alone made the fleet query 4× slower** (60–76 s). Each device's readings sit on hundreds of different heap pages, because the whole fleet's inserts interleave, so per-device index scans turned into random heap reads.
  - **A covering index fixes per-gateway and per-device reads** (46 ms against 3–29 s). It replaces `(device_id, measured_at)`, so the insert path keeps the same number of indexes.
  - **No query shape got the fleet's last hour under 2 s on this VM:** each has to read about 770k index entries (2,005 devices × about 382 readings an hour), 6–9 s however it's written. The cost grows with fleet size × window.
  - **Two measurement errors, corrected:** an early "147 rows" estimate came from a different EXPLAIN, without ANALYZE. A "PK order" variant looked index-only only because its outer query left out `measured_at`, so the planner dropped it; with that fixed, the planner took the covering index anyway.
- **The fixes:**
  - **Migration 006:** `autovacuum_analyze_scale_factor = 0.01` on readings. The covering index `(device_id, measured_at) INCLUDE (epoch, seq)` replaces the old one under the same name. It's built inside the migration's transaction, without CONCURRENTLY, which blocks writes while it builds. That's fine for the lab: 40.6 s on 4.33M rows. In production: `CREATE INDEX CONCURRENTLY` the new one, `DROP INDEX CONCURRENTLY` the old one, each outside a transaction, then rename.
  - **Per-device gap reads** (`gaps()`, a lateral subquery per device) for the per-gateway and per-device paths.
  - **Explanation only where it can matter:** the all-time read (S5-9) runs only for a gateway that has dropped readings *and* has a gap in the window. Every other gateway's gaps are unexplained by definition, so its history isn't read. In this lab 0 of 502 gateways had dropped anything. **This growth is honest:** the all-time read is still linear in the history of gateways that have dropped. A real fleet with flaky WAN has most gateways in that set, so this advantage shrinks over time, and that's the trigger for a per-gateway checkpoint rollup (an "explained through" cursor and the remaining budget, per gateway).
  - **Bounding explanation to 24 h was rejected:** `droppedTelemetry` is a lifetime counter, so a window-bounded history would spend old drops on new gaps (exactly S5-9's case, and its break check would catch it). S5-9's test and `gaps-explained-in-window` are unchanged and still CAUGHT.
  - **The fleet's last hour is now incremental (`core/gapWindow.ts`).** Each gaps pass reads only what was stored since its cursor (by `stored_at`, through its index), adds it to an in-memory window of the hour's readings per (device, epoch), drops what aged out, and reads the open gaps from memory. Database work per pass grows with the ingest rate, not with fleet × window.
    - **The cursor overlaps by 90 s.** `stored_at` is a transaction's *start*, so a batch that commits late carries an older `stored_at` than rows the cursor has passed. Each pass re-reads `stored_at > cursor − 90 s`, and re-read rows change nothing. That's only safe if transactions are bounded: ingest's connections now set Postgres 17 `transaction_timeout = 60 s` (25P04, retryable), and tests pin the two constants against each other.
    - **Clocks:** everything on the cursor path is the database's clock (the cursor starts at its `now()` and moves only by `stored_at`). The window's bounds compare with `measured_at`, the gateway's clock, using the API's `now` as the old query did.
    - **Exact, not approximate:** the window holds the hour's readings, not just the open gaps, because a gap at the window's left edge changes when its left reading ages out. A property test (300 runs) models the database: duplicates, replays, reordering, backfill, commits up to 60 s late, and ageing. At every pass the window must equal an oracle (`gapsOf`, the query's meaning written out). An integration test checks that oracle against the SQL on real data. The break checks `cursor-without-overlap` and `replay-does-not-close-gap` are both CAUGHT.
    - **Warm-up per gateway:** at start the window is filled one gateway at a time through the covering index (20–90 ms each, about 60 s for 500 gateways), so no single query reads the fleet's hour. Until then `/fleet` says `complete: false`, no gap figures are published, and `fleet_health_gaps_warm` is 0. Every part's last-success stamp starts at the job's start, so a warm-up that never finishes trips HealthJobStale.
    - **`/fleet` reads the job's last result,** with `asOf`, so it's at most one gaps pass (about 60 s) old.
    - **Its limits:** the window costs about 12 bytes a reading in memory (722,764 readings here, about 17 MB), and that grows with fleet × window. It lives in one API process: a second replica would hold its own copy and repeat the warm-up. Both are Stage 3 notes.
    - **Independent cross-check:** the window reported 10,004 missing readings in the hour, exactly the number scenario 7b lost (18:35–18:38).
  - **Three more whole-table scans, same cause:** clock skew (`received_at > now() − 5 min`, 4.5 s) and the ingest-lag figures in `/fleet` and `/gateways`. `received_at` has no index. Each now also bounds `stored_at > now() − (N + 1 min)`, which is indexed. `stored_at` is never earlier than `received_at` (a reading is received before its batch transaction starts), so no row is lost, and the extra minute covers clock differences between ingest and Postgres. Clock skew: 4.5 s → 0.46 s.
  - **The lab VM is slower than the Mac, and that mattered.** The same gaps pass took 0.86–1.25 s on the Mac and 3.4–9.7 s inside the API container, which burst to 165% CPU with GC churn. The step gauges `fleet_health_gaps_step_seconds{step}` and `fleet_health_gap_delta_rows` showed where it went. The delta now returns epoch milliseconds with no join (no `Date` parses) and pruning compacts in place.
- **Through the API, inside the VM** (CPU pressure 10–22%): gaps 0.63–1.18 s (read 0.33–0.99, window 0.18–0.32), clock skew 0.36–0.52 s, health 0.11–0.31 s, SLO ≤ 0.09 s, `/fleet` 0.37–0.77 s, `/gateways/:id` 0.27–0.55 s, device gaps 0.08–0.91 s.
- **The target, honestly: under 2 s in steady state under load, not in the first minutes after a cold start.** Each rerun starts from `compose down`: Postgres's cache is cold, 500 gateways reconnect, and the API's warm-up runs at the same time.

| rerun | verdict | health | clock skew | SLO | gaps | one warm-up read | batch write p95 / mean |
|---|---|---|---|---|---|---|---|
| 1 duplicate storm | PASS | 2.33 | 1.38 | 0.15 | 1.06 | 6.69 | 0.72 / 0.18 |
| 2 10% offline | PASS | 1.66 | 1.40 | 0.19 | 1.18 | 4.18 | 0.50 / 0.15 |
| 5 poison | PASS | 6.30 | 9.36 | 1.32 | never ran (warm-up unfinished) | 14.69 | 1.67 / 0.40 |
| 6 revocation | PASS | 5.78 | 2.15 | 0.34 | not reached | 1.28 | 4.40 / 1.08 |
| 7 kill -9 | INVALID | 10.48 | 2.59 | 0.87 | 8.13 | 8.17 | 1.38 / 0.33 |
| 7b down past the bound | PASS | 4.85 | 5.87 | 0.90 | 1.31 | 5.52 | 0.60 / 0.16 |
| 8 power loss | PASS | 0.80 | 1.33 | 0.07 | not reached | 0.60 | 0.62 / 0.17 |

  (Slowest pass of each part over the run, in seconds. 8's first attempt failed in 1 s with ECONNREFUSED because it expected a running stack; scenarios 6 and 8 now bring the stack up themselves. 7b settled its earlier "counted < lost": read after ingest-1 was back, counted 14,798 ≥ lost 14,548.)
  - **Not pool wait.** With Postgres logging every statement over 500 ms through a cold start, health's statements never appeared while the part took 5.3 s. The slow statements on the server were the warm-up's reads and ingest's inserts, and the API's event loop stalled for up to 3.4 s while minor GC took 26.2 s of 6 minutes. The warm-up holds one connection at a time, so it couldn't exhaust a pool of five.
  - **What was done:** the warm-up and the delta return epoch milliseconds with no copies, and the warm-up waits 50 ms between gateways and starts after the job's first pass. Each successful warm-up read counts as the gaps part being alive, so HealthJobStale fires for a warm-up that stalls, not one that is slow (before this, every restart under load would have raised it).
  - **After, over a cold start's first 6 minutes:** health peak 7.87 s (from 10.48), clock skew 4.16 s (from 9.36), gaps 2.53 s (from 8.13), one warm-up read 3.66 s (from 14.69), no failures (from one statement timeout), minor GC 17.7 s (from 26.2), API CPU 67 s (from 104). What remains is mostly Postgres reading a cold 1.1 GB table under load: warm-up reads up to 7.3 s, the clock-skew query 4.1 s and ingest inserts 4.5 s, each on the server.
  - **Scenario 7, gated on a warm API, after these changes: PASS (6 of 6).** The API reported its window warm 78 s after the stack came up. The quiet baseline followed 4 s later (p95 0.19 s). Readings measured after ingest-1 was back: p95 1.87 s. 64,875 of 64,875 stored; 132 redeliveries with DUP, 87 duplicates absorbed. Slowest passes in that run: health 3.50 s, clock skew 1.94 s, gaps 2.29 s.
  - **Open item: event-loop lag didn't move** (3.4 → 3.56 s at worst). The remaining GC is the window and delta work itself. The Stage 3 answer is to move gap work out of the API process, into its own worker or a rollup, not to tune it further here.
- **Insert cost of the covering index:** none measurable at steady load. Before (old index, 5 min steady): batch write p95 0.49 s, mean 0.18 s at 326 readings/s. After: 0.50 / 0.15 s at 285/s (scenario 2), 0.60 / 0.16 (7b), 0.62 / 0.17 at 400/s (8). The other reruns include duplicates, poison halving, a broker restart or a kill, and don't compare.
- **DBWriteErrors no longer fires on one blip.** It fired on a single retried connect timeout during a cold start, and an alert that fires on every restart teaches people to ignore it. It now needs at least 3 retries in 2 minutes, for a minute. Scenario 3a's real 120 s outage retried 31 times, and a cold start once. promtool tests cover both.
- **The final demo (2026-10-15, after the S7-11 fixes): 8 PASS / 3 FAIL / 0 INVALID**, on the most saturated VM of the day (CPU pressure avg60 median 45%, avg10 up to 86%). Every acknowledged reading was stored in every scenario that judged storage. The three FAILs were the VM:
  - **4** never reached its fault: fleet-sim's reconnect jitter (0–300 s) applies to a failed *first* connect too, and 3,020 CONNACK timeouts meant some gateways retried later than the fixed 3-minute setup budget. The budget now comes from fleet-sim's own settings (`fleetStartBudgetMs`: start-up 180 s + connect timeout + the jitter maximum, so 490 s for scenario 4), read from the module the simulator parses, so the two can't drift. First-connect jitter stays: a fleet powering on after an outage spreads out the same way.
  - **7 and 7b** stalled after a quiet baseline: within seconds of the fault the broker timed out 335 and then 367 simulated gateways for missed keepalives, CPU pressure was 64–86%, and ingest-2, which nothing touched, stored nothing for a 10 s window (7) and fell to 14/s (7b). No data was lost: 63,058 of 63,058 stored in 7, and counted 8,106 ≥ lost 7,856 in 7b.
  - So the stall rule above: the verdict line prints its evidence, e.g. `INVALID (... the VM stalled during the check: PSI avg10 86%, broker timed out 367 gateways, ingest-2 down to 14)`. Its truth table is unit-tested (`scripts/scenarios/stallRule.test.ts`): any one or two of the three signals change nothing, since an overloaded broker of our own would time gateways out too.
  - **Reruns with both changes (2026-10-16, 00:00–00:34):**
    - **4: PASS** (the derived 490 s budget).
    - **7b: INVALID (1 of 5), the rule working as meant.** "ingest-2 kept storing" printed its evidence (`PSI avg10 85%, broker timed out 299 gateways, ingest-2 down to 0`). Every data check passed: counted 2,269 ≥ lost 2,104, BrokerDroppingMessages fired, and ingest-1 received 383 msg/s once back.
    - **7: FAIL (2 of 6), and the rule rightly didn't excuse it:** CPU pressure peaked at 53% and the broker timed out no gateways, so only ingest-2's evidence was there. What it shows: received counts fell across the fleet near the end (101–552 per 10 s against about 2,000), and ingest-2 held one batch for its last 10 s window (0 inserted, 593 unacked, no retries): a fleet-side slowdown shorter than the keepalive timeout, plus one slow commit. Whether an autoanalyze (now every 1% of new rows, migration 006) was running then is unknown: the statistics counters were reset by the restart. All data checks passed: 56,728 of 56,728 stored, 0 dropped. It's recorded as an unexplained FAIL, not reclassified.
    - **The diagnostic run (00:35–00:41, Postgres logging every autovacuum and every statement over 500 ms; lab only, reset afterwards):** scenario 7 failed again on lag alone (1 of 6; fresh p95 4.23 s, 80,243 of 80,243 stored). The slowest stretch began at about 22:40:28 UTC, 55 s after the kill: an `INSERT INTO device_state` took 12.2 s and the lag queries 7.4–7.7 s. **Autoanalyze of `readings` didn't cause it.** It ran once in the whole run (8.73 s elapsed, of which 2.9 s CPU) and started at about 22:40:48, 20 s into the slow stretch. A time-based checkpoint started at 22:40:47, also after it. CPU pressure peaked at 54% and the broker timed out no gateways, so the stall rule rightly didn't apply either. 006's scale factor stays: once in 5.5 minutes, not every 2, because the counters start from zero at each restart, and in a long-running system it would be more often. Closed as unexplained, on the VM side.
- **A leftover task, checked and cleared.** A background shell job launched the day before (2026-10-14 17:09 UTC, during an earlier test) finished during these reruns: it had cut and restored the Postgres proxy right after launch, then only polled the alert log every 5 s for 29 hours. Its output's first line ("DBWriteErrors arrived") shows the cut and the restore both came before anything else. So it touched neither the demo nor the reruns, and the scenario 4 rerun that a first, wrong reading suggested (alert times are UTC, the runs local) was dropped. (The runner started it anyway, because its stop signal reached a log-follow subshell instead of the runner; it was killed 27 s into setup, before any fault, and its result discarded.) The runbook's "Running the demo" now says: one runner, no leftovers, and how to check.
- **Migration 006 while live:** 40.6 s. No fleet was sending (the rebuild also recreated fleet-sim), so it didn't demonstrate backpressure; to demo it, run `docker compose up -d migrate` with fleet-sim running and watch the broker queue. HealthJobFailing fired across the deploy, from the old API's timeouts before the rebuild plus one cold-cache timeout: expected deploy noise (runbook).
- **No time-budget break check.** One that restores the whole-table query and requires a test to fail a time budget would need millions of rows and a wall-clock limit, which would be flaky on this VM. The proof is the EXPLAIN numbers above, the oracle tests, and `gaps-across-epochs` and `gaps-explained-in-window` still being CAUGHT.
- **Stage 3's answer is time partitioning plus rollups,** not more indexes: each window touches one partition, retention is a DROP, and all-time history (gap explanation, SLO) comes from a rollup table.

### S7-12. ⚑ Clean reruns on an 8 vCPU / 32 GB VM: the laptop's FAILs were the laptop, and two checks were miscalibrated.

- **Why:** every timing so far came from a 4 GB Docker Desktop VM on a laptop, which the 500-gateway stack saturated on its own (S6-6; CPU pressure up to 86% in the last laptop demo). On that machine a FAIL could always be the environment. So the demo and the persistence comparison were rerun on a machine that isn't the limit.
- **The lab VM:** an Azure Standard_D8s_v5 (8 vCPU, 32 GB; Linux 6.17, Ubuntu 24.04), Docker 29.9.1 running natively, Node 24.22. Nothing else ran on it. CPU pressure ("some" avg60) averaged 0.5% through the whole demo and never passed 5.7%.
  - It's test infrastructure, not architecture: the design diagrams don't change.
  - **How it's used:** `scripts/lab-vm.sh` syncs the working tree (never `.git`, certificates, keys, `.env` or Terraform state), runs a command there, and pulls `demo-results/` back. The lab makes its own certificates; no CA key leaves the machine it was made on. Demos, scenarios and integration runs go there; the laptop is for editing, typecheck, lint and unit tests.
  - **Cost model:** billed only while running. It's deallocated between work sessions, an automatic nightly shutdown is the safety net, and nothing else runs in that Azure account.
- **The demo** (all of scripts/demo.sh, one run): **9 PASS / 2 FAIL / 0 INVALID**, against 8 / 3 / 0 in the last laptop run. Scenario 4 took 16 min (23–25 on the laptop).

| # | scenario | lab VM | last laptop run |
|---|---|---|---|
| 1 | duplicate storm | PASS | PASS |
| 2 | 10% offline | PASS | PASS |
| 3a | Postgres down 120 s | PASS | PASS |
| 3b | bound 5,000, down 60 s | PASS | PASS |
| 4 | reconnect storm | PASS | FAIL (setup budget, since fixed) |
| 5 | poison | **FAIL** (a setup race) → PASS after the fix | PASS |
| 6 | impersonation and revocation | PASS | PASS |
| 7 | kill -9 a replica | **FAIL** (a miscalibrated check) → PASS after the fix | FAIL (VM stall) |
| 7b | down past the bound | PASS | FAIL (VM stall) |
| 8 | power loss | PASS | PASS |
| 9 | the real gateway, WAN cut | PASS | PASS |

- **The two FAILs couldn't be blamed on the environment, and they weren't: the quiet machine exposed check bugs that the noisy one hid.**
  - **5 was a race in the scenarios' setup.** Scenario 4 leaves fleet-sim running with 300 s of reconnect jitter, so 5's `compose up` replaced it. `ensureFleet` counted "every gateway delivered in the last 15 s", which the *old* fleet-sim's readings satisfied, so it returned while the new one was still starting. 5's first call to fleet-sim's control API then met a closed port (5's log ended at 00:53:09 UTC; the new fleet-sim was running at 00:53:10). On the laptop everything was slow enough to hide it. **Fix:** ensureFleet counts only readings received after the call started and waits for fleet-sim's control API. A failed fetch now names its URL and cause instead of "fetch failed".
  - **7's "lag rose" was calibrated on the laptop.** The check also demanded that lag rise above 1 s while ingest-1 was down. On the VM it was back so quickly that lag peaked at 0.9 s, and readings after it returned had p95 0.04 s. **Fix:** the check judges recovery only. That the kill had an effect is what "restarted by Docker" and "redelivery happened (unacked at the kill, none lost)" already prove. A kill that lands with nothing in flight now makes the redelivery check INVALID, because it proves nothing either way.
  - **After the fixes, 5 and 7 each passed once on the VM.** 5: all five poison kinds dead-lettered, 66,316 of 66,316 stored, 400 readings/s before and during. 7: down 5.5 s, peak lag 5.9 s, p95 0.04 s once back, 26 unacked at the kill, none lost.
- **The persistence comparison, the clean version of S6-15/S7-7:** broker restart ×3, scenario 7 ×3 and 3b ×1 on each back end, `compose down` between runs, and the snapshot runs on a fresh volume (there's no migration between back ends, S6-14). **14 of 14 PASS.** Scenario 7 here ran before the check fix above, and passed anyway: lag rose past 1 s in every run.

| | persist-sqlite | snapshots |
|---|---|---|
| broker restart, lost | 0 of 21,016 / 21,000 / 21,032 | 0 of 20,948 / 20,959 / 20,958 |
| broker restart, all 500 back | 6 s, 6 s, 6 s | 6 s, 6 s, 6 s |
| 7, lost | 0 of 32,906 / 32,663 / 27,722 | 0 of 27,776 / 29,944 / 29,724 |
| 7, unacked at the kill / DUP | 18 / 44, 32 / 9, 35 / 21 | 17 / 39, 18 / 47, 39 / 40 |
| 7, peak lag while down | 6.5, 5.7, 3.7 s | 3.9, 4.1, 3.7 s |
| 7, p95 once back | 0.04, 0.04, 0.04 s | 0.04, 0.04, 0.04 s |
| 3b, lost ≤ counted | 16,060 ≤ 16,310 | 15,230 ≤ 15,730 |
| 3b, peak queue / drain | 14,057 / 28.2 s | 14,056 / 19.5 s |

- **What it settles:**
  - **No measurable difference between the back ends on a quiet machine.** The laptop's lag and DUP differences (S7-7) were noise. DUP is set on both, with no pattern (S7-7's "lower on sqlite" doesn't reproduce).
  - **The laptop's FAILs in 7 and 7b were the laptop.** Both pass on every run here, and the stall rule never had anything to excuse (no broker timeouts, CPU pressure under 1% at the kills).
  - **A clean broker restart lost nothing on snapshots either.** S7-8 expected that check to fail on the snapshot fallback. The broker log confirms snapshots were in use ("Saving in-memory database", "Restored 599 base messages"). On a quiet machine ingest re-subscribes in 250 ms, before the gateways' first readings after the restart arrive, so the S3-9 window exists but never opened. It's timing-dependent, not closed. So this check can't tell the back ends apart on a fast machine; the difference that matters is a broker crash, which the persist-sqlite canaries pin (S6-14).
  - **Still recorded:** 3b's status accounting stayed within ±300 on both (250 and 500 drops beyond the readings, against 167 and 421 statuses unwritten).
- **Still open after this:** nothing fails on the quiet machine. The cold-start peaks of S7-11 (health and clock-skew passes of 4–8 s on the laptop) weren't re-measured here; they were a laptop measurement, and the open item stands until they are.

## Slice 8: close the open items (2026-10-16)

### S8-1. Cold starts, re-measured on the lab VM: under 2 s, so the README item closes. One slow first pass slipped between scrapes.

- **Why:** S7-11's cold-start peaks (health 7.87 s, clock skew 4.16 s, 17.7 s of GC in 6 min) were measured on the laptop's 4 GB Docker VM, which S7-12 showed was the limit, not the system.
- **How:** the lab VM of S7-12, the same table size as the laptop (4,469,147 readings, seeded in 174 s). Three cold starts, each one `compose down`, the OS page cache dropped (`echo 3 > /proc/sys/vm/drop_caches`), then the stack and 500 gateways up, measured for 6 min. Postgres logged every statement over 500 ms.

| run | slowest part | /fleet (24 calls) | event loop max | GC (6 min) | API CPU | failures | Postgres > 500 ms |
|---|---|---|---|---|---|---|---|
| 1 | clock_skew 0.32 s | 0.10–0.25 s | 0.07 s | 0.19 s | 4.4 s | 0 | 14 |
| 2 | clock_skew 0.26 s | 0.10–0.11 s | 0.07 s | 0.15 s | 4.4 s | 0 | 0 |
| 3 | clock_skew 0.24 s | 0.10–0.11 s | 0.06 s | 0.19 s | 4.8 s | 0 | 2 (COMMIT 0.79 s) |

- **What it settles:** every part and every `/fleet` call was under 2 s on every run, so the laptop's peaks were the laptop. GC fell from 17.7 s to under 0.2 s.
- **The footnote, kept rather than averaged away:** run 1's Postgres log had **one clock-skew query at 6.13 s** and 12 COMMITs up to 1.53 s, but the API's gauge showed clock_skew at 0.32 s. That's a measurement gap: `fleet_health_job_pass_seconds` is a gauge of the *last* pass, scraped every 15 s, so a slow first pass overwritten before the next scrape is never seen. Run 1 came straight after the bulk seed, so a checkpoint flushing the seed and a visibility map not yet vacuumed are the likely causes; runs 2 and 3 have nothing over 0.8 s.
- **What changes:** the runbook gets "VACUUM ANALYZE after a bulk load or a restore". The health worker (S8-3) records pass time as a histogram, so a peak can't fall between scrapes again.

### S8-2. ⚑ `readings` partitioned by day on `measured_at`; the key gains `measured_at` (a D4 change), and readings older than retention are dead-lettered `too_old` (a D7 change).

- **Why:** S7-11's open item. `readings` was one table growing with fleet × time; retention would have been `DELETE` plus vacuum debt, and every window query planned against the whole table.
- **The key, decided 2026-10-16.** Postgres requires the partition key in every unique index, so partitioning changes the primary key whatever the choice:
  - **`stored_at` would break exactly-once.** A redelivery is stored later than the first copy, lands in another partition, and the per-partition key can't see the first copy: two rows.
  - **`measured_at` keeps it**, because a redelivery is the same bytes. The gateway serialises a reading once, when it's queued (`MqttUplinkService.cs:46-48`), stores those bytes as a BLOB (`LocalStore.cs:41`) and publishes them unchanged (`MqttUplinkService.cs:140`). Same `measured_at`, same partition, same key `(device_id, epoch, seq, measured_at)`: still stored once. The exactly-once property test now starts 15 s before midnight, so its duplicates cross a partition boundary.
  - **What changes:** a *different* reading that reuses a key with another timestamp (a producer bug, like S2-6's epoch reuse) was silently dropped before; now both rows are stored. Storing both and alerting beats a silent drop: both are real measurements, and a silent drop hid the bug. The detector lives in the health job's gap window: the same seq seen again with another `measured_at` counts `fleet_reading_key_reuse_total{gateway}` and fires `ReadingKeyReused`. It's bounded by the window, so a reuse more than an hour apart is stored without an alert; the runbook's query finds those.
- **The windows** (retention D days, A ahead, T = today 00:00 UTC):
  - partitions are kept for [T − D, T + A]; maintenance creates the missing days and drops what ends at or before T − D. Retention is `DROP TABLE`: instant, no vacuum debt.
  - ingest accepts readings measured from T − (D − 1). Anything older is dead-lettered `too_old`, before any write. One whole day of partitions lies between the two, so a reading ingest accepts always has a partition even when ingest and the maintenance job read the clock either side of midnight (a property test over the clock, the lag between them and D). Future readings are already bounded by the 5-minute skew rule (D7).
  - **No default partition.** A default would collect what doesn't fit, grow, and block the next `CREATE ... PARTITION OF` for any range it holds rows in. Instead a missing partition is impossible by construction, and the two checks that back that up both fail loudly: ingest refuses to start unless partitions cover its window (`assertPartitionsCover`), and Postgres's "no partition found" error is classified retryable, not poison, so a reading is never dead-lettered for an operator mistake; it waits, and `PartitionsRunningOut` pages.
  - **A retention floor of 4 days**, checked at startup by migrate, the maintenance job and ingest: the oldest reading a gateway can replay with one device online is 50,000 buffered × 5 s ≈ 69 h, plus a day of margin. With devices offline a buffer holds readings longer, without bound (S8-4), and those past retention are `too_old`. The default is a product decision: 30 days in production, 7 in the lab.
  - **`too_old` is data loss**, so unlike the other dead-letter reasons a single one alerts (`ReadingsTooOld`), with a runbook, and the README's guarantee says so. The raw payload stays in `dead_letters`.
- **The maintenance job** is a new `partitions` service (`db maintain`): one pass an hour, with metrics `fleet_partitions_ahead_days`, created/dropped counters and a last-success timestamp. Alerts: `PartitionsRunningOut` (fewer than 2 days ahead for 10 min, critical) and `PartitionMaintenanceStale` (no success for 3 h). Because a missing partition makes ingest retry rather than dead-letter, a dead maintenance job would end as an ingest stall with the backlog at the broker. These two alerts fire days before that: with 7 days ahead, `PartitionMaintenanceStale` fires 3 h after the job stops, `PartitionsRunningOut` about 5 days later, and inserts fail only when today's partition is missing, about 7 days after the job stopped. `db migrate` runs one pass too, so a fresh stack has its window before ingest starts.
- **Indexes:** the covering `(device_id, measured_at) INCLUDE (epoch, seq)` index of S7-11 is created on the parent, so every partition has it; an integration test checks each partition. 006's `autovacuum_analyze_scale_factor` can't be set on a partitioned parent and isn't needed: each day is a small table, analysed as it grows.
- **Measured on the lab VM** (4,470,000 readings, 2,000 devices over 31 h, `VACUUM ANALYZE` before each EXPLAIN, before and after 007):

| read | before 007 | after 007 |
|---|---|---|
| warm-up, one gateway's last hour (`gatewayReadingsSince`) | index-only, 0 heap fetches, 0.24 ms | index-only on today's partition, 0 heap fetches, 2 days pruned, 0.36 ms |
| gaps, one gateway's last hour | index-only, 0.52 ms | index-only, 9 partitions pruned at run time, 0.50 ms |
| gaps, one gateway's whole history (it has dropped readings) | index-only, 6.9 ms | index-only through each day's primary key, merged, 6.6 ms |
| the delta behind the cursor (`stored_at` > now − 2 min) | index scan, 0.06 ms | an index scan of every partition, 0.39 ms (planning 5.4 ms) |

  - The last row is the one cost partitioning adds: `stored_at` isn't the partition key, so the cursor read can't prune and probes each partition's `stored_at` index, one probe per kept day (37 in production). It stays well under a millisecond and is read every pass. A bound on `measured_at` would prune it, but would miss a backfill measured long ago and stored now, which is exactly what the cursor exists to catch, so it's left as is.
  - The empty days ahead show as sequential scans of zero rows: they cost nothing until they have rows, and then they're analysed and indexed like the rest.
- **The migration (007)** renames the old table, creates the partitioned one with partitions for every day its rows need plus a week around today, copies, and drops the old table, in the migration's transaction. That blocks writes while it runs. On the lab VM, 4,470,000 rows (1.3 GB with indexes) took **93 s**, into 10 partitions, every row kept. Fine for the lab. **In production, stage it:** create the partitioned table beside the old one, copy in batches by day while ingest writes to both (or note the copy's high-water `stored_at` and copy the tail last), switch the names in one short transaction, and drop the old table once the switch has run a day.
- **A trap found on the way:** ingest pre-creates every dead-letter reason's counter series at 0, because a series that first appears at 1 has no `increase()` for an alert to see. The list was hand-written and didn't have `too_old`, so the first too-old reading would never have fired `ReadingsTooOld`. It's now a `Record<DeadLetterReason, true>`, so a reason left out fails the typecheck. The key-reuse counter is created at 0 per gateway for the same reason.
- **Break checks:** each of these turns a test red, then is reverted: too-old routing removed; "no partition" classified poison; `acceptedSince` off by a day (both the window property test and ingest's boundary test fail); the key-reuse detector removed; a dead-letter reason left out of the initialised list (the typecheck fails). On the lab VM: the integration suite passed, after two fixture mistakes in the new partition test were corrected (300 rows at 10-min steps ran into a fourth day, and the redelivery used a timestamp no row had). All 27 break checks were CAUGHT, and the promtool rule tests passed (27 rules).

### S8-3. ⚑ The health job moves out of the API into a health worker, the single writer of the fleet metrics and of a snapshot `/fleet` reads.

- **Why:** S7-11 left the health job and its gap window inside every API process: each replica warmed its own window (about 1.4M rows read at start), exported its own copy of every fleet counter, and competed with requests for one event loop. Two API replicas would have doubled the SLO counters, which the burn-rate alerts sum across targets. S8-1 showed the cold start was fast enough on a real machine, so this is about ownership, not speed.
- **What moved:** `packages/api/src/workerMain.ts` is a composition root of its own, run as the `health-worker` service. It runs the same `startHealthJob`, unchanged in what it computes, and after each gaps pass writes the result to `health_snapshot` (migration 008), one row. The API no longer starts the job: `/fleet` reads the row, and reports its `asOf` plus `ageSeconds`, so a stopped worker shows as an ageing figure, not a wrong one. Before the first gaps pass, `complete: false`, as before. An incomplete pass never overwrites a snapshot. API replicas are now stateless and start in seconds.
- **Single writer by construction, not by convention:** the worker takes a Postgres session advisory lock (`pg_try_advisory_lock`, key `0x6865616c`) on a connection of its own before it starts the job. A second worker waits on standby, trying every 5 s, and exports `fleet_health_worker_leader` = 0 and no fleet metrics. The lock dies with its session, so the leader checks every 5 s that its session still answers and still holds the lock. If not, it exits with 1, and Docker restarts it on standby. A network cut shows up as that check timing out (10 s), so a worker runs without the lock for at most one check plus the timeout. Why not a Compose `replicas: 1` alone: that's a promise about one deployment file. The lock holds on any orchestrator and during a rolling deploy, when old and new overlap.
- **A lock alone doesn't make a single writer, so the snapshot write is fenced.** The leader learns it lost the lock only at its next check, up to 5 s later, and a pass can be in flight across that: a paused process, or a network cut that Postgres notices first. Meanwhile the standby takes the lock and writes a newer snapshot, and the old leader's stale result, written through the pool, would overwrite it. So the write runs on the lock connection, and the statement itself writes only `WHERE EXISTS` a granted advisory lock for its own `pg_backend_pid()`. A zombie's write affects 0 rows, whichever connection it goes through (and on the dead lock connection it fails outright). A refused write counts `fleet_health_snapshot_fenced_total` and makes the worker exit at once, without waiting for the next check. This is a fencing token in its simplest form: the token is the lock-holding session itself, checked by the storage at write time. A generation number (`UPDATE … WHERE generation <= $mine`) would be the general form, needed when the writer and the lock can't be checked in one statement, e.g. a write to another system.
  - **What isn't fenced:** a zombie's `/metrics`. Until its next check it keeps exporting, so for up to 5 s plus one pass both workers count the same readings into the SLO counters and `fleet_health_worker_leader` sums to 2. That's bounded, rare (it needs a takeover), and scenario 10 measures it. Fencing Prometheus counters would need the counts themselves stored in Postgres; not worth it at this size.
- **Seeing it:** `HealthWorkerNotRunning` (critical) fires when no target reports `fleet_health_worker_leader == 1` for 2 min, which also covers the worker's target vanishing. HealthJobStale can't see that case: when the worker's series disappear, its alert has nothing to evaluate. Every fleet alert depends on the worker, so it pages.
- **The pass histogram (S8-1's follow-up):** `fleet_health_job_pass_duration_seconds{part}` records every pass, failed ones included, in buckets up to 30 s, beside the last-pass gauge. A pass slower than its neighbours can no longer fall between two scrapes, which is how S8-1's 6.13 s first pass went unseen. `HealthJobPassSlow` (warning) fires when one part takes over 2 s at least 3 times in 15 min: a single slow pass after a bulk load is visible in the histogram but doesn't page.
- **The warm-up reads only the current partitions:** it reads each gateway's last hour by `measured_at`, and S8-2's EXPLAIN shows that read pruned to today's partition, index-only. Gap explanation for a gateway that has dropped readings still reads its whole history, every partition; that's S8-4.
- **What stays the same:** every metric name, so the dashboard and every alert expression work across the switch. Only the scrape target changes (`health-worker:9464`; the API's target now has process metrics only). HealthJobStale and HealthJobFailing name the worker. The runbooks gain "Health worker not running" and "Health job pass slow".
- **Trade-offs:**
  - One more service to run and watch. In exchange the API scales out without multiplying the fleet metrics or the warm-up.
  - `/fleet`'s open gaps are a database read instead of a memory read: one primary-key lookup.
  - A standby worker doesn't keep a warm window. A takeover warms from the database, about a minute at 500 gateways, during which `/fleet` shows the last snapshot with its age.
- **Break checks:** the lock always granted (the lock test fails: a second session takes it); the leader never re-checking its lock; the snapshot never published (the API test that compares the snapshot with the job's last pass fails); the histogram not observed; the fence removed (`zombie-leader-writes`: a leader paused mid-pass, its session lost, a new leader writing, then the old one resuming must leave the new snapshot in place; without the fence its stale pass overwrites it).
- **On the lab VM, 500 gateways:**
  - the integration suite passed (73 tests); all 32 break checks CAUGHT; promtool passed (29 rules);
  - scenarios 2 (10% offline), 7 (kill -9 an ingest replica) and 8 (power loss), which read fleet health from the worker now, all PASS;
  - **scenario 10 (new, kill the leading worker):** PASS on the fourth run. The standby took the lock 0.1 s after the kill (5.1 s in another run, when the kill fell just after its retry), `/fleet` advanced again after 62 s, written by the new leader (its warm-up), Prometheus showed one leader again after 12 s, the lock was never held by two sessions, and the killed worker, started again, came back on standby.
- **What the first three runs of scenario 10 found:**
  - **A real bug: Prometheus scraped one replica.** The static target `health-worker:9464` resolves to one of the replicas, so with a standby running Prometheus saw only the standby ("0 leaders of 1 worker"), and `HealthWorkerNotRunning` would have fired with a healthy leader. The job now uses DNS service discovery and scrapes every replica.
  - **A crashed leader usually takes its own lock back:** Docker restarts the container in under a second, before the standby's next 5 s try. That's correct (a free lock, first come), but it isn't a failover. So the scenario now removes the leader for good (`docker kill`, which Docker doesn't restart) and starts it again afterwards.
  - **Stale samples in the scenario's own queries:** Prometheus keeps its data across `compose down`, and after its restart a previous run's series stay current for the 5-min lookback, so the scenario counted a worker that no longer existed. Its queries, and `waitForGapsWarm` (used by scenario 7 too), now count only samples scraped in the last 25 s. A dead worker's last sample also lingers until its next failed scrape, so "one leader" is judged by `pg_locks`, and the Prometheus sum only has to settle within 30 s.
  - **The same lag in the alert:** after a Prometheus restart, a stale `leader = 1` from a worker that died meanwhile can hide its absence from `HealthWorkerNotRunning` for up to 5 min. Accepted: it needs Prometheus and the worker to restart together.
- **Not part of this change:** the integration suite's first run had one broker-security test fail. The broker logged "running" a moment before its mapped port completed a TLS handshake, and the test's first client was dropped mid-handshake. Its setup now waits for one successful handshake (10 s), and the rerun passed.

### S8-4. ⚑ Gap explanation resumes from an exact per-gateway checkpoint, recomputed whenever stored history could have changed under it.

- **Why:** S7-11's open item. `droppedTelemetry` is a lifetime counter, so explaining the open gaps of the last hour means explaining every gap the gateway ever had, oldest first (S5-9). For a gateway that has dropped readings and has a gap now, each 60 s gaps pass read its whole stored history, so its cost grew with that history: **1.1 s per pass for one gateway with 30 days (2.07M readings)** on the lab VM, every minute, per such gateway.
- **The checkpoint** (`gap_checkpoints`, migration 009, one row per gateway): T (`explained_through`), the explanation state after every gap that starts before T (`budget_left`, `stopped`), the counter it was computed with, the oldest kept partition then, and the database time up to which stored readings were checked against it. A pass reads only the gaps from T on, resumes the explanation from the stored state, and moves T to the start of its window. `explainGaps` became resumable (`explainFrom`) with a total order: `before`, then device, epoch and seq, because a checkpoint and a from-scratch run must take tied gaps in the same order.
- **Exact, by invalidation (decided before building it).** The buffer doesn't bound how old a replay can be, so there's no horizon after which history is settled. Instead the gateway recomputes from scratch when anything that could change a gap before T happened:
  - `droppedTelemetry` changed, or retention moved the oldest partition ("all-time" means all stored history, within retention);
  - a reading stored since the last check was **measured before T** (`measured_behind`);
  - or its **predecessor in its series**, by (seq, measured_at), **was measured before T** (`gap_behind`).
  - **Why those two, and why the first rule considered wasn't enough:** it was "a fill measured at or before T". A replay can fill a gap that straddles T, from a reading before T to one after, with a reading measured *after* T: the time rule misses it, and the gap was already folded. A device silent for longer than the window comes back with a gap that starts before T. A gateway clock that steps back defeats any rule on time alone. The predecessor rule covers all three: a gap is folded only if its reading-before was measured before T, so whatever lands inside it, the lowest new reading has that reading as its predecessor. Both rules are judged on what's stored when the pass checks, never on arrival order, because `stored_at` is a transaction's start and can't order arrivals exactly. The rows checked are those stored since the last check less the cursor overlap (S7-11): re-checking a row costs at most a needless recompute.
  - **Reading by seq, not by time:** the gaps from T are read per series from its first reading measured at or after T, onward by seq through the primary key. After a clock steps back, a pair whose second reading is older than T is still read.
  - **Stale checkpoints** (not checked for 15 min, a gateway whose gaps closed) are recomputed rather than checked, and so is a **backlog**: past 2,000 readings stored since the last check, checking each one (about 0.33 ms, a predecessor lookup in every partition) would cost more than a recompute. That happens once when a gateway drains its buffer after an outage.
- **The proof:**
  - **A unit oracle property test:** 3000 random histories of in-order appends, gap fills, keys reused with other timestamps, readings from the future and from far behind the window, counter increases, retention cuts and passes. Every pass must equal explaining all stored history from scratch, and the checkpoint must actually be resumed (in more than 15% of passes).
  - **The first generator was too weak, and the test said so only when asked:** with random seqs, gaps were far larger than any drop counter, the explanation stopped at the first gap, and the test still passed with *every* invalidation rule removed. Gaps are now generated at the counter's scale, and removing each rule was checked to fail. Retention and `measured_behind` need histories random generation rarely builds, so five fixed histories, each built so that without its rule the *result* differs, run in both the unit and the Postgres test.
  - **On Postgres:** random histories stored as real rows in partitioned `readings`. The SQL gaps must equal the model's for all history and from T, and every pass through the real `gap_checkpoints` must equal the oracle and the reference `gapsInWindow`.
  - **The Postgres test was blind too, the first time:** the break checks removing each SQL rule went UNNOTICED. A whole test history was stored within a few seconds, so inside the 90 s cursor overlap every pass re-checked every row, found an old reading, and recomputed: correct, but the rules under test never decided anything. The test now stores each reading with its own `stored_at` (a second apart, and 100 s before each pass), and all five break checks are caught.
- **A difference it exposed, kept:** for a gateway with no drops, the window-based paths (the health job's in-memory window, and `/devices/:id/gaps`) read only readings measured in the window. After a gateway clock steps back, a pair whose second reading is older than the window is invisible to them, while the checkpoint's by-seq read finds it. Nothing is explained with no drops either way, so only the list of open gaps differs, and only across a clock step. Found by the Postgres oracle, which compares with the reference only when the counter is above zero, when the reference reads all history.
- **Measured on the lab VM** (one gateway, 4 devices, 30 days, 2,071,524 readings, a gap every 997):

| pass | time |
|---|---|
| from scratch (the old cost of every pass) | 1,056–1,090 ms |
| resumed, nothing new | 15–18 ms |
| resumed, 120 new readings checked (a normal minute) | 55 ms |
| 2,000+ new readings since the last check | recomputed as backlog, 1,058–1,061 ms |

- **What uses it:** the health worker, for each gateway with drops and an open gap (`fleet_gap_checkpoint_total{outcome}`: resumed, or recomputed and why). `/devices/:id/gaps`, an on-demand read for any window, keeps the from-scratch path, which is also the reference the checkpoint is tested against.
- **Trade-offs:** a recompute costs what every pass cost before; it now happens only when history changed behind the window, e.g. once when a gateway comes back from a long outage and replays. The checkpoint is written by the worker alone, without fencing: a zombie's checkpoint is still self-consistent for the T and verification time it carries, so the worst it can cause is a recompute.
- **Break checks:** the drop-counter and retention rules removed (unit); each of the two SQL rules removed, and the gaps from T read by time instead of by seq (Postgres). On the lab VM: the integration suite passed (79 tests) and every break check was caught (37: 35 in the full run, and the two SQL-rule checks after the storage-clock fix).

### S8-5. ⚑ Revocation closes the gateway's open connection without a broker restart (a D6 change), through the dynamic security plugin.

- **Why:** S4-3's open item. Mosquitto checks the CRL only at the TLS handshake, so a revoked gateway's open connection kept publishing until a broker restart dropped every connection.
- **The mechanism, checked on 2.1.2 first:**
  - **Dynsec alongside the ACL file.** The image ships `mosquitto_dynamic_security.so`; the broker module of `mosquitto_ctrl` has no per-client disconnect, so dynsec's `disableClient`, which kicks a connected client, is the only supported way.
  - **The trap: either plugin can grant.** In 2.1, with `acl_file` and dynsec both loaded, access is allowed if *either* allows it. With dynsec's defaults set to allow, a gateway could publish on another gateway's topic, which the ACL file denies. So dynsec's defaults are all **deny** (publishClientReceive too, stricter than the default), gateways are registered with no roles, and the only grant in dynsec is `$CONTROL/dynamic-security/#` for `dynsec-admin`. The ACL file stays authoritative: every ACL test passes with dynsec loaded, and ingest still receives everything (scenario 1).
  - **The second trap: a kick isn't a lockout.** With `use_identity_as_username` the password check never runs, so dynsec's "disabled" isn't consulted at connect, and a kicked gateway reconnects at once on a valid certificate. Only the CRL keeps it out.
- **So the procedure is ordered, and the order is the guarantee** (`scripts/gateway-admin.ts revoke`, steps in `scripts/gatewayAdmin.ts`):
  1. `pki.sh revoke`, which regenerates the CRL;
  2. SIGHUP;
  3. poll a handshake with the gateway's own certificate until it's refused **as revoked**; a refusal for any other reason (the broker down) doesn't count;
  4. only then `disableClient`.

  If step 3 doesn't see it within 10 s, it fails without kicking: a kick then would look like success while the gateway walked straight back in. A unit test checks the order; a break check moves the kick before the confirmation and is caught.
- **Enrolment** (`gateway-admin.ts enrol`) issues the certificate and registers the dynsec client in one step, so every gateway can be kicked. A gateway enrolled before S8-5 is registered when it's revoked.
- **State to back up:** `/mosquitto/data/dynamic-security.json`, seeded from `deploy/mosquitto/dynamic-security.seed.json` on a first start (Compose copies it in after the entrypoint's chown and chowns it). An integration test backs it up from one broker and starts another on it: the admin works and the disabled gateway is still disabled. If it's lost, the seed comes back, gateways still connect (their access is the ACL file's), and revocation still works because the CRL is what refuses.
- **The admin's key:** `dynsec-admin` is issued by the lab CA and kept in `deploy/admin/certs`, which no container mounts and `deploy.sh` doesn't copy. Its blast radius: it can kick anyone, and because either plugin can grant, a role it adds could open topics the ACL file denies. That's less than the CA key, which can mint any identity, and it's kept the same way.
- **Measured on the lab VM** (scenario 6, 500 gateways): the CRL reload was confirmed on the first probe, 0.1 s after the start; the kick was sent at 0.1 s and the gateway's connection closed 0.15 s later, before the admin's command had returned; 0.3 s in all. A new handshake was refused as revoked, and nothing from it was stored after the kick. Scenario 6's KNOWN LIMIT is now a PASS check, with a budget of 5 s.
- **The CA key, which every service could read, now reaches none (part of S8-5, its own commit).** Review found that `deploy/certs`, CA key included, was mounted whole into the API, both ingest replicas, the broker exporter and fleet-sim, and the broker. Anyone with code execution in any of them could have minted a certificate for any gateway, or for `dynsec-admin`. Now:
  - each service mounts only its own certificate and key, `ca.crt`, and (the broker) `crl.pem`. The CRL is mounted as a file, which works because `pki.sh` rewrites it in place;
  - fleet-sim's gateways are issued into `deploy/certs/fleet` (`PKI_OUT`), with a copy of `ca.crt`, and fleet-sim mounts that directory only;
  - `ca.key` is mode 600 and stays where `pki.sh` runs (the operator's machine). It's mounted into nothing;
  - `deploy.sh` already copied an explicit list without `ca.key`. It now also excludes `deploy/admin`, which its `rsync` of `deploy/` would otherwise have copied (the dynsec admin key, new in this slice). So the always-on server (S8-6) gets neither key;
  - a guard test fails if any Compose file mounts the certificate directory whole, `ca.key`, or the admin directory, if the fleet's directory would hold CA files, or if `deploy.sh` would ship either key. A break check (`ca-key-mounted`) restores the old mount and is caught.
  - **On the lab VM, after the change:** a search of every service container's filesystem found no `ca.key`; each one held only its own certificate and key and `ca.crt` (the broker also `crl.pem`, fleet-sim its 500 gateways'). Scenarios 6, 1 and 2 passed, with fleet-sim's 500 gateways connecting from the new directory.
- **Threat list (S4-4), amended:**

| Threat | What stops it, or what's accepted |
|---|---|
| **CA key exposure → certificate minting** | Whoever holds `ca.key` can issue a certificate for any gateway (impersonation that the ACL and device binding can't tell apart from the real one), for `dynsec-admin` (change who may publish), or for `ingest-1` (read every reading). **Mitigation:** it's on the operator's machine only, mode 600, mounted into no container and never deployed, and a test guards each of those. A compromised service container exposes that service's own key, which revocation handles. **Residual risk, accepted for a lab:** the key is unencrypted on one machine, and the CA is also the root (no intermediate). A real fleet keeps the root offline or in an HSM, issues from an intermediate it can revoke, and enrols devices at manufacturing. |
| **The dynsec admin key** | It can kick any client and, because either plugin can grant, add a role that opens topics. Kept like the CA key: `deploy/admin/certs`, mounted nowhere, never deployed. Less powerful than the CA key: it can't create identities. |

- **Tests:** on a real broker with the repo's config: a kick alone lets the gateway straight back in (the trap, pinned); enrol then revoke closes the open connection within 5 s of the kick and refuses the reconnect as revoked; backup and restore. On the lab VM the integration suite passed (82 tests) and all 39 break checks were caught, including `kick-before-crl-confirmed` and `dynsec-defaults-allow` (dynsec's default send set to allow: the ACL test that a gateway can't publish as another fails).

### S8-6. ⚑ Always-on on Azure (a D8 change): one Standard_B2pls_v2 VM, sized by measurement, Terraform with azurerm.

- **Why:** D8 targeted a Hetzner VM, plan-only. The deployment moves to Azure, inside the existing credit, with a spend target of ≤ $30/month and always on (no nightly shutdown). Applying it, and every Azure operation, is done by the subscription's operator, never by the build.
- **Prices** (Azure retail prices API, queried 2026-10-16; Linux pay-as-you-go, USD, 730 h; Germany West Central and West Europe identical). Each total adds a 32 GiB Standard SSD (E4, $2.40) and a static Standard IPv4 ($3.65). Egress is negligible, under the free 100 GB/month:

| size | vCPU / RAM | VM | total | |
|---|---|---|---|---|
| B2ats_v2 | 2 / 1 GiB | $7.88 | $13.93 | below the build's 0.94 GiB |
| B1ms | 1 / 2 GiB | $17.52 | $23.57 | Bv1: End of Life, announced retirement |
| **B2pls_v2** (Arm) | 2 / 4 GiB | $28.03 | **$34.08** | the cheapest current 4 GiB size |
| B2als_v2 | 2 / 4 GiB | $31.54 | $37.59 | |
| B2s | 2 / 4 GiB | $35.04 | $41.09 | Bv1: End of Life |

  Microsoft lists Bv1 (B1ms, B2s) as End of Life and says to use current sizes for new deployments. The current burstable families (Basv2, Bsv2, Bpsv2) go from 1 GiB straight to 4 GiB. So nothing current fits under $30, and B2pls_v2 is $4.08 over the target, inside the credit. That's a decision for the person who owns the spend, not the build: put to them with this measurement, and **approved on 2026-10-16: Standard_B2pls_v2 at $34.08/month**, chosen over running it only while testing, or not deploying.
- **The measurement (sizing harness `scripts/sizing/`, on the lab VM):** the production profile's 15 cloud services in one systemd slice capped like the target VM (MemoryMax, no swap, CPUQuota), the real gateway with its simulated home and 10 simulated gateways outside it as load, sampled every 5 s through five phases: a from-scratch build of the Node image inside the cap (BuildKit in a container in the slice), a cold start with the page cache dropped, 20 min steady, a redeploy rebuilding the image while the stack runs, and a second cold start.

| phase | 1 vCPU / 2 GiB | 2 vCPU / 4 GiB |
|---|---|---|
| build | 934 MB max, 29% CPU | 937 MB, 29% |
| cold start | 2,047 MB, 25% | 3,083 MB, 26% |
| steady 20 min | 2,044 MB, **11.0%** | 3,046 MB, **11.1%** |
| redeploy | 2,047 MB, 27% | 3,535 MB, 25% |
| second cold start | 2,047 MB, 27% | 2,897 MB, 25% |

  - **These figures are `memory.current`, which includes page cache,** and cache fills whatever the cap allows. So a run at 2,047 of 2,048 MB looked like a box at its limit when most of it was reclaimable cache. The working set is the anonymous memory, sampled separately (max / median MB, and memory pressure "full", the share of time every task stalled on memory):

| phase | under 2 GiB | under 4 GiB |
|---|---|---|
| cold start | 989 / 984, pressure 0.02% | 1,023 / 982, 0 |
| steady | 960 / 899, 0 | 1,013 / 926, 0 |
| redeploy | **1,400** / 963, **0.66%** | 976 / 916, 0 |
| second cold start | 1,025 / 997, **1.47%** | 930 / 909, 0 |

    The working set is about 0.9–1.0 GiB and reaches 1.4 GiB in a redeploy. Under 2 GiB that's where real memory stalls appear; under 4 GiB there are none in any phase. The services' own sizes: Grafana 365 MB, cAdvisor 235, the health worker 205, Prometheus 132, Postgres 111, the rest under 75 each. **No OOM kill in any phase of either run**, and every service was up at the end.
  - **CPU, against the credit model:** steady 0.11 vCPU. B2pls_v2's baseline is 30% of 2 vCPU = 0.6 vCPU, banking 36 credits/h (one credit is one vCPU-minute at 100%) up to 864. At 0.11 it banks about 29 a hour net and fills the bank in about 29 h. Cold starts and redeploys average 0.25–0.27 vCPU, still under the baseline. So it never throttles in steady state; it can't run its credits down, because it never spends them. The measurement was on the lab VM's x86 cores; at half the speed on Ampere it would still bank about 23 an hour.
  - **Health over the 20 min steady phase, both caps:** ingest lag p95 0.048 s (max bucket 0.1 s under 2 GiB, 0.05 s under 4 GiB); 10,560 readings stored (8.8/s: the real gateway and 10 simulated ones); health-job pass p99 0.09 s and 0.05 s; no database retries, no broker drops, no health-job failures, no container restarts.
  - **The image is built on the box.** The build peaks at about 0.94 GiB for about 35 s and fits beside the running stack. Building in CI and pulling from a registry would matter only on a 2 GiB box, and would need an arm64 build in CI; it's the path if the box ever gets tight.
  - **Two harness bugs, found and fixed:** systemd reads a dash in a slice name as hierarchy (`fleet-sizing.slice` lives under `fleet.slice`), and buildx's docker-container driver ignored its cgroup-parent option, which would have built outside the cap. The harness now uses `fleetsizing.slice` and runs BuildKit itself in the slice as a remote builder, after checking that its process's cgroup is inside. And **a silent `|| true` hid a failed measurement:** `curl` took `[15m]` in a raw URL as a glob range, failed, and with `-s` and `|| true` wrote empty files that looked like a run with no data. The queries now go through `--data-urlencode` and fail loudly, and `scripts/sizing/health.sh` recovered the verdict from Prometheus's own data, which `compose down` keeps.
- **The production profile, trimmed for one small box:** toxiproxy (the lab's chaos proxy) doesn't run, and ingest reaches Postgres directly; Prometheus keeps 7 days. **Both ingest replicas stay:** failover is the design. One replica would save about 68 MB, which doesn't change the size.
- **Arm64:** every image in the production profile publishes linux/arm64, checked from their manifests (caddy, eclipse-mosquitto, cadvisor, grafana, postgres, alertmanager, prometheus, node:24.21.0-slim). The module picks Ubuntu 24.04's arm64 image for an Arm size.
- **One name, no domain:** the public IP's DNS label (`<label>.germanywestcentral.cloudapp.azure.com`). Caddy gets a Let's Encrypt certificate for it and serves the API at `/` and Grafana under `/grafana/` (Grafana's own sub-path serving). That replaces the two sslip.io names, so there's no dependency on a third party's DNS. Gateways connect to the same name on 8883.
- **Terraform** (`deploy/terraform`, azurerm 4.14.0), 9 resources in one resource group (`terraform destroy` removes everything):
  - a VNet and subnet;
  - an NSG allowing 22 from the operator's ranges only (validated: never 0.0.0.0/0), and 80, 443 and 8883 from the Internet; everything else is denied;
  - a static Standard public IP with the DNS label, and a NIC;
  - the VM: Ubuntu 24.04, Standard SSD, cloud-init (Docker, the `fleet` user in its group), password login off, no auto-shutdown.

  `vm_size` has no default, because it's what the approval is about. The subscription is passed explicitly, never taken from the CLI's default account. The plan is run and applied by the subscription's operator. The Hetzner module is removed rather than kept beside it, so no unused provider drifts.
- **deploy.sh** takes the VM's name, copies the repo without `deploy/certs` and `deploy/admin`, then only the service certificates by name, never the CA key, the dynsec admin key or the simulated gateways' keys (S8-5).
- **Revocation on the VM, a gap found before the first deploy:** S8-5's procedure reloaded the CRL through the *local* Compose stack, so on the VM the new CRL would never have reached its broker, and the confirmation step would have failed loudly (correctly, but with no way through). With `DEPLOY_HOST=fleet@<fqdn>`, `gateway-admin.ts` now runs Compose on the VM over SSH, writes the new CRL into the VM's copy **in place**, and reaches the broker at its public name with the server name `mosquitto`. The CA key stays on the operator's machine; only the CRL travels. "In place" matters, and `deploy.sh` now copies with `rsync --inplace` for the same reason: the broker mounts `crl.pem` as a single file, so a replaced file (a new inode) would go unseen by the running container until it was recreated.
- **The smoke check** (`scripts/smoke.ts`, run from outside after each deploy): HTTPS with a publicly trusted certificate and `/health/ready`; the API refuses `/fleet` without its token; Grafana under `/grafana/` refuses its API without a login; MQTT refuses a client without a certificate, accepts a lab-CA gateway certificate, and a reading published over 8883 is stored and served by the API; and Postgres, Prometheus, Alertmanager, Grafana's, the API's and the metrics ports are all closed from outside.

### S8-7. ⚑ The CRL's lifetime is watched and renewed: an always-on broker meets it a month after the first deploy.

- **Why:** the CRL is signed for 30 days (`default_crl_days`). After its `nextUpdate`, Mosquitto refuses every client certificate, so the whole fleet, ingest included, is locked out at once. No lab ran long enough to meet it; the always-on VM (S8-6) would have, on 2026-11-14.
- **Watching it:** the broker exporter mounts the CRL the broker loads (public data; a single-file mount, rewritten in place like the broker's), reads its `thisUpdate` and `nextUpdate` every minute with a small DER parser, and exports them. The parser is tested against what `openssl` reports for a CRL from the repo's own `pki.sh`, and a break check (`crl-expiry-misread`) is caught. An unreadable CRL exports an expiry of 0: it fails closed.
- **Alerts:** `CrlExpiringSoon` (warning, under 7 days left, or unreadable) and `CrlExpiring` (critical, under 2 days), both with promtool tests and a runbook entry. 24 alerts now.
- **Renewing it:** `pki.sh crl` re-signs the same list with new dates. `gateway-admin.ts renew-crl` does that, writes it in place on the target (the local stack, or the VM with `DEPLOY_HOST`), reloads, and confirms that a valid certificate still connects. The smoke check (S8-6) fails if the CRL has under 7 days left.
- **Why not automatic:** renewal signs with the CA key, and the CA key is kept off every service machine (S8-5). Automating it on the VM would put the key there. So it's an operator task, about every 3 weeks, with an alert a week ahead. A longer CRL lifetime would mean less toil, but would let an old copy of the CRL, without later revocations, stay valid for longer.

### S8-8. ⚑ Close-out: deployed and verified from outside, three first-deploy bugs fixed, and the full demo on the lab VM.

- **Deployed** (2026-10-16, by the subscription's operator, from a clean checkout of the S8-6 commit): `terraform apply` created the 9 resources, and `deploy.sh` started the production profile on the Standard_B2pls_v2 VM. **The smoke check passed all 8 checks from outside:** trusted HTTPS, the API's token, Grafana behind its login, MQTT refusing a client without a certificate and accepting a lab-CA gateway, a reading stored and served end to end, every internal port closed, and the CRL with 29.4 days left. **Remote revocation worked on the VM:** the CRL reload was confirmed after 1.7 s and the kick sent at 2.2 s. The smoke gateway wasn't connected then, so there was no open session to close.
- **Three bugs of the first real deploy, each fixed with a guard against its return:**
  1. **The first `up --build` failed: `image "docker.io/fleet-ingest/node:dev": already exists`.** Eight services built the same image tag in parallel, and the exports collided; a rerun worked only because of the cache. Now one service, `migrate`, builds the image, and the others name it with `pull_policy: never`; Compose builds before it creates containers, so they find it. A unit test checks that exactly one service builds the image, and the lab run below starts from an empty build cache.
  2. **HTTPS never came up: the ACME email default `admin@example.invalid`** was rejected by Let's Encrypt and ZeroSSL alike (`invalidContact`), so no certificate was issued and the TLS handshake failed. There's no placeholder any more: Caddy gets an `email` line only when `ACME_EMAIL` is set, and `deploy.sh` requires a real one. A unit test fails if the placeholder comes back.
  3. **The smoke check needs the repo's Node 24** (it runs TypeScript directly); with an older default Node it failed on the `.ts` file. The runbook now has "After a deploy: the smoke check", with `nvm use` first.
- **The close-out run on the lab VM** (2026-10-16, 11:40–12:40 UTC):
  - the integration suite passed (82 tests); all 41 break checks were caught; the promtool tests passed (31 rules);
  - **a first build from nothing** (image deleted, build cache pruned): `compose up -d --build` succeeded, which is the case that failed on the first deploy;
  - **the full demo, 500 gateways: 12 PASS / 0 FAIL / 0 INVALID**, in 50 minutes:

| # | scenario | result | min |
|---|---|---|---|
| 1 | duplicate storm | PASS | 3 |
| 2 | 10% of gateways offline for 2 min | PASS | 3 |
| 3a | Postgres down 120 s | PASS | 5 |
| 3b | bound 5,000, Postgres down 60 s | PASS | 4 |
| 4 | reconnect storm after a 10 min broker outage | PASS | 16 |
| 5 | poison messages | PASS | 3 |
| 6 | impersonation and revocation (kick, no restart) | PASS | 0 |
| 7 | kill -9 an ingest replica | PASS | 1 |
| 7b | ingest-1 down past a reduced bound | PASS | 4 |
| 8 | power loss on a gateway | PASS | 1 |
| 9 | the real gateway, 2 min WAN cut | PASS | 7 |
| 10 | kill -9 the leading health worker | PASS | 2 |

  - **the CRL watch, end to end (S8-7):** the exporter's `broker_crl_next_update_timestamp_seconds` equalled `openssl`'s `nextUpdate` to the second (1794745348); after `gateway-admin.ts renew-crl` it moved to the new date (1794746370), the broker still accepted valid certificates, and no CRL alert fired.
- **What's left open** is in the README: the open-gap window lives in the health worker's memory, and the load numbers are the laptop's (S7-12).
