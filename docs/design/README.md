# System design

Two Excalidraw files. Open them at [excalidraw.com](https://excalidraw.com) (menu → Open) or with the VS Code "Excalidraw" extension. Both are editable: move boxes and the arrows follow.

| File | What it shows |
|---|---|
| `fleet-ingest-hld.excalidraw` | High-level design as an **evolution**: monolith → split by workload (this build) → scaled |
| `fleet-ingest-lld.excalidraw` | Low-level design of Stage 2: ingest components, one reading's life with its crash windows, data model, commands as desired state |

This is a lab with a fictional domain. Only Stage 2 is built.

---

## High-level design: how the system grows

### Stage 1: Monolith (1–100 gateways)

One Node process subscribes to MQTT, validates each reading, inserts it, acknowledges it, serves the API and runs the health checks. One deploy, one log, one thing to debug. That's the right place to start.

**Rules that are right from day 1 and never change:** the idempotent key `(device, epoch, seq)`, acknowledge only after the database commit, per-gateway certificate identity, and metrics with at least one alert. Every later stage keeps these, which is why they're worth getting right in the monolith.

**What breaks first:**
- One insert per message limits throughput.
- Everything shares one event loop, so a heavy API query or a GC pause stalls ingestion.
- The whole system is one failure domain.
- You can't scale ingestion without also scaling the API.

**Trigger to move on:** ingest lag rising, or API load visibly hurting ingest.

### Stage 2: Split by workload (~500 gateways, ~400 msg/s). This build.

Ingestion and the API become separate services because they scale and fail differently. Ingest batches its writes (500 rows or 200 ms) and runs as two replicas sharing the subscription. Observability becomes its own concern: Prometheus with alert rules, Grafana, cAdvisor for broker memory, and a log-watch for broker drop lines.

**The backpressure chain** is the most important thing to be able to draw:

> Postgres slow → no COMMIT → ingest sends no PUBACK → Receive Maximum reached → the broker stops delivering → the broker queue grows, up to its bound → past the bound, drops are counted and alerted.

The gateway's own buffer is **not** part of this chain. The broker acknowledges the gateway as soon as it receives a message, so the gateway deletes its copy right away. The gateway buffer only covers losing the internet connection.

**The guarantee, stated honestly:** no loss, unless the broker crashes while a backlog exists (up to 5 s of queue, the persist-sqlite flush period) or the backlog outgrows the bound (drops counted and alerted). A clean restart keeps the shared subscriptions; a brand-new broker queues nothing until ingest subscribes.

**What breaks next:**
- One broker means a crash window, and outages are limited by broker memory.
- Shared subscriptions don't move a dead member's share to the live one.
- One large table, so retention by DELETE gets slow.

**Trigger to move on:** outages longer than broker memory can hold, needing more than one broker, needing real consumer takeover, or months of history.

### Stage 3: Scaled (10k–100k+ gateways). Not built.

The key change is a **durable log** (Kafka, Redpanda, JetStream, Kinesis, Event Hubs) behind a broker cluster:
- **The log becomes the buffer, not broker memory,** so the outage window is the log's retention, measured in days.
- **Consumer groups rebalance partitions,** so a dead worker's share really does move to the others. Stage 2 can't do this.
- **Replay from any offset:** reprocess, backfill, or fix a bug and run the data again.
- **Partitioning by `deviceId`** keeps each device's readings in order.
- **A time-series store partitioned by time** makes retention a DROP instead of a DELETE. Rollups serve the dashboards.
- **Device registry and provisioning** become a service (CA/HSM, issuing and revoking certificates), and so do commands/OTA.

**What stays the same:** the idempotent key, committing offsets only after a durable write, and per-gateway identity. **New costs:** many more moving parts, so prefer managed services; and capacity planning per partition.

---

## Low-level design (Stage 2)

**A. Ingest components.**
- **Green boxes are the pure core:** plain functions with no I/O, unit-tested.
- **Blue boxes are adapters:** MQTT, Postgres, HTTP, metrics.
- **Happy path:** MqttAdapter → parseTopic → validateMessage → checkMessage → Batcher → BatchWriter → AckTracker → PUBACK.
- **Poison messages** go to DeadLetterWriter and are acked, so they never block the pipeline.
- **Write errors** go through classifyError:
  - retryable → backoffDelay (full jitter) → retry the same batch
  - poison row → splitBatch finds the bad row and dead-letters it; the rest commits
  - fatal → exit, and `restart: always` brings the replica back with the same client ID
- `main.ts` is the only place that wires the pieces together.
- **Observability (slice 6a):**
  - Ingest, the health worker (S8-3) and a broker exporter (`$SYS` plus log-watch) each serve `/metrics`.
  - Prometheus evaluates 14 alerts and the SLO from them. Alerts go to Alertmanager, then to a logging webhook.
  - The dashboard's middle row is the backpressure chain: unacked and waiting in ingest, then the broker queue, then broker drops.
- **Health (slice 5):** a pure rule set decides online, offline or unknown per gateway, and stale per device, from receive times on ingest's clock. A reading after the latest `offline` proves life. Gaps are found per (device, epoch) with a window function, and are explained by the gateway's `droppedTelemetry`. The API publishes commands retained, with its own certificate; since slice 7 the retained command is a copy of desired state in Postgres (D below).
- **Identity (slice 4):** every client connects to the broker on 8883 with mutual TLS. The certificate CN is the MQTT username (gateway ID, `ingest-1`, `ingest-2`, `api`, `ops`), and an ACL file limits each one to its own topics. Revoked certificates are refused at the handshake, but an open connection isn't dropped by a CRL reload (decisions.md S4-3).
- **How PUBACKs are held (slice 0 finding S0-1):** mqtt.js's documented manual-ack hook serialises delivery. So the AckGate answers mqtt.js at once, swallows mqtt.js's PUBACK, and writes its own after COMMIT, in arrival order. Acks from a closed connection are discarded. mqtt.js is pinned to 5.16.0, and a canary test guards the behaviour.

**B. One reading's life.** The sequence numbers show why each crash point is safe:
- **Ingest dies before the commit:** there was no PUBACK, so the broker redelivers and the reading is stored once.
- **Ingest dies after the commit but before the PUBACK:** it's redelivered, and `ON CONFLICT DO NOTHING` ignores the second copy.
- **The broker crashes with a backlog:** this is the one real loss window in Stage 2. It's stated in the guarantee above.

**C. Data model.**
- **`readings`:** keyed by `(device_id, epoch, seq)`, which makes duplicates impossible.
- **`device_state`:** only moves forward (`WHERE newer`), so a late backfill can't overwrite fresher data.
- **`devices`:** binds each device to one gateway.
- **`gateways`:** the newer-only status, the receive times the health rules need, and two kinds of mode: `mode` as the gateway **reports** it, `desired_mode` as the cloud **asked** for it (slice 7).

**D. Commands as desired state (slice 7, decisions.md S7-1).**
- `PUT /gateways/:id/mode` writes `gateways.desired_mode` first, then publishes. It answers 202 even if the broker is down, because the record is in Postgres now, not on the broker.
- The `CommandReconciler` publishes every desired mode, retained, on each broker connection (a fresh broker gets them all back), and the pending ones every 60 s. All publishes go through one queue, so an older read can't overwrite a newer command.
- `commandStatus` (pure core) compares desired with reported: pending until the gateway's status reports the mode, stuck if an online gateway hasn't applied it 5 minutes after it could have. The health job turns that into `fleet_commands_pending` / `fleet_commands_stuck` and the CommandNotApplied alert.
- It's the device-twin pattern (desired, reported, delta) at Stage 2 size.

---

## Keeping these current

Any slice that changes a component, a flow or a table updates the diagram in the same commit. Box names in the LLD match the function and module names in the code. A stale diagram is a bug.
