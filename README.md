# fleet-ingest

The cloud side of a fictional fleet of home-energy gateways, in TypeScript. It authenticates gateways, stores every reading exactly once even though readings arrive late, twice and out of order, notices gateways and devices that go quiet, and survives its own database going down.

**This is a lab.** The domain, the devices and the fleet are fictional: one real lab gateway (a separate .NET gateway repo, `home-energy-gateway`; set `GATEWAY_REPO` to its checkout to run scenario 9) plus a simulator. Nothing here has served production traffic.

## Status

Built in slices; every decision and finding is in [`docs/decisions.md`](docs/decisions.md).

| Slice | What | State |
|---|---|---|
| 0 | Spike: mqtt.js manual acks against Mosquitto 2.1.2 ([`spikes/mqtt-acks`](spikes/mqtt-acks/README.md)) | done |
| 1 | Contract, schema, ingest happy path, API reads, 10-gateway fleet-sim | done |
| 2 | Idempotency and ordering: newer-only state and status, property test, dead letters (D7), break-check script | done |
| 3 | Backpressure and retries: batches of 500 / 200 ms, full-jitter backoff, poison isolation, toxiproxy, scenarios 3a, 3b and 7 | done |
| 4 | Identity: per-gateway certificates (mTLS), ACL by CN, CRL revocation, device binding, threat list, gateway TLS, scenario 6 | done |
| 5 | Health: evidence-based online/offline/unknown, stale devices, gaps per epoch (explained by droppedTelemetry), commands, scenarios 2 and 8 | done |
| 6a | Operability: metrics, broker exporter, alerts with runbooks (14 then; 16 today), SLO with burn-rate alerts, Grafana dashboard | done |
| 6b | Load numbers at 500 gateways, scenario 4, exact 3b accounting, persist-sqlite experiment | done |
| 7 | Ship: commands as desired state (reconciler), lint and CI, Terraform (plan only), production profile, the real gateway end to end, demo.sh; persist-sqlite adopted (S7-7); database hangs and dropped connections survived (S7-9, S7-10); fleet-wide reads without whole-table scans (S7-11); clean reruns on a lab VM (S7-12) | done |

## Run it

Needs Docker, Node 24 (`nvm use`) and corepack.

**Lab VM.** The demo, the scenarios and the integration tests run on a separate Linux machine, not on a laptop's Docker VM, which the 500-gateway stack saturates (decisions.md S6-6, S7-12). `scripts/lab-vm.sh` syncs the tree, runs a command there and pulls the results back: `LAB_HOST=user@host LAB_SSH_KEY=path scripts/lab-vm.sh sync`, then `… run scripts/demo.sh`, then `… pull-results`. Typecheck, lint and unit tests run anywhere.

```sh
corepack pnpm install
scripts/dev-certs.sh 10           # lab CA + a certificate per service and per simulated gateway (gitignored)
docker compose -f deploy/compose.yaml up -d --build
curl -s localhost:8080/fleet -H "Authorization: Bearer dev-token-change-me-0123"
open http://localhost:3000        # Grafana dashboard "fleet-ingest"; Prometheus :9090, alerts log :9095/alerts
```

Tests:

```sh
corepack pnpm lint                # ESLint (tools/lint, decisions.md S7-2)
corepack pnpm typecheck
corepack pnpm test               # unit, about a second
corepack pnpm test:integration   # real Postgres and Mosquitto via Testcontainers
node scripts/break-checks.ts     # breaks each rule on purpose; every break must make a test fail
corepack pnpm test:alerts        # promtool check + rule unit tests + every alert has a runbook entry
node scripts/scenarios/postgres-down.ts 3a   # also 3b; kill-replica.ts, broker-restart*.ts (500 gateways)
node scripts/scenarios/impersonation.ts      # scenario 6 (starts the stack if it isn't up)
node scripts/scenarios/gateways-offline.ts   # scenario 2 (500 gateways); power-loss.ts is scenario 8
scripts/demo.sh                              # every scenario (1–9): PASS, FAIL or INVALID, table in demo-results/ (S7-6, S7-7)
```

## The guarantee (D3)

At-least-once on the wire, exactly-once in Postgres, by construction:
- a reading is acknowledged only after its write commits;
- it's stored under the primary key `(device_id, epoch, seq)`.

The backpressure chain is: database slow → no COMMIT → no PUBACK → Receive Maximum reached → the broker queues, up to its bound (about 10 minutes of traffic) → past the bound, drops are counted. The gateway's own buffer covers losing the internet only, because the broker acknowledges a gateway as soon as it receives a message.

**No loss, unless:**
- the broker crashes (SIGKILL, OOM) while a backlog exists, which loses up to 5 s of queue: persistence is the persist-sqlite plugin, which commits every 5 s ([`decisions.md` S6-15, S7-7](docs/decisions.md));
- or the backlog outgrows the bound, in which case the drops are counted and alerted;
- or the broker is brand new, or an ingest session has expired after 7 days, so nothing is queued until ingest subscribes (see [`docs/runbooks.md`](docs/runbooks.md)).

A broker **restart** keeps the ingest sessions and their shared subscriptions, so readings published before ingest reconnects queue for it. (With snapshots they didn't, and those readings were lost; [`decisions.md` S3-9](docs/decisions.md).) The plugin is new in Mosquitto 2.1 and has open bugs; the ones that touch this system are pinned by canary tests ([`decisions.md` S6-14](docs/decisions.md)).

## Load numbers (slice 6b)

Measured on one laptop, so read them as relative, not as capacity planning: Apple M1, 8 cores, 8 GiB, Docker Desktop VM with 8 CPU / 4 GB. With the whole stack and 500 simulated gateways running, that VM is **saturated** (CPU pressure about 53%, about 90 MB free), and it stalls everything in it for seconds at a time ([`decisions.md` S6-6](docs/decisions.md)). 500 gateways send about 400 msg/s.

| Configuration | Steady msg/s | Lag p50 / p95 | Commit p95 | Backlog drain |
|---|---|---|---|---|
| 2 replicas, batches of 500 (the default) | 395 | 35 ms / 0.58 s | 0.45 s | 21k in 18 s |
| 1 replica, batches of 500 | 401 | 46 ms / 6.9 s | 0.81 s | 20k in 33 s, peak 2,756 msg/s |
| 2 replicas, one row per transaction | 175 of 400 | 167 s / 287 s, growing | 28 s | never; the queue passed its bound and 68,223 were dropped (counted) |

Batching is what makes the target reachable: one replica with batches sustains about 7× the fleet's load. Ingest memory stays within +20 MB under a flood, because Receive Maximum caps what it holds.

**Reconnect storm** (scenario 4: the broker is down for 10 minutes, then all 500 gateways reconnect and replay about 210k–240k buffered readings):

| Reconnect | Buffered at restart | Drain | Peak broker queue | Dropped | Acknowledged readings stored |
|---|---|---|---|---|---|
| every 2 s, no jitter | 209,844 | 682 s | 2,344 | 0 | 399,315 of 399,315 |
| uniform 0–300 s (2 runs) | 236,751 / 239,613 | 452 s / 636 s | 1,535 / 1,350 | 0 | 401,280 of 401,284 (4 lost, see below) / 456,153 of 456,153 |

**The drain column measures the simulator, not ingest** ([`decisions.md` S6-13](docs/decisions.md)). The broker queue never built up, so ingest was never the limit. The limit is the sending side. fleet-sim is one process holding 500 TLS clients on the saturated VM, and it replays at about 700 msg/s in total, far below 500 gateways × 200 msg/s. A real fleet would replay faster, and the 2,750 msg/s per replica above is the headroom that absorbs it.

The 4 lost readings in one jittered run were acknowledged by the broker but never reached ingest, and no drop was counted. That's most likely the broker-restart window in the guarantee above, hit by gateways that reconnected in under a second ([`decisions.md` S6-11](docs/decisions.md)).

## Open items

What the lab hasn't solved, each with its numbers in [`decisions.md`](docs/decisions.md):
- **Cold starts are slow, on the laptop.** In steady state under load every health-job part and `/fleet` answers in under 2 s. In the first minutes after a cold start on the laptop's Docker VM, health and clock-skew passes reached about 4–8 s: Postgres reading a cold table, and API event-loop stalls of up to 3.5 s from garbage collection (S7-11). Not yet re-measured on the lab VM. Moving gap work out of the API process is the next step.
- **The open-gap window lives in memory, in each API process:** about 12 bytes per reading in the last hour (about 17 MB at 500 gateways), growing with fleet × window, and each replica warms its own (S7-11).
- **Gap explanation reads a gateway's whole history** once that gateway has dropped readings, so its cost grows with that history; a per-gateway checkpoint rollup is the fix (S7-11).
- **One large table.** `readings` is a single table; at fleet scale the answer is time partitioning plus rollups, not more indexes (S7-11).
- **The laptop was the limit, not the system.** On a quiet 8 vCPU / 32 GB machine every scenario passes, both persistence back ends measure the same, and the laptop's lag FAILs don't reproduce (S7-12). The load numbers above were measured on the laptop and are kept as they are; read them as a floor.
- **Revocation needs a broker restart** to drop a revoked gateway's open connection (S4-3).
- **Nothing is deployed:** Terraform is plan-only and CI runs once the repo is pushed (S7-3, S7-4).

Design: [`docs/design/`](docs/design/README.md). Wire contract: [`docs/contract.md`](docs/contract.md).
