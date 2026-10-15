# Spike: mqtt.js manual acks against Mosquitto 2.1.2 (slice 0)

Throwaway, kept as runnable evidence. It checks the premises behind D2, D3 and D5 against a real `eclipse-mosquitto:2.1.2-alpine` broker, before any feature work starts. Slice 1 writes `packages/ingest` from scratch.

```sh
nvm use                       # Node 24.21.0, from .nvmrc at the repo root
corepack pnpm install         # pnpm 12.8.1, pinned in package.json
corepack pnpm --filter @fleet-ingest/spike-mqtt-acks spike             # all checks, about 2 min
corepack pnpm --filter @fleet-ingest/spike-mqtt-acks spike <check-id>  # one check
```

Each check starts its own broker container (`spike-<name>`) with its own config, and removes it afterwards. Raw observations are in `results/results.json`.

Measured on 2026-10-14: an Apple Silicon Mac, Docker Desktop 29.8.0 (8 CPUs, 4 GB VM), Node 24.21.0, mqtt.js 5.16.0.

## Go/no-go results

| Tier | Check | Result | Observed |
|---|---|---|---|
| stop | `deferred-puback-allows-batching` | **PASS, but only with a workaround** | 20/20 received while all 20 PUBACKs were held; released in arrival order; 0 redelivered after resume |
| stop | `deferred-hold-keeps-keepalive` | **PASS** | one ack held 7.5 s with a 2 s keepalive: 3 PINGRESPs, still connected, a 2nd message still arrived |
| stop | `unacked-redelivered-after-resume` | **PASS** | acked 1–5, killed the socket: 6–10 came back with DUP=1, 11–20 new, nothing from 1–5 |
| stop | `broker-honours-receive-maximum` | **PASS** | RM 10: exactly 10 delivered; after releasing 5 acks, exactly 15 |
| escalate | `queue-survives-clean-restart` | **PASS** | 500 queued → 500 delivered after `docker restart` |
| escalate | `crash-loses-since-last-snapshot` | **PASS** | autosave 5 s: all 500 from before the snapshot survived SIGKILL; all 300 queued after it were lost |
| record | `documented-hold-blocks-receiving` | **finding** | with the documented API, holding acks lets **1 of 20** messages in |
| record | `documented-hold-stalls-keepalive` | **finding** | holding one ack for 8 s with a 2 s keepalive led to `Keepalive timeout`, and the connection closed |
| record | `inflight-is-min-of-rm-and-broker` | **finding** | RM 1000 with the default `max_inflight_messages` gave 1000 in flight; RM 10 gave 10 |
| record | `offline-queue-bound` | measured | bound 1000: 1000 kept (seq 1–1000), 1000 dropped (newest), counter +1000, one log line |
| record | `connected-stalled-queue-bound` | measured | RM 10, bound 1000: 1020 delivered in total, counter +980 |
| record | `sys-topics-available` | measured | drops, `store/messages/count` and `bytes`, `heap/current` and `maximum` are all present |
| record | `shared-subscription-failover` | measured | strict rotation including the dead member; live member got 30/60; 20 dropped (counted); in-flight 1, 3 came back with DUP |
| record | `subscriptions-survive-restart` | **finding (slice 3)** | after a clean restart: the plain subscription was restored (5/5), the **shared one was not (0/5)**, and both reported `sessionPresent: true` |
| escalate | `shared-backlog-survives-restart` | **PASS (slice 3 review)** | 500 queued for an offline shared session, clean restart, re-subscribe: 500/500 delivered |
| record | `memory-and-snapshot-at-240k` | measured | RSS 6.4 → 209 MB (844 B per queued message, payload 299 B); peak RSS during snapshot (VmHWM) 209.3 MB, so the snapshot adds nothing; 104 MB on disk; **broker stalled 3.2 s** |

## What it found

1. **The documented manual-ack API can't batch.** mqtt.js reads the next incoming packet only after the current PUBACK is written (`client.js:350`, `handlers/publish.js:97-117`). Holding the `customHandleAcks` callback until a commit therefore lets in one message at a time. A commit slower than the keepalive also blocks PINGRESP, and the client drops a healthy connection. D5's 500-row batches are impossible this way.
2. **A workaround works: hold the PUBACK packet, not the callback.** Call the callback at once, intercept the outgoing `puback` in the client's private `_sendPacket`, and write it after the commit, in arrival order. All three stop-tier checks pass this way. The cost is a dependency on a private method of a pinned library version. **This was the stop condition, resolved before any feature work (decisions.md S0-1).**
3. **Redelivery, Receive Maximum and arrival order behave as D3 needs.**
4. **Persistence behaves exactly as the research read it.** A clean restart keeps the queue; a crash loses everything after the last snapshot.
5. **The broker setting doesn't cap in-flight to a v5 subscriber.** With the default `max_inflight_messages`, the broker still sent ingest its full Receive Maximum (1000). This contradicts the min(RM, broker) expectation. In-flight to ingest is set by ingest's own RM.
6. **Queue bound:**
   - Offline session: exactly `max_queued_messages`. The newest messages are dropped.
   - Connected but stalled session: about `max_queued_messages` + 2 × RM (1020 here).
   - Every drop is counted in `$SYS/broker/publish/messages/dropped`. The log says "Outgoing messages are being dropped for client X" only once per episode.
7. **`$SYS` on 2.1.2-alpine has more than the research expected:**
   - `heap/current` and `heap/maximum` are published, so cAdvisor is optional for broker memory.
   - `store/messages/count` gives scenario 3a its queue-depth gauge.
8. **Memory and snapshots at 240k:**
   - Memory: about 844 B of RSS per queued 299-byte message, 209 MB at 240k.
   - Snapshot: a full snapshot is 104 MB and **freezes the broker for about 3.2 s**. With `autosave_interval 60` and a full backlog, that is about a 5% stall. Gateways keep their 16-message window and wait it out.
9. **`max_queued_bytes` counts payload bytes only** (the research cites `src/database.c:161-169` in 2.0.22; not re-measured on 2.1.2). Sizing it from the RSS per message, as a first estimate did, would make it far larger than intended. Size it from payload: 299 B × 120k × 1.25 ≈ 45 MB.
