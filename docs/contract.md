# Wire contract

This is how gateways and the cloud talk over MQTT. It's unchanged from `home-energy-gateway` (`TelemetryMessages.cs`). The zod schemas in [`packages/contract/src/messages.ts`](../packages/contract/src/messages.ts) are the single source of truth, and every service decodes through `decodeTelemetry` / `decodeStatus`.

The schemas are tested against **114 messages captured from the real gateway** (2026-10-14, demo stack). The capture covers all four device kinds, two boots (epoch 3 → 4) and an `offline` Last Will. It's in [`packages/contract/test/fixtures/gateway-capture.tsv`](../packages/contract/test/fixtures/gateway-capture.tsv).

## Topics

| Topic | Direction | QoS | Retained |
|---|---|---|---|
| `homes/{gatewayId}/devices/{deviceId}/telemetry` | gateway → cloud | 1 | no |
| `homes/{gatewayId}/status` | gateway → cloud (and the broker, for the Last Will) | 1 | yes |
| `homes/{gatewayId}/commands` | cloud → gateway | 1 | yes |

Ingest joins `$share/ingest/homes/+/devices/+/telemetry` and `$share/ingest/homes/+/status` (D2).

## Telemetry

```json
{"deviceId":"northwind-metering-nm3p-004711","epoch":3,"seq":73,"timestamp":"2026-10-14T11:07:52.4362141+00:00","kind":"GridMeter","gridPowerW":-6054,"energyWh":1}
```

| Field | Type | Rule |
|---|---|---|
| `deviceId` | string, 1–128 chars | must equal the `{deviceId}` in the topic, or the message is dead-lettered (`device_id_mismatch`) |
| `epoch` | non-negative integer | the gateway's boot number |
| `seq` | non-negative integer | +1 per device; continues across restarts; may roll back only with a new epoch |
| `timestamp` | ISO 8601 **with offset** | .NET writes 7 fractional digits; any precision is accepted |
| `kind` | `Inverter` \| `Battery` \| `Charger` \| `GridMeter` | |
| `pvPowerW`, `batteryPowerW`, `gridPowerW`, `evPowerW`, `energyWh` | number, optional | **omitted** when not reported, never `null` |
| `socPercent` | 0–100, optional | |
| `chargeLimitA` | ≥ 0, optional | |

- **Signs:** grid import is positive, battery charging is positive, EV charging is positive.
- **Unknown extra fields are stripped, not rejected**, so a newer gateway can add a field without its readings being dead-lettered.
- **The idempotency key is `(deviceId, epoch, seq)`.** Dedup and gap checks are per (device, epoch). Never compare seq across epochs. A producer must never reuse an epoch (fleet-sim's bug S2-6 shows why).
- **A `timestamp` more than 5 minutes ahead of the cloud's clock is rejected** (`future_timestamp`, D7). The same applies to status timestamps.
- **A device belongs to the first gateway it appeared under.** The same `deviceId` from another gateway is rejected (`foreign_device`, D6).

What each kind actually sends (from the capture):

| Kind | Fields |
|---|---|
| `GridMeter` | `gridPowerW`, `energyWh` |
| `Inverter` | `pvPowerW`, `energyWh` |
| `Battery` | `batteryPowerW`, `socPercent` |
| `Charger` | `evPowerW`, `energyWh`, `chargeLimitA` |

## Status

```json
{"state":"online","timestamp":"2026-10-14T11:08:10.4973208+00:00","bufferedTelemetry":0,"droppedTelemetry":0,"mode":"surplus"}
{"state":"offline"}
```

- `online` is sent on every connect and every 60 s.
- `offline` is the Last Will. The broker publishes it with no timestamp, so ingest uses the receive time.
- `mode` is accepted as any string, so a mode this cloud doesn't know yet can't get a status dead-lettered.

## Command

```json
{"mode":"max"}
```

`mode` is one of `surplus`, `max` or `off`. It's published retained, so the gateway applies it when it next connects.

- **Who publishes it:** only the API, through `PUT /gateways/:id/mode`, with its `api` certificate. The broker ACL refuses everyone else, gateways included (S4-1).
- **The gateway reports the mode it applied** in its next status (`mode`).

## Payload sizes

These are for sizing `max_queued_bytes`, which counts payload bytes only:

| Source | p50 | p95 | max |
|---|---|---|---|
| Real gateway (114 messages) | 159 B | 163 B | 177 B |
| fleet-sim (48,000 messages) | 145 B | 159 B | 161 B |
