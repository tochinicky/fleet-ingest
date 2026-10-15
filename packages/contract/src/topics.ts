/**
 * The MQTT topic layout shared by the gateway, ingest, the API and fleet-sim.
 * It builds topics from IDs and parses a received topic back into the gateway and device it names.
 */

/** What a received topic refers to. `unknown` covers anything outside the contract, which ingest dead-letters. */
export type ParsedTopic =
  | { kind: "telemetry"; gatewayId: string; deviceId: string }
  | { kind: "status"; gatewayId: string }
  | { kind: "unknown" };

/** The shared subscription that ingest replicas join for readings (D2). */
export const SHARED_TELEMETRY_SUBSCRIPTION = "$share/ingest/homes/+/devices/+/telemetry";

/** The shared subscription that ingest replicas join for gateway status messages (D2). */
export const SHARED_STATUS_SUBSCRIPTION = "$share/ingest/homes/+/status";

/** The topic a gateway publishes one device's readings on. */
export function telemetryTopic(gatewayId: string, deviceId: string): string {
  return `homes/${gatewayId}/devices/${deviceId}/telemetry`;
}

/** The retained topic a gateway publishes its status on; the broker also publishes the Last Will here. */
export function statusTopic(gatewayId: string): string {
  return `homes/${gatewayId}/status`;
}

/** The retained topic the cloud publishes a gateway's desired mode on. */
export function commandTopic(gatewayId: string): string {
  return `homes/${gatewayId}/commands`;
}

/**
 * Splits a topic into its gateway and device IDs. Only the exact shapes `homes/{g}/devices/{d}/telemetry` and
 * `homes/{g}/status` are recognised, and every ID must be non-empty; anything else is `unknown`.
 */
export function parseTopic(topic: string): ParsedTopic {
  const parts = topic.split("/");
  const [root, gatewayId, third, deviceId, fifth] = parts;
  if (root !== "homes" || !gatewayId) return { kind: "unknown" };
  if (parts.length === 3 && third === "status") return { kind: "status", gatewayId };
  if (parts.length === 5 && third === "devices" && deviceId && fifth === "telemetry") {
    return { kind: "telemetry", gatewayId, deviceId };
  }
  return { kind: "unknown" };
}
