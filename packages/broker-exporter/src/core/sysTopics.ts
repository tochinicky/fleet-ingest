/**
 * Which broker statistics are exported, and under what metric names. Mosquitto publishes them under `$SYS/broker/…`
 * every `sys_interval` (10 s). The ones the alerts and the dashboard use are listed in decisions.md S6-3.
 * Pure: a topic and its payload in, a metric name and number out.
 */

/** One exported `$SYS` value: its topic, metric name and help text. */
export interface SysMetric {
  topic: string;
  name: string;
  help: string;
}

/**
 * The exported topics. Mosquitto's counters (dropped, received, sent) reset to 0 when it restarts. They're exported
 * as they are, and the alert rules use `increase()`, which treats a drop to a lower value as a reset (S0-3, D3).
 */
export const SYS_METRICS: SysMetric[] = [
  { topic: "$SYS/broker/publish/messages/dropped", name: "broker_publish_messages_dropped", help: "Messages the broker dropped because a session queue was full (resets on restart)." },
  { topic: "$SYS/broker/store/messages/count", name: "broker_store_messages", help: "Messages held by the broker: queued for sessions, in flight, retained." },
  { topic: "$SYS/broker/store/messages/bytes", name: "broker_store_messages_bytes", help: "Payload bytes of the messages held by the broker." },
  { topic: "$SYS/broker/heap/current", name: "broker_heap_bytes", help: "Broker heap in use." },
  { topic: "$SYS/broker/heap/maximum", name: "broker_heap_maximum_bytes", help: "Largest broker heap since start." },
  { topic: "$SYS/broker/clients/connected", name: "broker_clients_connected", help: "Clients connected now (gateways plus services)." },
  { topic: "$SYS/broker/clients/total", name: "broker_clients_total", help: "Clients known, connected or with a persistent session." },
  { topic: "$SYS/broker/subscriptions/count", name: "broker_subscriptions", help: "Subscriptions, shared ones included." },
  { topic: "$SYS/broker/messages/received", name: "broker_messages_received", help: "MQTT packets received (resets on restart)." },
  { topic: "$SYS/broker/messages/sent", name: "broker_messages_sent", help: "MQTT packets sent (resets on restart)." },
  { topic: "$SYS/broker/publish/messages/received", name: "broker_publish_messages_received", help: "PUBLISH packets received (resets on restart)." },
  { topic: "$SYS/broker/publish/messages/sent", name: "broker_publish_messages_sent", help: "PUBLISH packets sent (resets on restart)." },
  { topic: "$SYS/broker/uptime", name: "broker_uptime_seconds", help: "Seconds since the broker started." },
];

/** Parses a `$SYS` payload: a plain number, or a number followed by a unit ("1234 seconds"). Null if it isn't one. */
export function parseSysValue(payload: string): number | null {
  const match = /^\s*(-?\d+(?:\.\d+)?)/.exec(payload);
  return match ? Number(match[1]) : null;
}
