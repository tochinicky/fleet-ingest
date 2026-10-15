/**
 * fleet-sim's settings, from the environment. In their own module so the scenario scripts can read the same defaults
 * the simulator uses (scenario 4's setup budget is derived from them, S7-11) without starting a simulator.
 */
import { z } from "zod";

/** Every setting, defaulting to the lab fleet and the real gateway's uplink options. */
export const configSchema = z.object({
  MQTT_URL: z.url({ protocol: /^mqtts?$/ }),
  /**
   * A second route to the broker for every CUTTABLE_EVERY-th gateway. In Compose it's its own toxiproxy proxy, so a
   * scenario can cut the "internet" of 10% of the fleet (scenario 2) without touching the rest.
   */
  MQTT_URL_CUTTABLE: z.url({ protocol: /^mqtts?$/ }).optional(),
  CUTTABLE_EVERY: z.coerce.number().int().min(1).default(10),
  /** The broker certificate's name, when the URL's host is a proxy (toxiproxy). */
  MQTT_SERVERNAME: z.string().optional(),
  KEEPALIVE_SECONDS: z.coerce.number().int().min(5).default(15),
  /** Per-attempt CONNACK timeout; 10 s like the real gateway's MQTTnet `WithTimeout` (S7-8). */
  CONNECT_TIMEOUT_MS: z.coerce.number().int().min(1000).max(60_000).default(10_000),
  /** Full-jitter reconnect delay maximum; 0 means a fixed 2 s for every gateway (scenario 4's worst case). */
  RECONNECT_JITTER_MAX_MS: z.coerce.number().int().min(0).default(0),
  GATEWAY_COUNT: z.coerce.number().int().min(1).max(5000).default(10),
  DEVICES_PER_GATEWAY: z.coerce.number().int().min(1).max(16).default(4),
  GATEWAY_ID_PREFIX: z.string().regex(/^[a-z0-9-]+$/).default("sim-gw"),
  READING_INTERVAL_MS: z.coerce.number().int().min(100).default(5000),
  STATUS_INTERVAL_MS: z.coerce.number().int().min(1000).default(60_000),
  UPLINK_WINDOW: z.coerce.number().int().min(1).default(16),
  REPLAY_PER_SECOND: z.coerce.number().min(1).default(200),
  BUFFER_CAPACITY: z.coerce.number().int().min(1).default(50_000),
  /**
   * The boot number. The real gateway increments it on every boot and seq starts again only within a new epoch, so
   * a restarted simulator must never reuse an epoch: by default it's the start time in whole seconds (decisions.md S2-6).
   */
  EPOCH: z.coerce.number().int().min(1).default(() => Math.floor(Date.now() / 1000)),
  SEED: z.coerce.number().int().default(42),
  /**
   * Directory with `ca.crt` and one `<gatewayId>.crt` / `.key` per gateway (made by `scripts/pki.sh issue-fleet`).
   * Required for mqtts://.
   */
  CERTS_DIR: z.string().optional(),
  /** Port of the control API: `GET /ledger` (every reading the broker acknowledged) and `GET /stats`. */
  CONTROL_PORT: z.coerce.number().int().min(1).max(65535).default(8090),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug"]).default("info"),
});
