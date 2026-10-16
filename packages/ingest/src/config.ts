/**
 * Ingest's configuration, read from the environment once at startup and validated with zod. Code past `main.ts`
 * only ever sees the typed result, never `process.env`.
 */
import { RETENTION_FLOOR_DAYS } from "@fleet-ingest/db";
import { z } from "zod";

/** Every setting, its default, and what it's for. */
const configSchema = z.object({
  /** Broker URL: mqtts:// with a client certificate in every deployment (D6); mqtt:// only for tests. */
  MQTT_URL: z.url({ protocol: /^mqtts?$/ }),
  /** PEM files for mutual TLS: the lab CA (to verify the broker), and this replica's certificate (CN = client ID) and key. */
  MQTT_CA_FILE: z.string().optional(),
  MQTT_CERT_FILE: z.string().optional(),
  MQTT_KEY_FILE: z.string().optional(),
  /** Fixed per replica (ingest-1, ingest-2), so a restarted replica resumes its own session (D2). */
  MQTT_CLIENT_ID: z.string().regex(/^ingest-[a-z0-9-]+$/),
  /** Most unacknowledged messages the broker may send this connection: the bound on in-memory work (D3). */
  RECEIVE_MAXIMUM: z.coerce.number().int().min(1).max(65535).default(1000),
  /** How long the broker keeps this replica's session while it's gone. Long, because the share waits in it (D2). */
  SESSION_EXPIRY_SECONDS: z.coerce.number().int().min(60).default(7 * 24 * 3600),
  /** Retry interval for a lost broker connection; below the gateways' 1 s first retry (decisions.md S3-9, S7-7). */
  RECONNECT_PERIOD_MS: z.coerce.number().int().min(50).max(5000).default(250),
  KEEPALIVE_SECONDS: z.coerce.number().int().min(5).max(300).default(30),
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  DATABASE_POOL_SIZE: z.coerce.number().int().min(1).max(50).default(4),
  /**
   * Days of readings kept (S8-2); older readings are dead-lettered as too_old. At least the retention floor (4 days:
   * the oldest reading a gateway can replay with one device online, plus margin), checked here at startup.
   */
  RETENTION_DAYS: z.coerce.number().int().min(RETENTION_FLOOR_DAYS).default(30),
  /** A batch is written at this many rows... (D5) */
  BATCH_MAX_ROWS: z.coerce.number().int().min(1).max(5000).default(500),
  /** ...or this long after its oldest message arrived, whichever comes first (D5). */
  BATCH_MAX_WAIT_MS: z.coerce.number().int().min(0).max(10_000).default(200),
  /** Full-jitter backoff for a failing write: base and cap. */
  RETRY_BASE_MS: z.coerce.number().int().min(1).default(200),
  RETRY_CAP_MS: z.coerce.number().int().min(1).default(30_000),
  /** On SIGTERM, how long to keep trying to write and acknowledge what's in hand before exiting anyway. */
  SHUTDOWN_GRACE_MS: z.coerce.number().int().min(0).default(8000),
  /** Port of the Prometheus `/metrics` endpoint (Compose network only). */
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9464),
  /** How often a one-line summary of what ingest did is logged (until the metrics of slice 6). */
  SUMMARY_INTERVAL_MS: z.coerce.number().int().min(1000).default(10_000),
  /** Written once the subscriptions are in place; the container healthcheck tests for it. */
  READY_FILE: z.string().default("/tmp/ingest-ready"),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
});

/** The validated configuration. */
export type Config = z.infer<typeof configSchema>;

/** Validates `env`. On failure it throws one error listing every bad setting, so startup fails fast and clearly. */
export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const result = configSchema.safeParse(env);
  if (!result.success) {
    const problems = result.error.issues.map((issue) => `  ${issue.path.join(".")}: ${issue.message}`).join("\n");
    throw new Error(`invalid configuration:\n${problems}`);
  }
  const tlsFiles = [result.data.MQTT_CA_FILE, result.data.MQTT_CERT_FILE, result.data.MQTT_KEY_FILE];
  if (result.data.MQTT_URL.startsWith("mqtts:") && tlsFiles.some((f) => !f)) {
    throw new Error("invalid configuration:\n  mqtts:// needs MQTT_CA_FILE, MQTT_CERT_FILE and MQTT_KEY_FILE");
  }
  if (result.data.BATCH_MAX_ROWS > result.data.RECEIVE_MAXIMUM) {
    throw new Error("invalid configuration:\n  BATCH_MAX_ROWS must not exceed RECEIVE_MAXIMUM, or a batch could never fill");
  }
  return result.data;
}
