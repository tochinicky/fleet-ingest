/**
 * Composition root of the API: validates config once, creates the pool, connects the command publisher, starts the
 * command reconciler (S7-1), builds the app, listens, serves its process metrics on `/metrics`, and closes everything
 * on SIGTERM/SIGINT. The health job isn't here: it runs in the health worker (`workerMain.ts`, S8-3), and `/fleet`
 * reads its snapshot, so API replicas are stateless and each one starts in seconds.
 */
import type pg from "pg";
import { z } from "zod";
import type { FastifyInstance } from "fastify";
import { readFileSync } from "node:fs";
import { buildApp } from "./app.ts";
import { openApiPool } from "./databasePool.ts";
import { startCommandPublisher } from "./commandPublisher.ts";
import { startCommandReconciler } from "./commandReconciler.ts";
import type { CommandReconciler } from "./commandReconciler.ts";
import { readHealthSnapshot } from "./healthSnapshot.ts";
import { createRegistry, startMetricsServer } from "@fleet-ingest/observability";
import type { CommandPublisher } from "./commandPublisher.ts";

/** Every setting the API reads. The broker settings are for the commands endpoint (certificate CN `api`). */
const configSchema = z.object({
  MQTT_URL: z.url({ protocol: /^mqtts?$/ }),
  MQTT_CLIENT_ID: z.string().default("api"),
  MQTT_CA_FILE: z.string().optional(),
  MQTT_CERT_FILE: z.string().optional(),
  MQTT_KEY_FILE: z.string().optional(),
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  API_TOKEN: z.string().min(16, "API_TOKEN must be at least 16 characters"),
  PORT: z.coerce.number().int().min(1).max(65535).default(8080),
  /** Port of the Prometheus `/metrics` endpoint: the API process's own metrics (Compose network only). */
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9464),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug", "trace"]).default("info"),
});

/** Starts the API. */
async function main(): Promise<void> {
  const parsed = configSchema.safeParse(process.env);
  if (!parsed.success) throw new Error(`invalid configuration:\n${parsed.error.issues.map((i) => `  ${i.path.join(".")}: ${i.message}`).join("\n")}`);
  const config = parsed.data;
  const pool = openApiPool(config.DATABASE_URL, (error) => app.log.warn({ err: error }, "idle database connection failed"));
  const commands = await startCommandPublisher({
    url: config.MQTT_URL,
    clientId: config.MQTT_CLIENT_ID,
    ...(config.MQTT_CA_FILE && config.MQTT_CERT_FILE && config.MQTT_KEY_FILE
      ? { tls: { ca: readFileSync(config.MQTT_CA_FILE), cert: readFileSync(config.MQTT_CERT_FILE), key: readFileSync(config.MQTT_KEY_FILE) } }
      : {}),
  });
  const reconciler = startCommandReconciler({ pool, commands, onError: (error) => app.log.warn({ err: error }, "command reconcile failed") });
  const app = buildApp({
    pool, commands, reconciler, apiToken: config.API_TOKEN, now: () => new Date(), logger: { level: config.LOG_LEVEL },
    fleetGaps: (now) => readHealthSnapshot(pool, now),
  });
  await app.listen({ host: "0.0.0.0", port: config.PORT });
  const registry = createRegistry("api");
  startMetricsServer(registry, config.METRICS_PORT);

  process.on("SIGTERM", () => void shutDown(app, pool, commands, reconciler));
  process.on("SIGINT", () => void shutDown(app, pool, commands, reconciler));
}

/** Stops accepting requests, lets in-flight ones finish (fastify's close), stops reconciling, disconnects, closes the pool, exits 0. */
async function shutDown(app: FastifyInstance, pool: pg.Pool, commands: CommandPublisher, reconciler: CommandReconciler): Promise<void> {
  await app.close();
  reconciler.stop();
  await commands.close();
  await pool.end();
  process.exit(0);
}

await main();
