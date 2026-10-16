/**
 * Composition root of the database jobs (S8-2):
 * - `migrate` (the default): applies pending migrations from `db/migrations`, then one partition-maintenance pass so
 *   a fresh install has its partitions before ingest starts, and exits. Non-zero on any failure, so Compose doesn't
 *   start the services that depend on it.
 * - `maintain`: the single writer of `readings`'s partitions. Every hour it creates the days ahead and drops the
 *   days retention no longer keeps, and it exports how far ahead the partitions reach, for PartitionsRunningOut.
 * Both read RETENTION_DAYS and PARTITIONS_AHEAD_DAYS; the retention floor is checked at startup.
 */
import { setTimeout as sleep } from "node:timers/promises";
import pg from "pg";
import { z } from "zod";
import { Counter, createRegistry, Gauge, startMetricsServer } from "@fleet-ingest/observability";
import { applyMigrations, assertPartitionSettings, listPartitions, loadMigrations, maintainPartitions, MIGRATIONS_DIR } from "./index.ts";

/** The jobs' settings. */
const configSchema = z.object({
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  MIGRATIONS_DIR: z.string().default(MIGRATIONS_DIR),
  RETENTION_DAYS: z.coerce.number().int().default(30),
  PARTITIONS_AHEAD_DAYS: z.coerce.number().int().default(7),
  MAINTAIN_INTERVAL_MS: z.coerce.number().int().min(60_000).default(3_600_000),
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9466),
});
type Config = z.infer<typeof configSchema>;

/** Opens a client whose lost socket fails the job through its query rather than an unhandled event (S7-10). */
async function connect(config: Config): Promise<pg.Client> {
  const client = new pg.Client({ connectionString: config.DATABASE_URL });
  client.on("error", () => undefined);
  await client.connect();
  return client;
}

/** Applies migrations and runs one maintenance pass. */
async function migrate(config: Config): Promise<void> {
  const client = await connect(config);
  try {
    const applied = await applyMigrations(client, loadMigrations(config.MIGRATIONS_DIR));
    process.stdout.write(applied.length ? `applied: ${applied.join(", ")}\n` : "database is up to date\n");
    const done = await maintainPartitions(client, { retentionDays: config.RETENTION_DAYS, aheadDays: config.PARTITIONS_AHEAD_DAYS }, new Date());
    process.stdout.write(`partitions: created ${done.created.length}, dropped ${done.dropped.length}\n`);
  } finally {
    await client.end();
  }
}

/** Maintains the partitions forever, one pass per interval; a failed pass is logged and retried at the next. */
async function maintain(config: Config): Promise<never> {
  const registry = createRegistry("partitions");
  const registers = [registry];
  const aheadDays = new Gauge({ name: "fleet_partitions_ahead_days", help: "Whole days of partitions that exist after today: inserts need at least one (S8-2).", registers });
  const created = new Counter({ name: "fleet_partitions_created_total", help: "Daily partitions created.", registers });
  const dropped = new Counter({ name: "fleet_partitions_dropped_total", help: "Daily partitions dropped by retention.", registers });
  const lastSuccess = new Gauge({ name: "fleet_partitions_last_success_timestamp_seconds", help: "When maintenance last completed a pass.", registers });
  startMetricsServer(registry, config.METRICS_PORT);
  for (;;) {
    try {
      const client = await connect(config);
      try {
        const now = new Date();
        const done = await maintainPartitions(client, { retentionDays: config.RETENTION_DAYS, aheadDays: config.PARTITIONS_AHEAD_DAYS }, now);
        created.inc(done.created.length);
        dropped.inc(done.dropped.length);
        const newest = Math.max(...(await listPartitions(client)).map((p) => p.to.getTime()));
        aheadDays.set(Math.floor((newest - now.getTime()) / 86_400_000));
        lastSuccess.set(now.getTime() / 1000);
        if (done.created.length || done.dropped.length) process.stdout.write(`${now.toISOString()} created ${done.created.join(",") || "-"} dropped ${done.dropped.join(",") || "-"}\n`);
      } finally {
        await client.end();
      }
    } catch (error) {
      process.stderr.write(`${new Date().toISOString()} partition maintenance failed: ${error instanceof Error ? error.message : String(error)}\n`);
    }
    await sleep(config.MAINTAIN_INTERVAL_MS);
  }
}

/** Runs the job named by the first argument. */
async function main(): Promise<void> {
  const config = configSchema.parse(process.env);
  assertPartitionSettings({ retentionDays: config.RETENTION_DAYS, aheadDays: config.PARTITIONS_AHEAD_DAYS });
  if (process.argv[2] === "maintain") await maintain(config);
  else await migrate(config);
}

await main();
