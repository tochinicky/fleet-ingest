/**
 * Composition root of the migration job: validates its config, connects to Postgres, applies pending migrations from
 * `db/migrations`, and exits non-zero on any failure so Compose doesn't start the services that depend on it.
 */
import pg from "pg";
import { z } from "zod";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "./index.ts";

/** The job's only setting. */
const configSchema = z.object({
  DATABASE_URL: z.url({ protocol: /^postgres(ql)?$/ }),
  MIGRATIONS_DIR: z.string().default(MIGRATIONS_DIR),
});

/** Runs the job once. */
async function main(): Promise<void> {
  const config = configSchema.parse(process.env);
  const client = new pg.Client({ connectionString: config.DATABASE_URL });
  // A lost socket also rejects the query in flight, which fails the job below; without a listener the event would
  // throw first and the job would die with a stack trace instead of its error (S7-10).
  client.on("error", () => undefined);
  await client.connect();
  try {
    const applied = await applyMigrations(client, loadMigrations(config.MIGRATIONS_DIR));
    process.stdout.write(applied.length ? `applied: ${applied.join(", ")}\n` : "database is up to date\n");
  } finally {
    await client.end();
  }
}

await main();
