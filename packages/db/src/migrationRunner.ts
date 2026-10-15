/**
 * Adapter that applies pending migrations to Postgres. Each migration runs in its own transaction together with
 * its row in `schema_migrations`, so a migration is either fully applied and recorded, or not at all.
 * A session-level advisory lock means two runners started at once (two replicas booting) can't both apply.
 */
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import type pg from "pg";
import { parseMigrationFiles, pendingMigrations } from "./migrationPlan.ts";
import type { Migration } from "./migrationPlan.ts";

/** Arbitrary but fixed key for `pg_advisory_lock`; it only has to differ from other advisory locks in this database. */
const MIGRATION_LOCK_KEY = 7_420_001;

/** Reads every `.sql` file in `directory` as a migration. */
export function loadMigrations(directory: string): Migration[] {
  const names = readdirSync(directory).filter((name) => name.endsWith(".sql"));
  return parseMigrationFiles(names.map((name) => ({ name, sql: readFileSync(join(directory, name), "utf8") })));
}

/**
 * Applies every pending migration in order and returns the names it applied. Holds the advisory lock throughout and
 * always releases it, even when a migration fails, in which case that migration's transaction is rolled back and
 * the error is rethrown.
 */
export async function applyMigrations(client: pg.ClientBase, migrations: Migration[]): Promise<string[]> {
  await client.query("SELECT pg_advisory_lock($1)", [MIGRATION_LOCK_KEY]);
  try {
    await client.query(
      "CREATE TABLE IF NOT EXISTS schema_migrations (version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())",
    );
    const { rows } = await client.query<{ version: number }>("SELECT version FROM schema_migrations");
    const pending = pendingMigrations(migrations, rows.map((row) => row.version));
    for (const migration of pending) await applyOne(client, migration);
    return pending.map((m) => m.name);
  } finally {
    await client.query("SELECT pg_advisory_unlock($1)", [MIGRATION_LOCK_KEY]);
  }
}

/** Runs one migration and records it, in a single transaction. */
async function applyOne(client: pg.ClientBase, migration: Migration): Promise<void> {
  await client.query("BEGIN");
  try {
    await client.query(migration.sql);
    await client.query("INSERT INTO schema_migrations (version, name) VALUES ($1, $2)", [migration.version, migration.name]);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }
}
