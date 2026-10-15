/**
 * Pure core of the migration runner: decides which migration files still need applying, in what order, and refuses
 * to go on when the files and the database disagree about history.
 */

/** One migration file: its version (the numeric prefix), its file name and its SQL. */
export interface Migration {
  version: number;
  name: string;
  sql: string;
}

/** Thrown when the migration files can't be trusted, such as a duplicate version or a gap in history. Fatal. */
export class MigrationPlanError extends Error {
  override name = "MigrationPlanError";
}

const FILE_PATTERN = /^(\d{3})_[a-z0-9_]+\.sql$/;

/**
 * Turns `NNN_name.sql` files into migrations sorted by version. Rejects names that don't match the pattern, and
 * duplicate versions: either one means two people could think a different migration is version N.
 */
export function parseMigrationFiles(files: Array<{ name: string; sql: string }>): Migration[] {
  const migrations = files.map(({ name, sql }) => {
    const match = FILE_PATTERN.exec(name);
    if (!match?.[1]) throw new MigrationPlanError(`migration file name must look like 001_some_name.sql: ${name}`);
    return { version: Number(match[1]), name, sql };
  });
  migrations.sort((a, b) => a.version - b.version);
  for (let i = 1; i < migrations.length; i++) {
    const current = migrations[i];
    const previous = migrations[i - 1];
    if (current && previous && current.version === previous.version) {
      throw new MigrationPlanError(`two migrations share version ${current.version}: ${previous.name}, ${current.name}`);
    }
  }
  return migrations;
}

/**
 * Returns the migrations not yet applied, oldest first.
 * Refuses if the database has a version with no file (someone deleted or renamed one), or if a pending migration is
 * older than the newest applied one (it would run out of order).
 */
export function pendingMigrations(all: Migration[], appliedVersions: number[]): Migration[] {
  const known = new Set(all.map((m) => m.version));
  const missing = appliedVersions.filter((v) => !known.has(v));
  if (missing.length) throw new MigrationPlanError(`applied migrations have no file: ${missing.join(", ")}`);
  const applied = new Set(appliedVersions);
  const newestApplied = Math.max(0, ...appliedVersions);
  const pending = all.filter((m) => !applied.has(m.version));
  const outOfOrder = pending.filter((m) => m.version < newestApplied);
  if (outOfOrder.length) {
    throw new MigrationPlanError(`migrations older than the newest applied (${newestApplied}) are pending: ${outOfOrder.map((m) => m.name).join(", ")}`);
  }
  return pending;
}
