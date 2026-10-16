/**
 * Migration tooling shared by the job in `main.ts` and the integration tests of other packages.
 */
import { join } from "node:path";

export { applyMigrations, loadMigrations } from "./migrationRunner.ts";
export { MigrationPlanError, parseMigrationFiles, pendingMigrations } from "./migrationPlan.ts";
export type { Migration } from "./migrationPlan.ts";
export {
  acceptedSince, assertPartitionSettings, covers, ensurePartitions, keptDays, listPartitions, maintainPartitions, partitionName, planPartitions,
  RETENTION_FLOOR_DAYS, startOfUtcDay,
} from "./partitions.ts";
export type { PartitionRange, PartitionSettings } from "./partitions.ts";

/** Absolute path of the repo's `db/migrations` directory. */
export const MIGRATIONS_DIR = join(import.meta.dirname, "..", "..", "..", "db", "migrations");
