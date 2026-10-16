/**
 * Daily partitions of `readings` by `measured_at`, and the rules that keep inserts from ever meeting a missing one
 * (decisions.md S8-2).
 *
 * The windows, for retention D days and A days ahead, with T the start of today (UTC):
 * - **kept:** partitions for the days [T − D, T + A]; anything ending at or before T − D is dropped;
 * - **accepted by ingest:** readings measured at or after T − (D − 1); older ones are dead-lettered as `too_old`.
 * One whole day of partitions always lies between ingest's cutoff and the oldest kept partition, so a reading ingest
 * accepts always has a partition, even across midnight, when the two may read the clock a moment apart. Readings
 * from the future are bounded by the 5-minute skew rule (D7), far inside A days. There is no default partition: a
 * reading with nowhere to go is impossible by construction, not caught after the fact.
 *
 * Pure planning here; {@link maintainPartitions} and {@link listPartitions} are the adapter.
 */
import type pg from "pg";

/** One partition: its table name and the half-open range [from, to) of `measured_at` it holds. */
export interface PartitionRange {
  name: string;
  from: Date;
  to: Date;
}

/** Retention and look-ahead, in whole days. */
export interface PartitionSettings {
  retentionDays: number;
  aheadDays: number;
}

/**
 * The shortest retention allowed: the oldest reading a gateway can still replay with one device online, 50,000
 * buffered messages × 5 s ≈ 69 h, plus a day of margin (S8-2, S8-4). A shorter retention would dead-letter
 * replays that a gateway can legitimately send. Checked at startup by every service that reads the setting.
 */
export const RETENTION_FLOOR_DAYS = 4;

const DAY_MS = 86_400_000;

/** Midnight UTC of the day containing `at`. */
export function startOfUtcDay(at: Date): Date {
  return new Date(Math.floor(at.getTime() / DAY_MS) * DAY_MS);
}

/** `at` shifted by whole days. */
function addDays(at: Date, days: number): Date {
  return new Date(at.getTime() + days * DAY_MS);
}

/** The partition table name for the day starting at `day`: `readings_p20261016`. */
export function partitionName(day: Date): string {
  return `readings_p${day.toISOString().slice(0, 10).replaceAll("-", "")}`;
}

/** Throws unless the settings are usable: retention at or above the floor, at least two days ahead. */
export function assertPartitionSettings(settings: PartitionSettings): void {
  if (!Number.isInteger(settings.retentionDays) || settings.retentionDays < RETENTION_FLOOR_DAYS) {
    throw new Error(`RETENTION_DAYS must be a whole number of at least ${RETENTION_FLOOR_DAYS} (a gateway can replay readings about 69 h old); got ${settings.retentionDays}`);
  }
  if (!Number.isInteger(settings.aheadDays) || settings.aheadDays < 2) {
    throw new Error(`PARTITIONS_AHEAD_DAYS must be a whole number of at least 2; got ${settings.aheadDays}`);
  }
}

/** The oldest `measured_at` ingest accepts at `now`: T − (D − 1) days. Anything older is `too_old`. */
export function acceptedSince(now: Date, retentionDays: number): Date {
  return addDays(startOfUtcDay(now), -(retentionDays - 1));
}

/** The days the kept window spans at `now`, as partition ranges: [T − D, T + A]. */
export function keptDays(now: Date, settings: PartitionSettings): PartitionRange[] {
  const today = startOfUtcDay(now);
  const days: PartitionRange[] = [];
  for (let offset = -settings.retentionDays; offset <= settings.aheadDays; offset++) {
    const from = addDays(today, offset);
    days.push({ name: partitionName(from), from, to: addDays(from, 1) });
  }
  return days;
}

/** What maintenance must do at `now`: create every missing day of the kept window, drop partitions older than it. */
export function planPartitions(existing: PartitionRange[], now: Date, settings: PartitionSettings): { create: PartitionRange[]; drop: PartitionRange[] } {
  const have = new Set(existing.map((p) => p.from.getTime()));
  const oldestKept = addDays(startOfUtcDay(now), -settings.retentionDays);
  return {
    create: keptDays(now, settings).filter((day) => !have.has(day.from.getTime())),
    drop: existing.filter((p) => p.to.getTime() <= oldestKept.getTime()),
  };
}

/** True if `existing` covers [from, to) without a hole. */
export function covers(existing: PartitionRange[], from: Date, to: Date): boolean {
  const sorted = [...existing].sort((a, b) => a.from.getTime() - b.from.getTime());
  let reached = from.getTime();
  for (const p of sorted) {
    if (p.from.getTime() > reached) break;
    reached = Math.max(reached, p.to.getTime());
    if (reached >= to.getTime()) return true;
  }
  return reached >= to.getTime();
}

/** Parses one of Postgres's timestamptz literals (`2026-10-16 00:00:00+00`, `… 02:00:00+02`, `+05:30`) exactly. */
function parseTimestamptz(literal: string): Date {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?)([+-]\d{2})(?::?(\d{2}))?$/.exec(literal);
  if (!m) throw new Error(`unexpected timestamptz: ${literal}`);
  const at = new Date(`${m[1]}T${m[2]}${m[3]}:${m[4] ?? "00"}`);
  if (Number.isNaN(at.getTime())) throw new Error(`unexpected timestamptz: ${literal}`);
  return at;
}

/** Parses Postgres's partition bound expression: `FOR VALUES FROM ('…') TO ('…')`. */
export function parseBound(expression: string): { from: Date; to: Date } {
  const match = /FROM \('([^']+)'\) TO \('([^']+)'\)/.exec(expression);
  if (!match) throw new Error(`unexpected partition bound: ${expression}`);
  return { from: parseTimestamptz(match[1] as string), to: parseTimestamptz(match[2] as string) };
}

/** The partitions `readings` has now. */
export async function listPartitions(client: pg.ClientBase): Promise<PartitionRange[]> {
  const { rows } = await client.query<{ name: string; bound: string }>(
    `SELECT c.relname AS name, pg_get_expr(c.relpartbound, c.oid) AS bound
     FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_class p ON p.oid = i.inhparent
     WHERE p.relname = 'readings'`);
  return rows.map((row) => ({ name: row.name, ...parseBound(row.bound) }));
}

/** Creates the partition for one day; a no-op if it exists. */
async function createPartition(client: pg.ClientBase, day: PartitionRange): Promise<void> {
  await client.query(`CREATE TABLE IF NOT EXISTS ${day.name} PARTITION OF readings FOR VALUES FROM ('${day.from.toISOString()}') TO ('${day.to.toISOString()}')`);
}

/**
 * Brings `readings` to the kept window at `now`: creates the missing days, drops the expired ones (retention as
 * DROP TABLE, instant and without vacuum debt). Returns what it did. Safe to run concurrently with inserts: a
 * dropped partition holds only days ingest no longer accepts.
 */
export async function maintainPartitions(client: pg.ClientBase, settings: PartitionSettings, now: Date): Promise<{ created: string[]; dropped: string[] }> {
  assertPartitionSettings(settings);
  const plan = planPartitions(await listPartitions(client), now, settings);
  for (const day of plan.create) await createPartition(client, day);
  for (const old of plan.drop) await client.query(`DROP TABLE IF EXISTS ${old.name}`);
  return { created: plan.create.map((p) => p.name), dropped: plan.drop.map((p) => p.name) };
}

/** Creates daily partitions for every day in [from, to), whatever the window: for tests that store fixed past dates. */
export async function ensurePartitions(client: pg.ClientBase, from: Date, to: Date): Promise<void> {
  for (let day = startOfUtcDay(from); day.getTime() < to.getTime(); day = addDays(day, 1)) {
    await createPartition(client, { name: partitionName(day), from: day, to: addDays(day, 1) });
  }
}
