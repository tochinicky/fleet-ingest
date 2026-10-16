/**
 * Ingest's startup check that `readings` has a partition for every reading it could accept (S8-2): from its
 * retention cutoff ({@link acceptedSince}) to a day ahead, which is far beyond the 5-minute future-skew limit. Without
 * a default partition, a missing day would make inserts fail; this check makes that a loud refusal to start, after
 * which `restart: always` retries until partition maintenance has caught up, rather than something found mid-batch.
 */
import type pg from "pg";
import { acceptedSince, covers, listPartitions } from "@fleet-ingest/db";

/** Throws, naming the missing range, unless the partitions cover what ingest may accept at `now`. */
export async function assertPartitionsCover(pool: pg.Pool, retentionDays: number, now: Date): Promise<void> {
  const client = await pool.connect();
  try {
    const partitions = await listPartitions(client);
    const from = acceptedSince(now, retentionDays);
    const to = new Date(now.getTime() + 86_400_000);
    if (!covers(partitions, from, to)) {
      throw new Error(`readings has no partition for part of [${from.toISOString()}, ${to.toISOString()}): partition maintenance hasn't run, or RETENTION_DAYS differs between ingest and maintenance`);
    }
  } finally {
    client.release();
  }
}
