-- 006: fleet-wide reads of readings without a whole-table scan (slice 7, decisions.md S7-11).

-- 1. Keep the planner's statistics current. readings only grows, and its timestamps only move forward. With the
--    default analyze threshold (10% of the table changed), the histogram's last bucket falls behind the newest rows:
--    at 4.3M rows, 233,000 had changed since the last analyze and the estimate for "the last hour" was far off.
--    Analyzing after every 1% of new rows keeps it close.
ALTER TABLE readings SET (autovacuum_analyze_scale_factor = 0.01);

-- 2. Make (device_id, measured_at) covering, so a per-device read of a time range is an index-only scan. Without it,
--    each device's readings sit on hundreds of heap pages (the whole fleet's inserts interleave), and the per-device
--    gap reads did random heap I/O: the fleet's last hour took 76 s that way, against 8.7 s index-only. This replaces
--    the old index (same name), so the insert path keeps the same number of indexes, with somewhat wider entries.
--    Built inside the migration's transaction, so without CONCURRENTLY: it blocks writes while it builds, fine for
--    the lab. In production: CREATE INDEX CONCURRENTLY the new one, then DROP INDEX CONCURRENTLY the old one, each
--    outside a transaction, then rename.
CREATE INDEX readings_device_measured_cov_idx ON readings (device_id, measured_at) INCLUDE (epoch, seq);
DROP INDEX readings_device_measured_idx;
ALTER INDEX readings_device_measured_cov_idx RENAME TO readings_device_measured_idx;
