-- 007: readings partitioned by day on measured_at (slice 8, decisions.md S8-2).
-- Retention becomes DROP TABLE of a day's partition, and each window query reads only its days.
--
-- The key changes (a D4 change, decided 2026-10-16): Postgres requires the partition key in the primary key, so it
-- is (device_id, epoch, seq, measured_at). A redelivery is the same payload, with the same measured_at, so it meets
-- the same partition and the same key: still stored once. Partitioning by stored_at would have broken that, because
-- a redelivery is stored later and would land in another partition. What changes: a different reading that reuses a
-- key with another timestamp (a producer bug, S2-6) is now stored as a second row instead of dropped, and a detector
-- counts and alerts on it (S8-2).
--
-- There is no default partition: partitions are created ahead of time, ingest dead-letters what retention no longer
-- holds as too_old, and an insert with nowhere to go is impossible by construction. The maintenance job keeps the
-- window; this migration creates the days the existing rows need plus a week around today.
--
-- It moves every existing row inside the migration's transaction, which blocks writes while it runs: fine for the
-- lab. In production, stage it: create the partitioned table beside the old one, copy in batches by day, switch
-- names in one short transaction, and drop the old table later.
--
-- Index notes: the covering (device_id, measured_at) INCLUDE (epoch, seq) index of 006 is created on the parent, so
-- every partition gets it and the per-device gap reads stay index-only. 006's autovacuum_analyze_scale_factor can't
-- be set on a partitioned parent and isn't needed: each day is its own small table, analyzed as it grows.

ALTER TABLE readings RENAME TO readings_unpartitioned;
ALTER INDEX readings_pkey RENAME TO readings_unpartitioned_pkey;
ALTER INDEX readings_device_measured_idx RENAME TO readings_unpartitioned_device_measured_idx;
ALTER INDEX readings_stored_at_idx RENAME TO readings_unpartitioned_stored_at_idx;

CREATE TABLE readings (
    device_id        text             NOT NULL,
    epoch            bigint           NOT NULL,
    seq              bigint           NOT NULL,
    measured_at      timestamptz      NOT NULL,
    received_at      timestamptz      NOT NULL,
    kind             text             NOT NULL,
    pv_power_w       double precision,
    battery_power_w  double precision,
    grid_power_w     double precision,
    ev_power_w       double precision,
    energy_wh        double precision,
    soc_percent      double precision,
    charge_limit_a   double precision,
    stored_at        timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (device_id, epoch, seq, measured_at)
) PARTITION BY RANGE (measured_at);

CREATE INDEX readings_device_measured_idx ON readings (device_id, measured_at) INCLUDE (epoch, seq);
CREATE INDEX readings_stored_at_idx ON readings (stored_at);

DO $$
DECLARE
  first_day date := LEAST(
    COALESCE((SELECT min(measured_at AT TIME ZONE 'UTC')::date FROM readings_unpartitioned), (now() AT TIME ZONE 'UTC')::date),
    (now() AT TIME ZONE 'UTC')::date - 1);
  last_day date := GREATEST(
    COALESCE((SELECT max(measured_at AT TIME ZONE 'UTC')::date FROM readings_unpartitioned), (now() AT TIME ZONE 'UTC')::date),
    (now() AT TIME ZONE 'UTC')::date + 7);
  d date;
BEGIN
  d := first_day;
  WHILE d <= last_day LOOP
    EXECUTE format(
      'CREATE TABLE readings_p%s PARTITION OF readings FOR VALUES FROM (%L) TO (%L)',
      to_char(d, 'YYYYMMDD'), d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
    d := d + 1;
  END LOOP;
END $$;

INSERT INTO readings (device_id, epoch, seq, measured_at, received_at, kind, pv_power_w, battery_power_w, grid_power_w,
                      ev_power_w, energy_wh, soc_percent, charge_limit_a, stored_at)
SELECT device_id, epoch, seq, measured_at, received_at, kind, pv_power_w, battery_power_w, grid_power_w,
       ev_power_w, energy_wh, soc_percent, charge_limit_a, stored_at
FROM readings_unpartitioned;

DROP TABLE readings_unpartitioned;
