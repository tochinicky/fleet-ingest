-- 004: what the SLO needs (slice 6, decisions.md S6-1).
-- readings.stored_at: when the reading became queryable, by the database's clock (its transaction time). received_at
-- is ingest's receive time, before any retries, so during a database outage it says nothing about when the reading
-- could be queried.
ALTER TABLE readings ADD COLUMN stored_at timestamptz NOT NULL DEFAULT now();
UPDATE readings SET stored_at = received_at;
CREATE INDEX readings_stored_at_idx ON readings (stored_at);

-- gateways.came_back_at: ingest's receive time of the first `online` status after the latest `offline`. Readings
-- measured before it were buffered while the gateway was disconnected (backfill), and don't count against the SLO.
ALTER TABLE gateways ADD COLUMN came_back_at timestamptz;
