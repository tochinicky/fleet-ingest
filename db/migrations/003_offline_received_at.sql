-- 003: when ingest last received an `offline` status (usually the Last Will), by ingest's clock (decisions.md S5-6).
-- A reading counts as proof that a gateway is alive only if ingest received it after this: readings sent before a
-- link died must not keep a dead gateway "online" for another minute.
ALTER TABLE gateways ADD COLUMN last_offline_received_at timestamptz;
UPDATE gateways SET last_offline_received_at = last_status_received_at WHERE last_status = 'offline';
