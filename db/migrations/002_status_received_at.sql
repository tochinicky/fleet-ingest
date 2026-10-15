-- 002: when ingest last heard any status message from a gateway, by ingest's clock (slice 5, decisions.md S5-1).
-- last_status / last_status_at stay newer-only by the gateway's timestamp (S2-2). This column moves on every status
-- received, even one the newer-only guard rejects, so health can tell "silent for 3 minutes" without trusting the
-- gateway's clock.
ALTER TABLE gateways ADD COLUMN last_status_received_at timestamptz;
UPDATE gateways SET last_status_received_at = last_status_at WHERE last_status_at IS NOT NULL;
