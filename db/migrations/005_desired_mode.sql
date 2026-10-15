-- 005: desired state for commands (slice 7, decisions.md S7-1).
-- gateways.mode is the *reported* mode, from the gateway's own status. desired_mode is what the cloud asked for, and
-- desired_at is when. The retained command on the broker becomes a copy of desired_mode that the API's reconciler
-- rebuilds, so a fresh broker no longer loses a command a gateway hasn't applied yet.
ALTER TABLE gateways ADD COLUMN desired_mode text;
ALTER TABLE gateways ADD COLUMN desired_at timestamptz;
