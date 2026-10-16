-- 008: the health worker's snapshot (slice 8, decisions.md S8-3).
-- The health job moved out of the API into its own service, the single writer of the fleet health metrics. What the
-- API still needs from it, the fleet's open gaps as of the last gaps pass, is this one row, rewritten each pass, so
-- every API replica reports the same figures and none of them keeps a window of its own.

CREATE TABLE health_snapshot (
    -- One row: the key can only be true.
    id          boolean     PRIMARY KEY DEFAULT true CHECK (id),
    -- When the gaps pass that produced it ran: /fleet reports it as asOf.
    as_of       timestamptz NOT NULL,
    open_gaps   jsonb       NOT NULL,
    -- Which worker wrote it (its hostname), for the runbook.
    writer      text        NOT NULL,
    written_at  timestamptz NOT NULL DEFAULT now()
);
