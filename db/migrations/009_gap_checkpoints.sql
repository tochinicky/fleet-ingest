-- 009: per-gateway gap-explanation checkpoints (slice 8, decisions.md S8-4).
-- droppedTelemetry is a lifetime counter, so explaining a window's gaps means explaining every gap the gateway ever
-- had, oldest first. A checkpoint is the explanation state after every gap that started before explained_through, so
-- a gaps pass reads only the gaps since then. It's exact: the health worker recomputes a gateway from scratch whenever
-- anything that could change those gaps happened (core/gapCheckpoint.ts says what, and how it's noticed).
-- Written only by the health worker. A lost or stale row costs one recompute, never a wrong answer. Derived data,
-- so no foreign key to gateways: an orphan row is harmless, and a key would tie every gateways cleanup to this cache.

CREATE TABLE gap_checkpoints (
    gateway_id          text        PRIMARY KEY,
    -- T: every gap whose reading-before was measured earlier than this is folded into budget_left and stopped.
    explained_through   timestamptz NOT NULL,
    budget_left         bigint      NOT NULL,
    stopped             boolean     NOT NULL,
    -- The counter the state was computed with: a different counter means recompute.
    dropped_telemetry   bigint      NOT NULL,
    -- The oldest kept partition's start then: retention moving it means the oldest gaps are gone.
    retained_from       timestamptz,
    -- Readings stored after this (less the cursor overlap) are checked against the checkpoint at the next pass.
    verified_stored_at  timestamptz NOT NULL,
    computed_at         timestamptz NOT NULL DEFAULT now()
);
