-- Storage governor cross-process state.
--
-- The worker computes a single storage-pressure level from
-- pg_database_size / configured capacity budget and writes it here. Other
-- runtime roles (ingestor, hydration job handler) read this row to decide
-- whether to throttle candidate-expanding ingest or refuse new hydration runs.
--
-- This is a single-row table (id is a singleton boolean). It is operational
-- state, not canonical data; it is safe to truncate/reset at any time.
CREATE TABLE IF NOT EXISTS storage_pressure_state (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE,
    level INTEGER NOT NULL DEFAULT 0,
    ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    database_bytes BIGINT NOT NULL DEFAULT 0,
    capacity_bytes BIGINT NOT NULL DEFAULT 0,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT storage_pressure_state_singleton CHECK (id = TRUE)
);

INSERT INTO storage_pressure_state (id) VALUES (TRUE)
ON CONFLICT (id) DO NOTHING;
