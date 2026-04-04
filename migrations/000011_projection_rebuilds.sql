CREATE TABLE IF NOT EXISTS derivation_active_versions (
    derivation_name TEXT PRIMARY KEY,
    active_version INTEGER NOT NULL CHECK (active_version > 0),
    target_version INTEGER NOT NULL CHECK (target_version > 0),
    description TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS projection_rebuild_runs (
    id BIGSERIAL PRIMARY KEY,
    derivation_name TEXT NOT NULL REFERENCES derivation_active_versions (derivation_name) ON DELETE CASCADE,
    target_version INTEGER NOT NULL CHECK (target_version > 0),
    scope_type TEXT NOT NULL CHECK (scope_type IN ('full', 'event', 'pubkey', 'time_range')),
    scope_event_id TEXT,
    scope_pubkey TEXT,
    scope_start_created_at BIGINT,
    scope_end_created_at BIGINT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    job_id BIGINT REFERENCES jobs (id) ON DELETE SET NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (scope_type = 'full' AND scope_event_id IS NULL AND scope_pubkey IS NULL AND scope_start_created_at IS NULL AND scope_end_created_at IS NULL)
        OR
        (scope_type = 'event' AND scope_event_id IS NOT NULL AND scope_pubkey IS NULL AND scope_start_created_at IS NULL AND scope_end_created_at IS NULL)
        OR
        (scope_type = 'pubkey' AND scope_event_id IS NULL AND scope_pubkey IS NOT NULL AND scope_start_created_at IS NULL AND scope_end_created_at IS NULL)
        OR
        (scope_type = 'time_range' AND scope_event_id IS NULL AND scope_pubkey IS NULL AND scope_start_created_at IS NOT NULL AND scope_end_created_at IS NOT NULL AND scope_start_created_at <= scope_end_created_at)
    )
);

CREATE INDEX IF NOT EXISTS idx_projection_rebuild_runs_derivation_created
    ON projection_rebuild_runs (derivation_name, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_projection_rebuild_runs_status
    ON projection_rebuild_runs (status, created_at DESC);
