CREATE TABLE IF NOT EXISTS trust_seeds (
    pubkey TEXT PRIMARY KEY,
    is_active BOOLEAN NOT NULL DEFAULT true,
    note TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trust_runs (
    id BIGSERIAL PRIMARY KEY,
    derivation_name TEXT NOT NULL,
    target_version INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    input_follower_edges_count BIGINT NOT NULL DEFAULT 0 CHECK (input_follower_edges_count >= 0),
    score_rows_published BIGINT NOT NULL DEFAULT 0 CHECK (score_rows_published >= 0),
    redis_snapshot_ref TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trust_runs_created_desc
    ON trust_runs (created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_trust_runs_status
    ON trust_runs (status, created_at DESC);

CREATE TABLE IF NOT EXISTS trust_scores_global (
    pubkey TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL,
    rank BIGINT NOT NULL CHECK (rank > 0),
    run_id BIGINT NOT NULL REFERENCES trust_runs(id) ON DELETE CASCADE,
    derivation_name TEXT NOT NULL,
    target_version INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trust_scores_global_rank
    ON trust_scores_global (rank ASC);

CREATE INDEX IF NOT EXISTS idx_trust_scores_global_score
    ON trust_scores_global (score DESC, pubkey ASC);
