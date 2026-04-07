ALTER TABLE trust_runs
    ADD COLUMN IF NOT EXISTS current_phase TEXT CHECK (current_phase IN ('sync', 'compute', 'promote')),
    ADD COLUMN IF NOT EXISTS sync_job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS compute_job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS promote_job_id BIGINT REFERENCES jobs(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS phase_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS phase_finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS phase_last_error TEXT;

CREATE TABLE IF NOT EXISTS trust_scores_global_stage (
    run_id BIGINT NOT NULL REFERENCES trust_runs(id) ON DELETE CASCADE,
    pubkey TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    rank BIGINT NOT NULL CHECK (rank > 0),
    derivation_name TEXT NOT NULL,
    target_version INTEGER NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, pubkey)
);

CREATE INDEX IF NOT EXISTS idx_trust_scores_global_stage_run_rank
    ON trust_scores_global_stage (run_id, rank ASC);
