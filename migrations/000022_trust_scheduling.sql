CREATE TABLE IF NOT EXISTS ingest_pubkey_frontier (
    pubkey TEXT PRIMARY KEY,
    source_run_id BIGINT NOT NULL,
    trust_rank BIGINT NOT NULL,
    trust_score DOUBLE PRECISION NOT NULL,
    state TEXT NOT NULL DEFAULT 'candidate',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_selected_at TIMESTAMPTZ,
    last_fetched_at TIMESTAMPTZ,
    next_eligible_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    fetch_attempts INTEGER NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    last_error_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ingest_pubkey_frontier_state_chk
        CHECK (state IN ('candidate', 'active', 'cooldown', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_ingest_pubkey_frontier_state_eligibility
    ON ingest_pubkey_frontier (state, next_eligible_at, trust_rank);

CREATE TABLE IF NOT EXISTS trust_relay_suggestions (
    relay_url TEXT PRIMARY KEY,
    weighted_score DOUBLE PRECISION NOT NULL,
    supporting_pubkeys_count INTEGER NOT NULL DEFAULT 0,
    supporting_pubkeys_sample JSONB NOT NULL DEFAULT '[]'::jsonb,
    source_run_id BIGINT,
    source_computed_at TIMESTAMPTZ,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_promoted_at TIMESTAMPTZ,
    is_recommended BOOLEAN NOT NULL DEFAULT false,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trust_relay_suggestions_recommended
    ON trust_relay_suggestions (is_recommended, weighted_score DESC);
