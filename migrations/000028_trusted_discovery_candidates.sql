CREATE TABLE IF NOT EXISTS trusted_note_discovery_candidates (
    event_id TEXT PRIMARY KEY REFERENCES note_discovery_stats(event_id) ON DELETE CASCADE,
    author_pubkey TEXT NOT NULL,
    min_hops INTEGER NULL CHECK (min_hops >= 0),
    trust_score DOUBLE PRECISION NULL,
    source_run_id BIGINT NULL REFERENCES trust_runs(id) ON DELETE SET NULL,
    trust_snapshot_refreshed_at TIMESTAMPTZ NULL,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trusted_note_discovery_hops_score
    ON trusted_note_discovery_candidates (min_hops ASC NULLS LAST, trust_score DESC NULLS LAST, event_id ASC);

CREATE TABLE IF NOT EXISTS trusted_profile_discovery_candidates (
    pubkey TEXT PRIMARY KEY REFERENCES profile_discovery_stats(pubkey) ON DELETE CASCADE,
    min_hops INTEGER NULL CHECK (min_hops >= 0),
    trust_score DOUBLE PRECISION NULL,
    source_run_id BIGINT NULL REFERENCES trust_runs(id) ON DELETE SET NULL,
    trust_snapshot_refreshed_at TIMESTAMPTZ NULL,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trusted_profile_discovery_hops_score
    ON trusted_profile_discovery_candidates (min_hops ASC NULLS LAST, trust_score DESC NULLS LAST, pubkey ASC);

CREATE TABLE IF NOT EXISTS trusted_discovery_projection_state (
    projection_name TEXT PRIMARY KEY,
    trust_snapshot_refreshed_at TIMESTAMPTZ NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    derivation_version INTEGER NOT NULL DEFAULT 1
);
