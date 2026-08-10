-- Denormalized hop+score view of trust state, one row per pubkey that appears
-- in trust_graph_snapshot and/or trust_scores_global. Rebuildable from those
-- tables; maintained by trust promote and trust_graph_snapshot refresh.
CREATE TABLE IF NOT EXISTS trust_pubkeys_latest (
    pubkey TEXT PRIMARY KEY,
    min_hops INTEGER CHECK (min_hops IS NULL OR min_hops >= 0),
    is_seed BOOLEAN NOT NULL DEFAULT false,
    score DOUBLE PRECISION,
    rank BIGINT CHECK (rank IS NULL OR rank > 0),
    source_run_id BIGINT REFERENCES trust_runs(id) ON DELETE SET NULL,
    computed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trust_pubkeys_latest_rank
    ON trust_pubkeys_latest (rank ASC)
    WHERE rank IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trust_pubkeys_latest_hops
    ON trust_pubkeys_latest (min_hops ASC, pubkey ASC)
    WHERE min_hops IS NOT NULL;
