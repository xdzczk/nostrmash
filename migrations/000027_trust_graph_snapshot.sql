CREATE TABLE IF NOT EXISTS trust_graph_snapshot (
    pubkey TEXT PRIMARY KEY,
    min_hops INTEGER NOT NULL CHECK (min_hops >= 0),
    is_seed BOOLEAN NOT NULL DEFAULT false,
    source_run_id BIGINT REFERENCES trust_runs(id) ON DELETE SET NULL,
    refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_trust_graph_snapshot_hops
    ON trust_graph_snapshot (min_hops ASC, pubkey ASC);
