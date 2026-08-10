-- Seeded trust neighborhoods: BFS-reachable members per active seed, staged
-- per trust run and published on promote. Rebuildable from follower_edges +
-- trust_seeds; optional Redis working copies are disposable.
CREATE TABLE IF NOT EXISTS trust_neighborhood_members_stage (
    run_id BIGINT NOT NULL REFERENCES trust_runs(id) ON DELETE CASCADE,
    seed_pubkey TEXT NOT NULL,
    member_pubkey TEXT NOT NULL,
    hops INTEGER NOT NULL CHECK (hops >= 0),
    weight DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (weight >= 0),
    PRIMARY KEY (run_id, seed_pubkey, member_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_trust_neighborhood_members_stage_run
    ON trust_neighborhood_members_stage (run_id);

CREATE TABLE IF NOT EXISTS trust_neighborhood_members (
    seed_pubkey TEXT NOT NULL,
    member_pubkey TEXT NOT NULL,
    hops INTEGER NOT NULL CHECK (hops >= 0),
    weight DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (weight >= 0),
    source_run_id BIGINT REFERENCES trust_runs(id) ON DELETE SET NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (seed_pubkey, member_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_trust_neighborhood_members_member
    ON trust_neighborhood_members (member_pubkey);

CREATE INDEX IF NOT EXISTS idx_trust_neighborhood_members_run
    ON trust_neighborhood_members (source_run_id);
