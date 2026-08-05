-- Materialized related-profile edges for the discovery API.
-- Populated write-through by GetRelatedProfiles and optionally refreshed by ops.
CREATE TABLE IF NOT EXISTS related_profiles (
    focal_pubkey TEXT NOT NULL,
    related_pubkey TEXT NOT NULL,
    topic_overlap BIGINT NOT NULL DEFAULT 0,
    reply_adjacency BIGINT NOT NULL DEFAULT 0,
    interaction_adjacency BIGINT NOT NULL DEFAULT 0,
    quote_repost_adjacency BIGINT NOT NULL DEFAULT 0,
    reasons TEXT[] NOT NULL DEFAULT '{}',
    score BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (focal_pubkey, related_pubkey)
);

CREATE INDEX IF NOT EXISTS idx_related_profiles_focal_score
    ON related_profiles (focal_pubkey, score DESC);

CREATE INDEX IF NOT EXISTS idx_related_profiles_focal_updated
    ON related_profiles (focal_pubkey, updated_at DESC);
