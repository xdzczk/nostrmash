CREATE TABLE IF NOT EXISTS note_discovery_stats (
    event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    author_pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    reply_count BIGINT NOT NULL DEFAULT 0 CHECK (reply_count >= 0),
    repost_count BIGINT NOT NULL DEFAULT 0 CHECK (repost_count >= 0),
    reaction_count BIGINT NOT NULL DEFAULT 0 CHECK (reaction_count >= 0),
    zap_count BIGINT NOT NULL DEFAULT 0 CHECK (zap_count >= 0),
    zap_msats BIGINT NOT NULL DEFAULT 0 CHECK (zap_msats >= 0),
    score_24h DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_7d DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_24h_rank
    ON note_discovery_stats (score_24h DESC, created_at DESC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_7d_rank
    ON note_discovery_stats (score_7d DESC, created_at DESC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_created_at
    ON note_discovery_stats (created_at DESC, event_id DESC);
