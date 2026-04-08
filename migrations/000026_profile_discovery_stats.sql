CREATE TABLE IF NOT EXISTS profile_discovery_stats (
    pubkey TEXT PRIMARY KEY,
    score_24h DOUBLE PRECISION NOT NULL DEFAULT 0,
    score_7d DOUBLE PRECISION NOT NULL DEFAULT 0,
    rising_score_24h DOUBLE PRECISION NOT NULL DEFAULT 0,
    rising_score_7d DOUBLE PRECISION NOT NULL DEFAULT 0,
    recent_post_count BIGINT NOT NULL DEFAULT 0 CHECK (recent_post_count >= 0),
    recent_reply_count BIGINT NOT NULL DEFAULT 0 CHECK (recent_reply_count >= 0),
    recent_engagement_received BIGINT NOT NULL DEFAULT 0 CHECK (recent_engagement_received >= 0),
    recent_zap_volume_msats BIGINT NOT NULL DEFAULT 0 CHECK (recent_zap_volume_msats >= 0),
    recent_active_days INTEGER NOT NULL DEFAULT 0 CHECK (recent_active_days >= 0),
    recent_activity_at BIGINT,
    last_scored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_24h_rank
    ON profile_discovery_stats (score_24h DESC, recent_activity_at DESC, pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_7d_rank
    ON profile_discovery_stats (score_7d DESC, recent_activity_at DESC, pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_rising_24h_rank
    ON profile_discovery_stats (rising_score_24h DESC, recent_activity_at DESC, pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_rising_7d_rank
    ON profile_discovery_stats (rising_score_7d DESC, recent_activity_at DESC, pubkey ASC);
