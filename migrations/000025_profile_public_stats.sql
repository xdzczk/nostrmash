CREATE TABLE IF NOT EXISTS profile_public_stats (
    pubkey TEXT PRIMARY KEY,
    follower_count BIGINT NOT NULL DEFAULT 0,
    following_count BIGINT NOT NULL DEFAULT 0,
    note_count BIGINT NOT NULL DEFAULT 0,
    reply_count BIGINT NOT NULL DEFAULT 0,
    recent_activity_at BIGINT,
    derivation_version INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_profile_public_stats_recent_activity
    ON profile_public_stats (recent_activity_at DESC NULLS LAST, pubkey ASC);
