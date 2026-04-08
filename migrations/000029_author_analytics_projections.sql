CREATE TABLE IF NOT EXISTS author_activity_daily (
    pubkey TEXT NOT NULL,
    activity_date DATE NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    note_count BIGINT NOT NULL DEFAULT 0,
    reply_count BIGINT NOT NULL DEFAULT 0,
    engagement_received BIGINT NOT NULL DEFAULT 0,
    engagement_given BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, activity_date)
);

CREATE INDEX IF NOT EXISTS idx_author_activity_daily_pubkey_date
    ON author_activity_daily (pubkey, activity_date DESC);

CREATE TABLE IF NOT EXISTS author_engagement_stats (
    pubkey TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    note_count BIGINT NOT NULL DEFAULT 0,
    reply_count BIGINT NOT NULL DEFAULT 0,
    active_days INTEGER NOT NULL DEFAULT 0,
    engagement_received BIGINT NOT NULL DEFAULT 0,
    engagement_given BIGINT NOT NULL DEFAULT 0,
    cadence_posts_per_day DOUBLE PRECISION NOT NULL DEFAULT 0,
    cadence_posts_per_active_day DOUBLE PRECISION NOT NULL DEFAULT 0,
    recent_activity_at BIGINT,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, window_days),
    CONSTRAINT author_engagement_stats_window_days_chk CHECK (window_days IN (7, 30, 90))
);

CREATE TABLE IF NOT EXISTS author_topic_stats (
    pubkey TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    hashtag TEXT NOT NULL,
    usage_count BIGINT NOT NULL DEFAULT 0,
    active_days INTEGER NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, window_days, hashtag),
    CONSTRAINT author_topic_stats_window_days_chk CHECK (window_days IN (7, 30, 90))
);

CREATE INDEX IF NOT EXISTS idx_author_topic_stats_rank
    ON author_topic_stats (pubkey, window_days, usage_count DESC, hashtag ASC);

CREATE TABLE IF NOT EXISTS author_media_mix_stats (
    pubkey TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    total_posts BIGINT NOT NULL DEFAULT 0,
    with_image_count BIGINT NOT NULL DEFAULT 0,
    with_video_count BIGINT NOT NULL DEFAULT 0,
    with_link_count BIGINT NOT NULL DEFAULT 0,
    text_only_count BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, window_days),
    CONSTRAINT author_media_mix_stats_window_days_chk CHECK (window_days IN (7, 30, 90))
);
