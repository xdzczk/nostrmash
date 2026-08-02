-- Incremental author/profile stats support.
--
-- Steady-state counters are maintained with O(1) idempotent deltas instead of
-- full-history recomputes. applied_stat_deltas is the exactly-once ledger that
-- makes at-least-once bundle retries safe. The fine-grained daily tables feed
-- cheap windowed roll-ups for topics/media/hourly projections.

CREATE TABLE IF NOT EXISTS applied_stat_deltas (
    event_id TEXT NOT NULL,
    projection TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, projection)
);

CREATE INDEX IF NOT EXISTS idx_applied_stat_deltas_applied_at
    ON applied_stat_deltas (applied_at ASC);

CREATE TABLE IF NOT EXISTS author_hashtag_daily (
    pubkey TEXT NOT NULL,
    activity_date DATE NOT NULL,
    hashtag TEXT NOT NULL,
    usage_count BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, activity_date, hashtag)
);

CREATE INDEX IF NOT EXISTS idx_author_hashtag_daily_pubkey_date
    ON author_hashtag_daily (pubkey, activity_date DESC);

CREATE TABLE IF NOT EXISTS author_media_daily (
    pubkey TEXT NOT NULL,
    activity_date DATE NOT NULL,
    total_posts BIGINT NOT NULL DEFAULT 0,
    with_image_count BIGINT NOT NULL DEFAULT 0,
    with_video_count BIGINT NOT NULL DEFAULT 0,
    with_link_count BIGINT NOT NULL DEFAULT 0,
    with_article_count BIGINT NOT NULL DEFAULT 0,
    text_only_count BIGINT NOT NULL DEFAULT 0,
    total_attachment_count BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, activity_date)
);

CREATE INDEX IF NOT EXISTS idx_author_media_daily_pubkey_date
    ON author_media_daily (pubkey, activity_date DESC);

CREATE TABLE IF NOT EXISTS author_hourly_activity (
    pubkey TEXT NOT NULL,
    activity_date DATE NOT NULL,
    day_of_week SMALLINT NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    note_count BIGINT NOT NULL DEFAULT 0,
    reply_count BIGINT NOT NULL DEFAULT 0,
    engagement_received BIGINT NOT NULL DEFAULT 0,
    reply_received BIGINT NOT NULL DEFAULT 0,
    reaction_received BIGINT NOT NULL DEFAULT 0,
    repost_received BIGINT NOT NULL DEFAULT 0,
    zap_received BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, activity_date, day_of_week, hour_of_day),
    CONSTRAINT author_hourly_activity_day_chk CHECK (day_of_week BETWEEN 0 AND 6),
    CONSTRAINT author_hourly_activity_hour_chk CHECK (hour_of_day BETWEEN 0 AND 23)
);

CREATE INDEX IF NOT EXISTS idx_author_hourly_activity_pubkey_date
    ON author_hourly_activity (pubkey, activity_date DESC);
