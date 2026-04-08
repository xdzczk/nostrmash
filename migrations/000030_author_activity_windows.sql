CREATE TABLE IF NOT EXISTS author_activity_windows (
    pubkey TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    day_of_week SMALLINT NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    engagement_received BIGINT NOT NULL DEFAULT 0,
    reply_received BIGINT NOT NULL DEFAULT 0,
    reaction_received BIGINT NOT NULL DEFAULT 0,
    repost_received BIGINT NOT NULL DEFAULT 0,
    zap_received BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, window_days, day_of_week, hour_of_day),
    CONSTRAINT author_activity_windows_window_days_chk CHECK (window_days IN (7, 30, 90)),
    CONSTRAINT author_activity_windows_day_chk CHECK (day_of_week BETWEEN 0 AND 6),
    CONSTRAINT author_activity_windows_hour_chk CHECK (hour_of_day BETWEEN 0 AND 23)
);

CREATE INDEX IF NOT EXISTS idx_author_activity_windows_rank
    ON author_activity_windows (pubkey, window_days, day_of_week, hour_of_day);

CREATE TABLE IF NOT EXISTS author_posting_patterns (
    pubkey TEXT NOT NULL,
    window_days INTEGER NOT NULL,
    day_of_week SMALLINT NOT NULL,
    hour_of_day SMALLINT NOT NULL,
    post_count BIGINT NOT NULL DEFAULT 0,
    note_count BIGINT NOT NULL DEFAULT 0,
    reply_count BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, window_days, day_of_week, hour_of_day),
    CONSTRAINT author_posting_patterns_window_days_chk CHECK (window_days IN (7, 30, 90)),
    CONSTRAINT author_posting_patterns_day_chk CHECK (day_of_week BETWEEN 0 AND 6),
    CONSTRAINT author_posting_patterns_hour_chk CHECK (hour_of_day BETWEEN 0 AND 23)
);

CREATE INDEX IF NOT EXISTS idx_author_posting_patterns_rank
    ON author_posting_patterns (pubkey, window_days, day_of_week, hour_of_day);
