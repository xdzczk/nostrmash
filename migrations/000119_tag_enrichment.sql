-- Project the remaining tags that ingest already receives into narrow
-- tables. event_tags stays closed except for the ends tag added in 000118.

ALTER TABLE event_attributes
    ADD COLUMN IF NOT EXISTS subject TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS has_alt BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS published_at BIGINT,
    ADD COLUMN IF NOT EXISTS zap_split_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS word_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS reading_time_s INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS mention_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hashtag_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS content_length INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS event_labels (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    labeler_pubkey TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT '',
    label TEXT NOT NULL,
    is_self BOOLEAN NOT NULL DEFAULT FALSE,
    target_type TEXT NOT NULL DEFAULT 'e',
    target_value TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (event_id, labeler_pubkey, namespace, label, target_type, target_value)
);

CREATE INDEX IF NOT EXISTS idx_event_labels_target
    ON event_labels (target_type, target_value);

CREATE TABLE IF NOT EXISTS language_label_agreement_daily (
    day DATE PRIMARY KEY,
    agree BIGINT NOT NULL DEFAULT 0,
    disagree BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS custom_emoji_daily (
    day DATE NOT NULL,
    shortcode TEXT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, shortcode)
);

CREATE TABLE IF NOT EXISTS profile_external_identities (
    pubkey TEXT NOT NULL,
    platform TEXT NOT NULL,
    identity TEXT NOT NULL DEFAULT '',
    proof TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, platform)
);

CREATE TABLE IF NOT EXISTS relay_hint_daily (
    day DATE NOT NULL,
    relay_url TEXT NOT NULL,
    hint_count BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (day, relay_url)
);
