CREATE TABLE IF NOT EXISTS event_hashtags (
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    author_pubkey TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    hashtag TEXT NOT NULL,
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, hashtag)
);

CREATE INDEX IF NOT EXISTS idx_event_hashtags_hashtag_created_at
    ON event_hashtags (hashtag, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_event_hashtags_created_at
    ON event_hashtags (created_at DESC);
