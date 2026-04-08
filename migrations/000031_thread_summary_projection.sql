CREATE TABLE IF NOT EXISTS thread_summaries (
    root_event_id TEXT PRIMARY KEY REFERENCES events (id) ON DELETE CASCADE,
    reply_count BIGINT NOT NULL DEFAULT 0 CHECK (reply_count >= 0),
    participant_count INTEGER NOT NULL DEFAULT 1 CHECK (participant_count >= 0),
    max_depth INTEGER NOT NULL DEFAULT 0 CHECK (max_depth >= 0),
    last_activity_at BIGINT NOT NULL,
    replies_24h BIGINT NOT NULL DEFAULT 0 CHECK (replies_24h >= 0),
    replies_7d BIGINT NOT NULL DEFAULT 0 CHECK (replies_7d >= 0),
    derivation_version INTEGER NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_thread_summaries_last_activity
    ON thread_summaries (last_activity_at DESC, root_event_id ASC);
