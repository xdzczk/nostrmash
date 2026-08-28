-- Anti-farming inputs for hot conversations: unique non-root-author repliers
-- per window (optionally trust-weighted at projection time). Raw replies_24h /
-- replies_7d stay as display counters; velocity scoring moves to these columns
-- so a single account replying to itself in a loop buys zero velocity.
ALTER TABLE thread_summaries
    ADD COLUMN IF NOT EXISTS reply_weight_24h DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (reply_weight_24h >= 0),
    ADD COLUMN IF NOT EXISTS reply_weight_7d DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (reply_weight_7d >= 0);

CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_weight_24h
    ON thread_summaries (reply_weight_24h DESC, last_activity_at DESC, root_event_id ASC)
    WHERE reply_weight_24h > 0;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_weight_7d
    ON thread_summaries (reply_weight_7d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE reply_weight_7d > 0;
