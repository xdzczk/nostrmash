CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_24h
    ON thread_summaries (replies_24h DESC, last_activity_at DESC, root_event_id ASC)
    WHERE replies_24h > 0;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_7d
    ON thread_summaries (replies_7d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE replies_7d > 0;
