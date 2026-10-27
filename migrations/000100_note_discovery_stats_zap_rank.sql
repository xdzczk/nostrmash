SET LOCAL statement_timeout = '10min';

-- Most-zapped pages walk zap volume descending and keep notes inside the
-- requested window. Sorting every note in that window exceeded the websocket
-- request budget, so the feed came back empty.
CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_zap_rank
    ON note_discovery_stats (zap_msats DESC, created_at DESC, event_id ASC)
    WHERE zap_msats > 0;
