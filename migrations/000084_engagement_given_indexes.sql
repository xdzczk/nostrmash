-- Indexes for the "given" direction of the engagement denormalization
-- (migration 000082). The author-activity recompute and rebuild paths now
-- filter repost_events by reposter_pubkey and reply_count_contributions by
-- source_pubkey; without these indexes both were full sequential scans
-- (reaction_events already had the equivalent reactor index, reposts and
-- reply contributions were missed).
--
-- The database enforces a short global statement_timeout; raise it for this
-- transaction so the index builds aren't killed mid-migration.
SET LOCAL statement_timeout = '10min';

CREATE INDEX IF NOT EXISTS idx_repost_events_reposter_created
    ON repost_events (reposter_pubkey, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_reply_contributions_source_pubkey
    ON reply_count_contributions (source_pubkey, source_created_at)
    WHERE source_pubkey IS NOT NULL;
