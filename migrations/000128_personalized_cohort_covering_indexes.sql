-- One-hop discovery was heap-fetching every engagement row and every
-- follow edge just to read the target. These indexes carry that target, so
-- the cohort aggregate is an index-only scan. The actor indexes that lead
-- with the actor's own event id stay: other reads order by that id.
--
-- follower_edges and the engagement tables are large. The build takes a
-- write lock, and the cluster statement_timeout would kill it.
SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_follower_edges_follower_followed
    ON follower_edges (follower_pubkey, followed_pubkey);

CREATE INDEX IF NOT EXISTS idx_reaction_events_reactor_target
    ON reaction_events (reactor_pubkey, created_at DESC)
    INCLUDE (target_event_id, target_pubkey);

CREATE INDEX IF NOT EXISTS idx_repost_events_reposter_target
    ON repost_events (reposter_pubkey, created_at DESC)
    INCLUDE (target_event_id, target_pubkey);

CREATE INDEX IF NOT EXISTS idx_reply_contributions_source_target
    ON reply_count_contributions (source_pubkey, source_created_at DESC)
    INCLUDE (target_event_id, target_pubkey)
    WHERE source_pubkey IS NOT NULL AND target_pubkey IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_zap_receipts_sender_target
    ON zap_receipts (sender_pubkey, created_at DESC)
    INCLUDE (event_id, receiver_pubkey, amount_sats);

CREATE INDEX IF NOT EXISTS idx_follower_gain_events_follower_target
    ON follower_gain_events (follower_pubkey, gained_at DESC)
    INCLUDE (followed_pubkey);
