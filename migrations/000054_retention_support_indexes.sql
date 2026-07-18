-- Storage discipline Phase 4: supporting index for the new retention loops.
--
-- idx_author_recent_events_created_at serves the age pass of
-- PruneAuthorRecentEvents (global created_at cutoff; the existing
-- idx_author_recent_events_order leads with author_pubkey and cannot).
--
-- PurgeStaleEventRelays needs no new index: the seen_at cutoff scan is
-- served by idx_event_relays_seen_at_pubkey from migration 000045.
CREATE INDEX IF NOT EXISTS idx_author_recent_events_created_at
    ON author_recent_events (created_at ASC);
