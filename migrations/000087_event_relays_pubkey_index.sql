-- Per-author relay footprint index.
--
-- GET /api/v1/authors/{pubkey}/analytics/summary (store/read
-- author_analytics_relays_media.go) filters event_relays by pubkey to count
-- distinct relays / events and rank the author's top relays. event_relays
-- carries pubkey since migration 000045 but every existing index leads with
-- seen_at or relay_url, so both queries were parallel sequential scans of
-- the whole table (~12M rows, ~5-6s each in production => 12s per request).
--
-- (pubkey, relay_url, event_id) covers both statements: the equality on
-- pubkey, GROUP BY / COUNT(DISTINCT relay_url), and COUNT(DISTINCT event_id)
-- are all answerable from the index alone.
--
-- Production built this with CREATE INDEX CONCURRENTLY before this migration
-- shipped, so IF NOT EXISTS makes it a no-op there. Fresh databases build it
-- here; raise the short cluster statement_timeout so the build is not killed.
SET LOCAL statement_timeout = '10min';

CREATE INDEX IF NOT EXISTS idx_event_relays_pubkey_relay_event
    ON event_relays (pubkey, relay_url, event_id);
