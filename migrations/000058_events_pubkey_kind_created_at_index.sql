-- Covering index for per-pubkey analytics rebuilds.
--
-- author_activity_daily / windowed engagement queries filter
--   WHERE pubkey = $1 AND kind = 1 AND created_at <= ...
-- Against idx_events_pubkey_created_at (pubkey, created_at) Postgres must
-- heap-fetch every event for the pubkey just to apply the kind predicate.
-- For whale authors that is tens of thousands of random reads and was the
-- dominant cost behind author-analytics sweeper timeouts.
--
-- Production built this with CREATE INDEX CONCURRENTLY before this migration
-- shipped; IF NOT EXISTS keeps redeploys idempotent.
CREATE INDEX IF NOT EXISTS idx_events_pubkey_kind_created_at
    ON events (pubkey, kind, created_at DESC);
