-- Backing table for asynchronous author-analytics recomputes.
--
-- The previous design ran the full per-author analytics rebuild
-- (author_activity_daily + 5 windowed projections × 3 windows) inline
-- inside derive_event_bundle for EVERY incoming event from a pubkey. For
-- hot authors receiving thousands of reactions per hour, that meant
-- thousands of identical full-history aggregations per hour, all queued
-- behind a per-pubkey advisory lock — production observed throughput
-- collapse from ~thousands to tens of jobs per 5 minutes.
--
-- The new design: derive_event_bundle does a cheap upsert into this table
-- marking each affected pubkey as dirty. A separate background sweeper in
-- the worker process drains a batch of dirty pubkeys at a time and runs
-- the heavy rebuild ONCE per pubkey per cycle, naturally coalescing
-- bursts into a single rebuild.
--
-- The (marked_at, pubkey) ordering lets the sweeper claim the
-- oldest-marked rows first via FOR UPDATE SKIP LOCKED so multiple sweeper
-- workers can drain the table in parallel without contention.

CREATE TABLE IF NOT EXISTS pending_author_analytics_recomputes (
    pubkey TEXT PRIMARY KEY,
    marked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_author_analytics_marked_at
    ON pending_author_analytics_recomputes (marked_at ASC, pubkey ASC);
