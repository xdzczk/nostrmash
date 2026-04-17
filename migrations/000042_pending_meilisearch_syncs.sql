-- Backing table for asynchronous Meilisearch index syncs.
--
-- The previous design called h.meili.SyncEvent inline as the final step
-- of derive_event_bundle for every kind=0 (profile) and kind=1 (note)
-- event. Each call goes over HTTP to Meilisearch and waits for the task
-- to be enqueued. When Meilisearch is healthy the call returns in tens
-- of milliseconds, but during ingestion bursts (or whenever Meilisearch
-- itself is busy) the per-event sync routinely hits its 30-second
-- timeout, single-handedly capping live-pool throughput at
--   live_concurrency * (60s / 30s) = ~8 events/min
-- which is what production exhibited. Even when sync succeeds, several
-- hundred ms per event is enough to dominate the bundle's runtime once
-- all the cheap projections are inlined.
--
-- The new design: derive_event_bundle does a cheap upsert into this
-- table marking the event as needing a sync. A separate background
-- sweeper in the worker process claims a batch of pending events at a
-- time using FOR UPDATE SKIP LOCKED, batches them into a single
-- multi-document Meilisearch upsert per index, and removes them on
-- success. Failures leave the row in place so the next cycle retries.
--
-- We key the table on event_id (rather than (event_id, kind)) because
-- the sync logic re-derives kind from the events row when processing.
-- A coalesced design (one row per affected pubkey for kind=0, one per
-- event for kind=1) is unnecessary because Meilisearch upserts are
-- idempotent — duplicate work for the same event between marker and
-- drain is correctness-safe, just slightly wasteful.

CREATE TABLE IF NOT EXISTS pending_meilisearch_syncs (
    event_id TEXT PRIMARY KEY,
    marked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_meilisearch_syncs_marked_at
    ON pending_meilisearch_syncs (marked_at ASC, event_id ASC);
