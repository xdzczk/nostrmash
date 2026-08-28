-- Support the deletion_events ledger retention sweep (orphan tombstones).
--
-- The ledger was made durable in 000050 so tombstones survive raw kind-5
-- purges, but it had no retention at all: production accumulated 24.6M rows
-- (~14 GB) against a 6.5M-row events table, i.e. the vast majority of
-- tombstones target events we never stored and no read path ever consults
-- them. The new PurgeOrphanDeletionLedger sweep scans the ledger in
-- (created_at, event_id) keyset windows and deletes old tombstones whose
-- target event is absent; this index drives that scan and its exact
-- composite cursor (the existing indexes lead with event_id /
-- target_event_id and cannot).
--
-- Plain CREATE INDEX (not CONCURRENTLY) because migrations run inside a
-- transaction; operators may CREATE INDEX CONCURRENTLY out-of-band on live
-- databases before this migration lands.
CREATE INDEX IF NOT EXISTS idx_deletion_events_created_at
    ON deletion_events (created_at, event_id);

-- The sweep will delete tens of millions of rows over its first passes and a
-- steady trickle afterwards. Default percentage autovacuum thresholds lag on
-- a table this size (0.2 * 24.6M ~= 5M dead tuples before a vacuum); use the
-- fixed absolute thresholds pattern from 000057/000062/000066 so dead space
-- is reclaimed promptly.
ALTER TABLE deletion_events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 50000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 50000,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_vacuum_cost_limit = 2000
);
