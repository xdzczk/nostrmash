-- The three pending_* sweeper queues are tiny live sets (hundreds to low
-- thousands of rows) with extremely high UPDATE/DELETE churn: every dirty
-- mark bumps marked_at, every claim updates claimed_at/claim_token, and
-- every successful rebuild deletes the row. Percentage-based autovacuum
-- thresholds (even the tightened 0.02 from 000055) lag once the heap has
-- already bloated — production saw pending_profile_stats_recomputes grow
-- to ~2.5 GB / ~9M dead tuples against ~4k live rows, which then made
-- the FOR UPDATE SKIP LOCKED claim query thrash and the sweeper fall
-- behind kind=3 follow-list bursts.
--
-- Switch to fixed absolute thresholds so vacuum/analyze keep pace with
-- claim/delete traffic regardless of (temporarily bloated) table size.

ALTER TABLE pending_profile_stats_recomputes SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 2000
);

ALTER TABLE pending_author_analytics_recomputes SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 2000
);

ALTER TABLE pending_meilisearch_syncs SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 2000
);
