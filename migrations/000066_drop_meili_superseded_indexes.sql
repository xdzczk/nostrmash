-- Single-server remediation Phase 2: drop full-text / ranking indexes that
-- production no longer uses (pg_stat_user_indexes.idx_scan = 0) after the
-- Meilisearch search path became authoritative.
--
-- Kept intentionally:
--   - idx_event_tags_tag_name_value (documented covering index for e/p lookups)
--   - idx_events_kind_created_at (still has non-zero scans)
--
-- Plain DROP INDEX (not CONCURRENTLY) because migrations run inside a
-- transaction; operators may also DROP INDEX CONCURRENTLY out-of-band on
-- live databases before this migration lands.
DROP INDEX IF EXISTS idx_trust_scores_global_score;
DROP INDEX IF EXISTS idx_events_kind1_content_trgm;
DROP INDEX IF EXISTS idx_search_documents_search_tsv;
DROP INDEX IF EXISTS idx_events_content_tsv;
DROP INDEX IF EXISTS idx_search_documents_freshness;
DROP INDEX IF EXISTS idx_search_documents_type_popularity;

-- event_relays / author_recent_events have tiny live sets relative to heap
-- bloat from retention deletes. Percentage thresholds still lag once the heap
-- is already swollen; switch to fixed absolute thresholds (same pattern as
-- 000057 / 000062) and raise the cost limit so vacuum can finish.
ALTER TABLE event_relays SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 5000,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_vacuum_cost_limit = 2000
);

ALTER TABLE author_recent_events SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 5000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 5000,
    autovacuum_vacuum_cost_delay = 2,
    autovacuum_vacuum_cost_limit = 2000
);
