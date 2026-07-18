-- Storage discipline follow-up: drop two large indexes with zero production
-- scans over 6+ weeks of uptime (pg_stat_user_indexes.idx_scan = 0).
--
-- idx_event_tags_e_lookup (9.3 GB): partial index on event_tags for e-tag
-- lookups. Its only code owner (highlights-by-event-id in
-- compat_queries_followers_highlights.go) is served by
-- idx_event_tags_tag_name_value, which covers (tag_name, value) for
-- tag_name IN ('e','p') — verified via EXPLAIN against production.
--
-- idx_trusted_note_discovery_hops_score (13 GB for ~900k rows — heavily
-- bloated): ordering index on trusted_note_discovery_candidates. All reads
-- join or delete by event_id (the primary key); no query orders by
-- (min_hops, trust_score).
--
-- Plain DROP INDEX (not CONCURRENTLY) because migrations run inside a
-- transaction; the brief exclusive lock is acceptable at startup.
DROP INDEX IF EXISTS idx_event_tags_e_lookup;
DROP INDEX IF EXISTS idx_trusted_note_discovery_hops_score;
