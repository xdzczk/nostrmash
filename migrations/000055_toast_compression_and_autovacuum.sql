-- Storage discipline: TOAST compression + autovacuum tuning (replaces the
-- rejected partitioning approach; see docs/design/storage-discipline.md).
--
-- 1) LZ4 column compression for the big JSONB payloads. Applies to NEW writes
--    only (existing TOAST data keeps its original compression until the row
--    is rewritten, e.g. by pg_repack or VACUUM FULL). LZ4 compresses Nostr
--    JSON slightly worse than pglz but decompresses several times faster,
--    which matters because raw_json is read on every event-shaped API
--    response. Guarded: only applied when the server was built with LZ4
--    support, so operators on a minimal external Postgres are not broken.
--
-- 2) Per-table autovacuum tuning for the churn tables the retention loops
--    delete from continuously. The default scale factor (0.2 = 20% of the
--    table must be dead before autovacuum runs) lets millions of dead tuples
--    accumulate on large tables; 2-5% keeps dead-tuple pressure bounded so
--    disk space is reused instead of growing.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_settings
        WHERE name = 'default_toast_compression'
          AND 'lz4' = ANY (enumvals)
    ) THEN
        EXECUTE 'ALTER TABLE events ALTER COLUMN raw_json SET COMPRESSION lz4';
        EXECUTE 'ALTER TABLE invalid_events ALTER COLUMN raw_payload SET COMPRESSION lz4';
    ELSE
        RAISE NOTICE 'lz4 compression not available in this PostgreSQL build; keeping pglz';
    END IF;
END
$$;

-- Highest churn: every job row is inserted and later deleted.
ALTER TABLE jobs SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);

-- Large canonical tables with steady retention deletes.
ALTER TABLE events SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE event_tags SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE event_relays SET (autovacuum_vacuum_scale_factor = 0.05);

-- Projections with the new retention/groom loops.
ALTER TABLE search_documents SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE author_recent_events SET (autovacuum_vacuum_scale_factor = 0.05);
ALTER TABLE account_states SET (autovacuum_vacuum_scale_factor = 0.05);
