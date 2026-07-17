-- trusted_*_discovery_candidates are rebuildable projections refreshed from
-- note_discovery_stats / profile_discovery_stats. The parent FKs cause
-- intermittent SQLSTATE 23503 failures when a concurrent sweeper deletes a
-- parent row during INSERT...SELECT (PostgreSQL re-checks FK against the
-- latest committed state). That abort rolls back the entire
-- trust_graph_snapshot refresh.
--
-- Soft cleanup already exists (DELETE WHERE NOT EXISTS parent) at the start of
-- each refresh, and discovery queries INNER JOIN the parent tables, so orphans
-- are never served. Drop the FKs; keep source_run_id FKs to trust_runs.
ALTER TABLE trusted_profile_discovery_candidates
    DROP CONSTRAINT IF EXISTS trusted_profile_discovery_candidates_pubkey_fkey;

ALTER TABLE trusted_note_discovery_candidates
    DROP CONSTRAINT IF EXISTS trusted_note_discovery_candidates_event_id_fkey;
