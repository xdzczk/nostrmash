-- Language rankings walk note_discovery_stats by primary_language and the
-- window score. The unscoped score indexes stay the path for "all languages".
--
-- Production may build these with CREATE INDEX CONCURRENTLY before the
-- migration ships. IF NOT EXISTS makes a second build a no-op. Fresh
-- databases build them here; the cluster statement_timeout is too short
-- for an index on the full note table.
SET LOCAL statement_timeout = '15min';

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_lang_score_24h
    ON note_discovery_stats (primary_language, score_24h DESC, created_at DESC, event_id)
    WHERE primary_language IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_lang_score_7d
    ON note_discovery_stats (primary_language, score_7d DESC, created_at DESC, event_id)
    WHERE primary_language IS NOT NULL;

-- One cursor for the resumable 7-day language backfill. The worker drains
-- it; the admin route inserts or resets the row.
CREATE TABLE IF NOT EXISTS derivation_backfill_cursors (
    name TEXT PRIMARY KEY,
    phase TEXT NOT NULL,
    status TEXT NOT NULL,
    window_start BIGINT NOT NULL,
    cursor_created_at BIGINT NOT NULL DEFAULT 0,
    cursor_event_id TEXT NOT NULL DEFAULT '',
    cursor_pubkey TEXT NOT NULL DEFAULT '',
    scanned BIGINT NOT NULL DEFAULT 0,
    updated_rows BIGINT NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ
);
