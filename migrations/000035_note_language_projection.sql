ALTER TABLE note_discovery_stats
    ADD COLUMN IF NOT EXISTS primary_language TEXT,
    ADD COLUMN IF NOT EXISTS language_confidence DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_primary_language_created_at
    ON note_discovery_stats (primary_language, created_at DESC, event_id DESC);

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_author_language
    ON note_discovery_stats (author_pubkey, primary_language, created_at DESC);
