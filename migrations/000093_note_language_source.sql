-- Why a note received its language, or why it stayed unmarked.
-- content/tag are detected from the note itself and are stable.
-- thread/author are inferred and stored at a lower confidence.
-- short/uncertain keep primary_language NULL so search for "und" can split them.
ALTER TABLE note_discovery_stats
    ADD COLUMN IF NOT EXISTS language_source TEXT;

ALTER TABLE note_discovery_stats
    DROP CONSTRAINT IF EXISTS note_discovery_stats_language_source_check;

ALTER TABLE note_discovery_stats
    ADD CONSTRAINT note_discovery_stats_language_source_check
    CHECK (language_source IS NULL OR language_source IN (
        'content', 'tag', 'thread', 'author', 'short', 'uncertain'
    ));

-- Author-language fallback only counts notes the detector or a tag labeled.
-- Short notes are the bulk of a pubkey, so they stay out of this index.
CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_author_content_language
    ON note_discovery_stats (author_pubkey, created_at DESC)
    WHERE language_source IN ('content', 'tag') AND primary_language IS NOT NULL;
