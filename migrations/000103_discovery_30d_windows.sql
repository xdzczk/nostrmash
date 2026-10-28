-- 30-day discovery rankings. Score and velocity columns default to 0 so
-- unread rows stay out of a score > 0 list until a rewrite fills them.
-- Language and scored-input columns stay nullable, matching 24h/7d:
-- a language view skips authors the refresh has not classified yet.
--
-- Production may build the note indexes with CREATE INDEX CONCURRENTLY
-- before this migration ships. IF NOT EXISTS makes a second build a no-op.
-- Fresh databases build them here. The cluster statement_timeout is too
-- short for an index on the full note table, and this migration runs
-- inside one transaction so CONCURRENTLY is unavailable.
SET LOCAL statement_timeout = '15min';

ALTER TABLE note_discovery_stats
    ADD COLUMN IF NOT EXISTS score_30d DOUBLE PRECISION NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_30d_rank
    ON note_discovery_stats (score_30d DESC, created_at DESC, event_id ASC);

CREATE INDEX IF NOT EXISTS idx_note_discovery_stats_lang_score_30d
    ON note_discovery_stats (primary_language, score_30d DESC, created_at DESC, event_id)
    WHERE primary_language IS NOT NULL;

ALTER TABLE profile_discovery_stats
    ADD COLUMN IF NOT EXISTS score_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS rising_score_30d DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS scored_engagement_30d DOUBLE PRECISION
        CHECK (scored_engagement_30d IS NULL OR scored_engagement_30d >= 0),
    ADD COLUMN IF NOT EXISTS scored_new_followers_30d DOUBLE PRECISION
        CHECK (scored_new_followers_30d IS NULL OR scored_new_followers_30d >= 0),
    ADD COLUMN IF NOT EXISTS primary_language_30d TEXT;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_30d_rank
    ON profile_discovery_stats (score_30d DESC, recent_activity_at DESC, pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_rising_30d_rank
    ON profile_discovery_stats (rising_score_30d DESC, recent_activity_at DESC, pubkey ASC);

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_30d
    ON profile_discovery_stats (primary_language_30d, score_30d DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_30d IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_rising_30d
    ON profile_discovery_stats (primary_language_30d, rising_score_30d DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_30d IS NOT NULL;

ALTER TABLE thread_summaries
    ADD COLUMN IF NOT EXISTS replies_30d BIGINT NOT NULL DEFAULT 0 CHECK (replies_30d >= 0),
    ADD COLUMN IF NOT EXISTS authors_30d INTEGER NOT NULL DEFAULT 0 CHECK (authors_30d >= 0),
    ADD COLUMN IF NOT EXISTS reply_weight_30d DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (reply_weight_30d >= 0);

CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_30d
    ON thread_summaries (replies_30d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE replies_30d > 0;

CREATE INDEX IF NOT EXISTS idx_thread_summaries_hot_weight_30d
    ON thread_summaries (reply_weight_30d DESC, last_activity_at DESC, root_event_id ASC)
    WHERE reply_weight_30d > 0;
