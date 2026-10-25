-- Dominant content language of an author's notes in each ranking window.
-- Null until the profile row is refreshed, so a language view skips authors
-- the backfill and the hot path have not classified yet.
ALTER TABLE profile_discovery_stats
    ADD COLUMN IF NOT EXISTS primary_language_24h TEXT,
    ADD COLUMN IF NOT EXISTS primary_language_7d TEXT;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_24h
    ON profile_discovery_stats (primary_language_24h, score_24h DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_24h IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_rising_24h
    ON profile_discovery_stats (primary_language_24h, rising_score_24h DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_24h IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_7d
    ON profile_discovery_stats (primary_language_7d, score_7d DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_7d IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_profile_discovery_stats_lang_rising_7d
    ON profile_discovery_stats (primary_language_7d, rising_score_7d DESC, recent_activity_at DESC, pubkey ASC)
    WHERE primary_language_7d IS NOT NULL;
