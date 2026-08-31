-- Persist the engagement / new-follower values that actually entered
-- profile trending and rising scores. Raw recent_* columns stay as display
-- counters (and keep feeding incremental-vs-full reconciliation). When
-- TRUST_DISCOVERY_ENGAGEMENT_WEIGHTING is on, these scored_* columns hold
-- the deduplicated trust-weighted votes; otherwise they equal the raw
-- window counters. Nullable so pre-rebuild rows fall back to raw evidence
-- rather than looking like they scored zero.
ALTER TABLE profile_discovery_stats
    ADD COLUMN IF NOT EXISTS scored_engagement_24h DOUBLE PRECISION
        CHECK (scored_engagement_24h IS NULL OR scored_engagement_24h >= 0),
    ADD COLUMN IF NOT EXISTS scored_engagement_7d DOUBLE PRECISION
        CHECK (scored_engagement_7d IS NULL OR scored_engagement_7d >= 0),
    ADD COLUMN IF NOT EXISTS scored_new_followers_24h DOUBLE PRECISION
        CHECK (scored_new_followers_24h IS NULL OR scored_new_followers_24h >= 0),
    ADD COLUMN IF NOT EXISTS scored_new_followers_7d DOUBLE PRECISION
        CHECK (scored_new_followers_7d IS NULL OR scored_new_followers_7d >= 0);
