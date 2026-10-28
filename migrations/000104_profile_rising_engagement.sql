-- Distinct engagers that entered the rising score. scored_engagement_*
-- stays the trending numerator (people on root notes published in the
-- window). Rising cites a different population — anyone who showed up
-- in the window, counted once — so it cannot share that column.
-- Nullable so a row written before the refresh falls back to the raw
-- counter instead of advertising zero.
ALTER TABLE profile_discovery_stats
    ADD COLUMN IF NOT EXISTS scored_rising_engagement_24h DOUBLE PRECISION
        CHECK (scored_rising_engagement_24h IS NULL OR scored_rising_engagement_24h >= 0),
    ADD COLUMN IF NOT EXISTS scored_rising_engagement_7d DOUBLE PRECISION
        CHECK (scored_rising_engagement_7d IS NULL OR scored_rising_engagement_7d >= 0),
    ADD COLUMN IF NOT EXISTS scored_rising_engagement_30d DOUBLE PRECISION
        CHECK (scored_rising_engagement_30d IS NULL OR scored_rising_engagement_30d >= 0);
