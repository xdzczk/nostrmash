-- Incremental profile_discovery_stats support.
--
-- Steady-state discovery scoring used to re-scan events / reply_count_contributions
-- / reaction_events / repost_events / zap_receipts / follower_edges per dirty
-- pubkey (including an unbounded MAX(created_at) UNION). With this migration the
-- sweeper can roll 24h/7d windows from the already-incremental daily/hourly
-- tables plus two small additions:
--   * zap_msats_received on author_activity_daily / author_hourly_activity
--   * follower_gains_daily (true edge-diff gains from kind=3 contact lists)
--   * profile_discovery_recent_activity (O(1) GREATEST recent_activity_at)

ALTER TABLE author_activity_daily
    ADD COLUMN IF NOT EXISTS zap_msats_received BIGINT NOT NULL DEFAULT 0;

ALTER TABLE author_hourly_activity
    ADD COLUMN IF NOT EXISTS zap_msats_received BIGINT NOT NULL DEFAULT 0;

-- Counts every authored event in the hour (all kinds). Used for
-- profile_discovery_stats.recent_active_days so the incremental rollup matches
-- the legacy full scan (COUNT DISTINCT dates over events WHERE pubkey=$1).
ALTER TABLE author_hourly_activity
    ADD COLUMN IF NOT EXISTS authored_event_count BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS follower_gains_daily (
    pubkey TEXT NOT NULL,
    activity_date DATE NOT NULL,
    gained BIGINT NOT NULL DEFAULT 0,
    derivation_version INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (pubkey, activity_date)
);

CREATE INDEX IF NOT EXISTS idx_follower_gains_daily_pubkey_date
    ON follower_gains_daily (pubkey, activity_date DESC);

CREATE TABLE IF NOT EXISTS profile_discovery_recent_activity (
    pubkey TEXT PRIMARY KEY,
    recent_activity_at BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Backfill zap volumes for the last 90 days (largest analytics window). Overwrite
-- semantics match 000064: safe to re-run alongside the live incremental writer.
INSERT INTO author_activity_daily (
    pubkey, activity_date, zap_msats_received, derivation_version
)
SELECT
    zr.receiver_pubkey,
    (to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::date AS activity_date,
    COALESCE(SUM(zr.amount_sats * 1000), 0)::bigint,
    1
FROM zap_receipts zr
WHERE zr.receiver_pubkey IS NOT NULL
  AND zr.receiver_pubkey <> ''
  AND zr.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY zr.receiver_pubkey, (to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::date
ON CONFLICT (pubkey, activity_date) DO UPDATE
SET zap_msats_received = EXCLUDED.zap_msats_received,
    updated_at = now();

INSERT INTO author_hourly_activity (
    pubkey, activity_date, day_of_week, hour_of_day,
    zap_msats_received, derivation_version
)
SELECT
    zr.receiver_pubkey,
    (to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::date AS activity_date,
    EXTRACT(DOW FROM to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::smallint,
    EXTRACT(HOUR FROM to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::smallint,
    COALESCE(SUM(zr.amount_sats * 1000), 0)::bigint,
    1
FROM zap_receipts zr
WHERE zr.receiver_pubkey IS NOT NULL
  AND zr.receiver_pubkey <> ''
  AND zr.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY
    zr.receiver_pubkey,
    (to_timestamp(zr.created_at) AT TIME ZONE 'UTC')::date,
    EXTRACT(DOW FROM to_timestamp(zr.created_at) AT TIME ZONE 'UTC'),
    EXTRACT(HOUR FROM to_timestamp(zr.created_at) AT TIME ZONE 'UTC')
ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
SET zap_msats_received = EXCLUDED.zap_msats_received,
    updated_at = now();

-- Seed authored_event_count for discovery active_days parity.
INSERT INTO author_hourly_activity (
    pubkey, activity_date, day_of_week, hour_of_day,
    authored_event_count, derivation_version
)
SELECT
    e.pubkey,
    (to_timestamp(e.created_at) AT TIME ZONE 'UTC')::date AS activity_date,
    EXTRACT(DOW FROM to_timestamp(e.created_at) AT TIME ZONE 'UTC')::smallint,
    EXTRACT(HOUR FROM to_timestamp(e.created_at) AT TIME ZONE 'UTC')::smallint,
    COUNT(*)::bigint,
    1
FROM events e
WHERE e.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY
    e.pubkey,
    (to_timestamp(e.created_at) AT TIME ZONE 'UTC')::date,
    EXTRACT(DOW FROM to_timestamp(e.created_at) AT TIME ZONE 'UTC'),
    EXTRACT(HOUR FROM to_timestamp(e.created_at) AT TIME ZONE 'UTC')
ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
SET authored_event_count = EXCLUDED.authored_event_count,
    updated_at = now();

-- Seed follower_gains_daily from current follower_edges timestamps. This matches
-- the previous full-scan "new_followers" definition for historical rows
-- (edges whose contact_list_created_at falls in the window). Live writes after
-- cutover use true kind=3 edge-diff gains, which is the intended rising-score
-- semantic going forward.
INSERT INTO follower_gains_daily (pubkey, activity_date, gained, derivation_version)
SELECT
    fe.followed_pubkey,
    (to_timestamp(fe.contact_list_created_at) AT TIME ZONE 'UTC')::date AS activity_date,
    COUNT(*)::bigint,
    1
FROM follower_edges fe
WHERE fe.contact_list_created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY fe.followed_pubkey, (to_timestamp(fe.contact_list_created_at) AT TIME ZONE 'UTC')::date
ON CONFLICT (pubkey, activity_date) DO UPDATE
SET gained = EXCLUDED.gained,
    updated_at = now();

-- Seed discovery recent-activity from the currently scored discovery rows so
-- the first incremental re-score after deploy does not lose recency.
INSERT INTO profile_discovery_recent_activity (pubkey, recent_activity_at)
SELECT pubkey, recent_activity_at
FROM profile_discovery_stats
WHERE recent_activity_at IS NOT NULL
ON CONFLICT (pubkey) DO UPDATE
SET recent_activity_at = GREATEST(
        profile_discovery_recent_activity.recent_activity_at,
        EXCLUDED.recent_activity_at
    ),
    updated_at = now();
