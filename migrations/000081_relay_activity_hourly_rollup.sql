-- Hourly relay-activity rollup backing the homepage relay snapshots.
--
-- computeTopRelaysSnapshot and computeRelaySummarySnapshot used to GROUP BY /
-- COUNT(DISTINCT) directly over the raw event_relays rows for the 24h and 7d
-- windows on every 5-minute refresh. That cost scales with raw ingest volume:
-- when the relay-admission cap bug let 337 relays go active, ingest jumped
-- ~8x (4.8M event_relays rows/day) and the 7d aggregation blew through the
-- 100s statement budget every cycle — the homepage snapshot went stale for
-- days (NostrMashRelayWindowSnapshotStale).
--
-- The rollup makes the refresh cost proportional to *new* rows since the
-- last refresh instead of the whole window: each refresh aggregates the
-- (rolled_up_until, now-lag] slice of event_relays into hourly per-relay
-- buckets, and the snapshot queries then rank/sum over at most
-- relays x hours rows (tens of thousands) instead of tens of millions.
-- COUNT(DISTINCT pubkey) cannot be pre-aggregated across buckets; the
-- snapshot computes it only for the final top-N relays via bounded
-- index-only probes of idx_event_relays_relay_url_seen_at.
CREATE TABLE IF NOT EXISTS relay_activity_hourly (
    bucket_start timestamptz NOT NULL,
    relay_url    text        NOT NULL,
    event_count  bigint      NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket_start, relay_url)
);

-- Single-row watermark: everything at or before rolled_up_until is already
-- reflected in relay_activity_hourly. Selected FOR UPDATE by the refresh so
-- concurrent worker replicas serialize instead of double-counting a slice.
CREATE TABLE IF NOT EXISTS relay_activity_rollup_state (
    id              boolean     PRIMARY KEY DEFAULT TRUE CHECK (id),
    rolled_up_until timestamptz NOT NULL
);

-- Start the watermark a full 7d window back so the first refresh cycles
-- backfill the whole ranking window from raw rows (in bounded chunks) and
-- the top-relays snapshot converges to exact numbers within the first hour
-- of deployment instead of starting from an empty rollup.
INSERT INTO relay_activity_rollup_state (id, rolled_up_until)
VALUES (TRUE, now() - interval '7 days')
ON CONFLICT (id) DO NOTHING;

-- The current-hour bucket rows are re-upserted on every 5-minute refresh
-- (dead tuple per relay per refresh). The table is tiny, so vacuum by fixed
-- threshold rather than the default 20% scale factor.
ALTER TABLE relay_activity_hourly SET (
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 2000,
    autovacuum_analyze_scale_factor = 0,
    autovacuum_analyze_threshold = 2000
);
