-- Materialized "homepage relay stats" snapshot table.
--
-- Why this exists
-- ---------------
-- The relay summary stats and "top relays by activity" lists on the
-- public homepage all rely on COUNT(DISTINCT pubkey) over the
-- event_relays table:
--
--   * 24h window:  ~600k rows  →  ~28k distinct pubkeys
--   * 7d  window:  ~3.7M rows  →  ~120k distinct pubkeys
--   * per-relay GROUP BY (top relays): ~3.7M rows hashed into
--     ~20 relay buckets, each accumulating its own distinct-pubkey set
--
-- COUNT(DISTINCT) cannot use an index for the distinct count; the
-- planner must read every matching row and hash it. On a 4-core
-- production box the 7d aggregate takes ~9s of pure CPU even with
-- the covering idx_event_relays_seen_at_pubkey index, work_mem
-- bumped to 128MB, and ANALYZE up to date. There is no SQL trick
-- that can make this faster: the cost is fundamental to the
-- cardinality of the input and the cardinality of the output.
--
-- Recomputing this on every homepage request is therefore
-- infeasible. The previous design (recompute on cache miss every
-- 60s with a 30s API timeout) caused two cascading failures:
--   1. Each cache-miss request blocked for 11s+, exceeding the
--      Next.js 30s fetch timeout under any concurrency.
--   2. Concurrent cache misses serialized on the same connection
--      pool, exhausting it and starving every other endpoint with
--      "context canceled" 500s.
--
-- The fix: precompute these stats out-of-band on a 5-minute cadence,
-- store the latest snapshot here, and have the homepage handler do
-- a single sub-millisecond row lookup. The relay summary changes
-- slowly (~minutes for the 24h window, ~hours for the 7d window),
-- so a 5-minute snapshot lag is invisible to users.
--
-- Schema design
-- -------------
-- One row per snapshot kind, identified by a stable text label.
-- The payload is JSONB so we can evolve the per-kind shape without
-- another migration: e.g. add new fields to the summary, or change
-- the top-relays list length. The reader marshals/unmarshals into
-- typed Go structs.
--
-- Initial seed
-- ------------
-- The migration computes the initial values inline so the very first
-- /home request after deploy returns real numbers instead of zeros.
-- The seed runs in the same transaction as the schema DDL; if it
-- fails the whole migration rolls back and 000047 stays unapplied,
-- which is the correct behavior (better to retry the migration than
-- to deploy an empty snapshot table).
--
-- The seed itself uses SET LOCAL work_mem = '128MB' for the same
-- reason the projection does — the default 4MB forces a 200MB
-- external merge sort to disk.

CREATE TABLE IF NOT EXISTS relay_window_snapshots (
    snapshot_label TEXT        PRIMARY KEY,
    payload        JSONB       NOT NULL,
    computed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bump work_mem for the inline seed so COUNT(DISTINCT pubkey) does
-- not spill 200MB to disk during migration.
SET LOCAL work_mem = '128MB';

-- Seed the relay summary row.
WITH
total AS (
    -- Loose index scan: ~20 distinct relay_urls out of 4.4M rows.
    -- A plain COUNT(DISTINCT relay_url) takes ~1.6s; this takes ~5ms.
    WITH RECURSIVE distinct_relays AS (
        (SELECT relay_url FROM event_relays ORDER BY relay_url ASC LIMIT 1)
        UNION ALL
        SELECT (
            SELECT relay_url
            FROM event_relays
            WHERE relay_url > prev.relay_url
            ORDER BY relay_url ASC
            LIMIT 1
        )
        FROM distinct_relays prev
        WHERE prev.relay_url IS NOT NULL
    )
    SELECT COALESCE(COUNT(*), 0)::bigint AS value
    FROM distinct_relays
    WHERE relay_url IS NOT NULL
),
window_24h AS (
    SELECT
        COALESCE(COUNT(DISTINCT relay_url), 0)::bigint AS active,
        COALESCE(COUNT(*), 0)::bigint                  AS events,
        COALESCE(COUNT(DISTINCT pubkey), 0)::bigint    AS authors
    FROM event_relays
    WHERE seen_at >= now() - INTERVAL '24 hours'
),
window_7d AS (
    SELECT
        COALESCE(COUNT(DISTINCT relay_url), 0)::bigint AS active,
        COALESCE(COUNT(*), 0)::bigint                  AS events,
        COALESCE(COUNT(DISTINCT pubkey), 0)::bigint    AS authors
    FROM event_relays
    WHERE seen_at >= now() - INTERVAL '7 days'
)
INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'summary',
    jsonb_build_object(
        'total',          total.value,
        'active_24h',     window_24h.active,
        'active_7d',      window_7d.active,
        'events_24h',     window_24h.events,
        'events_7d',      window_7d.events,
        'authors_24h',    window_24h.authors,
        'authors_7d',     window_7d.authors
    ),
    now()
FROM total, window_24h, window_7d
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

-- Seed the top-relays-by-activity row (7-day window, top 10).
INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_relays_7d',
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'relay_url',      relay_url,
            'event_count',    event_count,
            'unique_authors', unique_authors
        )
        ORDER BY event_count DESC, unique_authors DESC, relay_url ASC
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        er.relay_url,
        COUNT(*)::bigint                  AS event_count,
        COUNT(DISTINCT er.pubkey)::bigint AS unique_authors
    FROM event_relays er
    WHERE er.seen_at >= now() - INTERVAL '7 days'
    GROUP BY er.relay_url
    ORDER BY event_count DESC, unique_authors DESC, er.relay_url ASC
    LIMIT 10
) ranked
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;
