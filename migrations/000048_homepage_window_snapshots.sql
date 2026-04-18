-- Extend the relay_window_snapshots table (introduced in 000047) to
-- materialize the rest of the homepage bundle's expensive aggregates.
--
-- Why this exists
-- ---------------
-- After 000047 moved the relay summary off the request path, the
-- /api/v1/discovery/home endpoint still took ~12s end-to-end on
-- production. EXPLAIN ANALYZE on the remaining queries showed three
-- additional COUNT(DISTINCT) aggregates with the same fundamental
-- shape as the relay summary:
--
--   * getPublicWindowStats (24h + 7d):
--       SELECT COUNT(*), COUNT(DISTINCT author_pubkey)
--       FROM note_discovery_stats WHERE created_at >= ?
--     ~600k-3M rows × 10k-40k distinct authors per window.
--
--   * getTopLanguages (24h + 7d):
--       SELECT COALESCE(primary_language,'und'), COUNT(*)
--       FROM note_discovery_stats WHERE created_at >= ?
--       GROUP BY 1 ORDER BY 2 DESC LIMIT 8
--     Same scan as above, plus a hash-aggregate by language.
--
--   * GetTrendingHashtags (24h + 7d, called from the homepage):
--       SELECT hashtag, COUNT(*), COUNT(DISTINCT author_pubkey)
--       FROM event_hashtags WHERE created_at >= ?
--       GROUP BY hashtag ORDER BY ... LIMIT 50
--     Scans the entire windowed slice of event_hashtags and hashes
--     into per-hashtag distinct-author sets.
--
-- Each of these is CPU-bound on COUNT(DISTINCT) at exactly the same
-- order of magnitude as the relay summary was — none can be fixed
-- with indexes or work_mem alone, and their cumulative cost made
-- the homepage handler spend ~10s of CPU per cache miss even after
-- 000047 removed the relay queries. Snapshotting them follows the
-- same pattern.
--
-- Table reuse
-- -----------
-- The existing table is named relay_window_snapshots for historical
-- reasons. It is in fact a generic homepage-window snapshot store
-- keyed by snapshot_label, so we keep using it rather than
-- introducing a parallel table with identical schema. The new
-- labels follow the existing naming convention.
--
-- Snapshot labels added by this migration
-- ---------------------------------------
--   home_window_24h    →  { note_volume, active_authors }
--   home_window_7d     →  { note_volume, active_authors }
--   top_languages_24h  →  [ { language, count }, ... ]   (top 8)
--   top_languages_7d   →  [ { language, count }, ... ]   (top 8)
--   top_hashtags_24h   →  [ { hashtag, event_count, unique_authors }, ... ]
--   top_hashtags_7d    →  [ { hashtag, event_count, unique_authors }, ... ]
--
-- The hashtag snapshots are sized at 50 rows (the API max). The
-- store layer slices them down to the request's actual limit so we
-- only need one snapshot per window regardless of caller-requested
-- limit.
--
-- Same SET LOCAL work_mem hack as 000047 — the default 4MB forces
-- the COUNT(DISTINCT) hashtables to spill ~100MB to disk for the
-- 7d window, which would push migration time from ~20s to over a
-- minute.

COMMENT ON TABLE relay_window_snapshots IS
    'Generic homepage-window snapshot store keyed by snapshot_label. '
    'Despite the historical name, hosts snapshots for relay summary, '
    'note volume / active authors, top languages, and top hashtags. '
    'Refreshed every few minutes by '
    'derivation.RefreshRelayWindowSnapshots — see '
    'internal/derivation/projection_relay_window_snapshots.go.';

SET LOCAL work_mem = '128MB';

-- home_window_24h and home_window_7d: note_volume + active_authors.
INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'home_window_24h',
    jsonb_build_object(
        'note_volume',    COALESCE(COUNT(*), 0)::bigint,
        'active_authors', COALESCE(COUNT(DISTINCT author_pubkey), 0)::bigint
    ),
    now()
FROM note_discovery_stats
WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'home_window_7d',
    jsonb_build_object(
        'note_volume',    COALESCE(COUNT(*), 0)::bigint,
        'active_authors', COALESCE(COUNT(DISTINCT author_pubkey), 0)::bigint
    ),
    now()
FROM note_discovery_stats
WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

-- top_languages_24h and top_languages_7d: top 8 languages by note count.
INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_languages_24h',
    COALESCE(jsonb_agg(
        jsonb_build_object('language', language, 'count', count_value)
        ORDER BY count_value DESC, language ASC
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        COALESCE(primary_language, 'und') AS language,
        COUNT(*)::bigint                  AS count_value
    FROM note_discovery_stats
    WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint
    GROUP BY COALESCE(primary_language, 'und')
    ORDER BY count_value DESC, language ASC
    LIMIT 8
) ranked
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_languages_7d',
    COALESCE(jsonb_agg(
        jsonb_build_object('language', language, 'count', count_value)
        ORDER BY count_value DESC, language ASC
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        COALESCE(primary_language, 'und') AS language,
        COUNT(*)::bigint                  AS count_value
    FROM note_discovery_stats
    WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint
    GROUP BY COALESCE(primary_language, 'und')
    ORDER BY count_value DESC, language ASC
    LIMIT 8
) ranked
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

-- top_hashtags_24h and top_hashtags_7d: top 50 hashtags ranked the
-- same way GetTrendingHashtags does (unique_authors DESC, diversity
-- DESC, event_count DESC, hashtag ASC). We snapshot 50 (the API max)
-- so the store layer can serve any request limit ≤50 from the same
-- row.
INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_hashtags_24h',
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'hashtag',        hashtag,
            'event_count',    event_count,
            'unique_authors', unique_authors
        )
        ORDER BY rank
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        hashtag,
        event_count,
        unique_authors,
        ROW_NUMBER() OVER (
            ORDER BY
                unique_authors DESC,
                unique_authors::double precision / GREATEST(event_count, 1) DESC,
                event_count DESC,
                hashtag ASC
        ) AS rank
    FROM (
        SELECT
            hashtag,
            COUNT(*)::bigint                       AS event_count,
            COUNT(DISTINCT author_pubkey)::bigint  AS unique_authors
        FROM event_hashtags
        WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint
        GROUP BY hashtag
    ) per_hashtag
) ranked
WHERE rank <= 50
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_hashtags_7d',
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'hashtag',        hashtag,
            'event_count',    event_count,
            'unique_authors', unique_authors
        )
        ORDER BY rank
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        hashtag,
        event_count,
        unique_authors,
        ROW_NUMBER() OVER (
            ORDER BY
                unique_authors DESC,
                unique_authors::double precision / GREATEST(event_count, 1) DESC,
                event_count DESC,
                hashtag ASC
        ) AS rank
    FROM (
        SELECT
            hashtag,
            COUNT(*)::bigint                       AS event_count,
            COUNT(DISTINCT author_pubkey)::bigint  AS unique_authors
        FROM event_hashtags
        WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint
        GROUP BY hashtag
    ) per_hashtag
) ranked
WHERE rank <= 50
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;
