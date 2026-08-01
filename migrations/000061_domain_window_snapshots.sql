-- Extend the relay_window_snapshots table (introduced in 000047, extended
-- in 000048 with note-volume/language/hashtag aggregates) with the
-- homepage's trending-domains bundle.
--
-- Why this exists
-- ---------------
-- /api/v1/discovery/home builds its "trending_domains" section from
-- GetTrendingDomains, which GROUPs event_urls BY canonical_domain and
-- computes COUNT(DISTINCT event_id) / COUNT(DISTINCT author_pubkey) per
-- domain — the same COUNT(DISTINCT) shape and cost class as the hashtag
-- aggregate snapshotted in 000048 (~1M+ row scan, hashed into per-domain
-- distinct-author sets). After 000048 snapshotted the relay, note-volume,
-- language, and hashtag aggregates, trending domains was the one
-- remaining live COUNT(DISTINCT) aggregate left on the homepage request
-- path, still capable of turning a cache-miss request into a multi-second
-- (or, under load, timed-out) response.
--
-- There is no SQL trick or index that fixes COUNT(DISTINCT) at these
-- cardinalities — see the rationale in 000047 and 000048. The fix is the
-- same: compute this out-of-band on a fixed cadence
-- (derivation.RefreshRelayWindowSnapshots, every 5 minutes) and serve the
-- homepage from a sub-millisecond row lookup.
--
-- Snapshot labels added by this migration
-- ----------------------------------------
--   top_domains_24h -> [ { domain, latest_event_at,
--                           activity_24h: {link_count, note_count, unique_authors},
--                           activity_7d:  {link_count, note_count, unique_authors} },
--                         ... ]  (top 50, candidate set restricted to the
--                         last 24h and ranked by unique-author breadth
--                         within that set)
--   top_domains_7d  -> same shape, candidate set restricted to the last
--                         7 days and ranked by unique-author breadth
--                         within that set
--
-- These mirror exactly what store.GetTrendingDomains computes for a
-- window=24h / window=7d request: the outer WHERE clause scopes the
-- candidate rows to the requested window, and the two FILTER'd
-- sub-aggregates populate both activity windows in the response
-- regardless of which one is being ranked. The store layer
-- (GetHomeTrendingDomains) slices this list down to the per-request
-- limit, so we always materialize the API max (50) here.
--
-- Same SET LOCAL work_mem hack as 000047/000048 — the default 4MB would
-- force the COUNT(DISTINCT) hashtables built per canonical_domain to
-- spill to disk for the 7-day window.

SET LOCAL work_mem = '128MB';

INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_domains_24h',
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'domain', canonical_domain,
            'latest_event_at', latest_event_at,
            'activity_24h', jsonb_build_object(
                'link_count', link_count_24h,
                'note_count', note_count_24h,
                'unique_authors', unique_authors_24h
            ),
            'activity_7d', jsonb_build_object(
                'link_count', link_count_7d,
                'note_count', note_count_7d,
                'unique_authors', unique_authors_7d
            )
        )
        ORDER BY rank
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        canonical_domain,
        latest_event_at,
        link_count_24h, note_count_24h, unique_authors_24h,
        link_count_7d, note_count_7d, unique_authors_7d,
        ROW_NUMBER() OVER (
            ORDER BY
                unique_authors_24h DESC,
                unique_authors_24h::double precision / GREATEST(note_count_24h, 1) DESC,
                note_count_24h DESC,
                link_count_24h DESC,
                canonical_domain ASC
        ) AS rank
    FROM (
        SELECT
            canonical_domain,
            MAX(created_at) AS latest_event_at,
            COUNT(*) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS link_count_24h,
            COUNT(DISTINCT event_id) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS note_count_24h,
            COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS unique_authors_24h,
            COUNT(*) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS link_count_7d,
            COUNT(DISTINCT event_id) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS note_count_7d,
            COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS unique_authors_7d
        FROM event_urls
        WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint
          AND NOT (url ~* '\.(png|jpe?g|gif|webp|svg|bmp|ico|tiff?|avif|heic|mp4|mov|webm|m4v|avi|mkv|wmv|flv|mp3|wav|ogg|m4a|flac|aac|opus)(\?|#|$)')
        GROUP BY canonical_domain
    ) agg
) ranked
WHERE rank <= 50
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;

INSERT INTO relay_window_snapshots (snapshot_label, payload, computed_at)
SELECT
    'top_domains_7d',
    COALESCE(jsonb_agg(
        jsonb_build_object(
            'domain', canonical_domain,
            'latest_event_at', latest_event_at,
            'activity_24h', jsonb_build_object(
                'link_count', link_count_24h,
                'note_count', note_count_24h,
                'unique_authors', unique_authors_24h
            ),
            'activity_7d', jsonb_build_object(
                'link_count', link_count_7d,
                'note_count', note_count_7d,
                'unique_authors', unique_authors_7d
            )
        )
        ORDER BY rank
    ), '[]'::jsonb),
    now()
FROM (
    SELECT
        canonical_domain,
        latest_event_at,
        link_count_24h, note_count_24h, unique_authors_24h,
        link_count_7d, note_count_7d, unique_authors_7d,
        ROW_NUMBER() OVER (
            ORDER BY
                unique_authors_7d DESC,
                unique_authors_7d::double precision / GREATEST(note_count_7d, 1) DESC,
                note_count_7d DESC,
                link_count_7d DESC,
                canonical_domain ASC
        ) AS rank
    FROM (
        SELECT
            canonical_domain,
            MAX(created_at) AS latest_event_at,
            COUNT(*) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS link_count_24h,
            COUNT(DISTINCT event_id) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS note_count_24h,
            COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '24 hours')::bigint) AS unique_authors_24h,
            COUNT(*) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS link_count_7d,
            COUNT(DISTINCT event_id) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS note_count_7d,
            COUNT(DISTINCT author_pubkey) FILTER (WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint) AS unique_authors_7d
        FROM event_urls
        WHERE created_at >= EXTRACT(EPOCH FROM now() - INTERVAL '7 days')::bigint
          AND NOT (url ~* '\.(png|jpe?g|gif|webp|svg|bmp|ico|tiff?|avif|heic|mp4|mov|webm|m4v|avi|mkv|wmv|flv|mp3|wav|ogg|m4a|flac|aac|opus)(\?|#|$)')
        GROUP BY canonical_domain
    ) agg
) ranked
WHERE rank <= 50
ON CONFLICT (snapshot_label) DO UPDATE
SET payload     = EXCLUDED.payload,
    computed_at = EXCLUDED.computed_at;
