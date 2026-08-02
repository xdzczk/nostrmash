-- Backfill for the fine-grained daily tables introduced in
-- 000063_incremental_author_stats.sql.
--
-- Why this is needed: author_hashtag_daily / author_media_daily /
-- author_hourly_activity start empty. The windowed rollup queries in
-- internal/derivation/projection_author_windowed_topics_media.go and
-- projection_author_windowed_timing.go check "does this pubkey have any row
-- in the daily table after the window cutoff?" and, if so, sum ONLY the
-- daily table for the window (skip the old full-scan path entirely). Once
-- the incremental writer lands its first row for an active pubkey, that
-- check flips true and any pre-existing activity within the window that
-- isn't yet represented in the daily table is silently dropped from
-- topic/media/hourly stats until this backfill runs.
--
-- Scope: only the last 90 days, because 90 days is the largest window_days
-- value these rollups ever query (CHECK (window_days IN (7, 30, 90)) on the
-- windowed tables) — nothing older is ever read out of these daily tables,
-- so backfilling further back would be wasted work.
--
-- Idempotency / safety with the already-live incremental writer: this does
-- a fresh bulk aggregate straight from the source-of-truth tables
-- (event_hashtags, note_discovery_stats, events, event_references,
-- reaction_events, repost_events, zap_receipts) and OVERWRITES each
-- (pubkey, date[, ...]) row with the freshly computed total, rather than
-- adding to whatever is already there. That makes it safe to run even
-- though the incremental delta path may have already written partial rows
-- for "today" between deploy and backfill — the recompute for that day
-- already includes everything in the source tables, so overwriting with
-- the fresh total is correct and also self-heals any drift for the
-- backfilled window as a side effect. It is also safe to re-run.
--
-- Operational note: each statement below is a single bulk aggregate over a
-- bounded (90-day) slice, which is far cheaper than the old per-pubkey
-- rebuild sweepers it replaces, but it still does full scans of
-- event_references / reaction_events / repost_events / zap_receipts (no
-- pubkey filter to narrow the scan). Prefer running this during a
-- lower-traffic window if the database is already under load.

-- author_hashtag_daily: one row per (pubkey, activity_date, hashtag).
INSERT INTO author_hashtag_daily (pubkey, activity_date, hashtag, usage_count, derivation_version)
SELECT
    eh.author_pubkey,
    to_timestamp(eh.created_at)::date AS activity_date,
    eh.hashtag,
    COUNT(*)::bigint AS usage_count,
    1
FROM event_hashtags eh
WHERE eh.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY eh.author_pubkey, to_timestamp(eh.created_at)::date, eh.hashtag
ON CONFLICT (pubkey, activity_date, hashtag) DO UPDATE
SET usage_count = EXCLUDED.usage_count,
    updated_at = now();

-- author_media_daily: one row per (pubkey, activity_date), sourced from the
-- already-projected media-presence flags on note_discovery_stats (kind=1
-- notes only, matching the live incremental writer and the old media-mix
-- rebuild).
INSERT INTO author_media_daily (
    pubkey, activity_date, total_posts, with_image_count, with_video_count,
    with_link_count, with_article_count, text_only_count, total_attachment_count,
    derivation_version
)
SELECT
    nds.author_pubkey,
    to_timestamp(nds.created_at)::date AS activity_date,
    COUNT(*)::bigint,
    COUNT(*) FILTER (WHERE nds.has_image)::bigint,
    COUNT(*) FILTER (WHERE nds.has_video)::bigint,
    COUNT(*) FILTER (WHERE nds.has_link)::bigint,
    COUNT(*) FILTER (WHERE nds.has_article)::bigint,
    COUNT(*) FILTER (
        WHERE NOT nds.has_image AND NOT nds.has_video
          AND NOT nds.has_link AND NOT nds.has_article
    )::bigint,
    COALESCE(SUM(nds.attachment_count), 0)::bigint,
    1
FROM note_discovery_stats nds
WHERE nds.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
GROUP BY nds.author_pubkey, to_timestamp(nds.created_at)::date
ON CONFLICT (pubkey, activity_date) DO UPDATE
SET total_posts = EXCLUDED.total_posts,
    with_image_count = EXCLUDED.with_image_count,
    with_video_count = EXCLUDED.with_video_count,
    with_link_count = EXCLUDED.with_link_count,
    with_article_count = EXCLUDED.with_article_count,
    text_only_count = EXCLUDED.text_only_count,
    total_attachment_count = EXCLUDED.total_attachment_count,
    updated_at = now();

-- author_hourly_activity, authored side: post_count / note_count /
-- reply_count per (pubkey, activity_date, day_of_week, hour_of_day), mirrors
-- the "authored" CTE in upsertAuthorPostingPatternsTx's full-scan fallback.
INSERT INTO author_hourly_activity (
    pubkey, activity_date, day_of_week, hour_of_day, post_count, note_count, reply_count,
    derivation_version
)
SELECT
    a.pubkey,
    a.posted_at::date,
    EXTRACT(DOW FROM a.posted_at)::smallint,
    EXTRACT(HOUR FROM a.posted_at)::smallint,
    COUNT(*)::bigint,
    COUNT(*) FILTER (WHERE NOT a.is_reply)::bigint,
    COUNT(*) FILTER (WHERE a.is_reply)::bigint,
    1
FROM (
    SELECT
        e.pubkey,
        to_timestamp(e.created_at) AT TIME ZONE 'UTC' AS posted_at,
        EXISTS (
            SELECT 1 FROM event_references er
            WHERE er.source_event_id = e.id AND er.relation = 'reply'
        ) AS is_reply
    FROM events e
    WHERE e.kind = 1
      AND e.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
) a
GROUP BY a.pubkey, a.posted_at::date, EXTRACT(DOW FROM a.posted_at), EXTRACT(HOUR FROM a.posted_at)
ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
SET post_count = EXCLUDED.post_count,
    note_count = EXCLUDED.note_count,
    reply_count = EXCLUDED.reply_count,
    updated_at = now();

-- author_hourly_activity, received side: engagement_received / reply_received
-- / reaction_received / repost_received / zap_received per (pubkey,
-- activity_date, day_of_week, hour_of_day), mirrors the "received_events"
-- CTE in upsertAuthorActivityWindowsTx's full-scan fallback. This is a
-- separate statement from the authored-side one above so the two ON
-- CONFLICT DO UPDATE clauses only ever touch their own column subset and
-- can't stomp on each other's totals for a shared (pubkey, date, dow, hour)
-- row.
INSERT INTO author_hourly_activity (
    pubkey, activity_date, day_of_week, hour_of_day,
    engagement_received, reply_received, reaction_received, repost_received, zap_received,
    derivation_version
)
SELECT
    r.pubkey,
    r.engaged_at::date,
    EXTRACT(DOW FROM r.engaged_at)::smallint,
    EXTRACT(HOUR FROM r.engaged_at)::smallint,
    COUNT(*)::bigint,
    COUNT(*) FILTER (WHERE r.interaction_type = 'reply')::bigint,
    COUNT(*) FILTER (WHERE r.interaction_type = 'reaction')::bigint,
    COUNT(*) FILTER (WHERE r.interaction_type = 'repost')::bigint,
    COUNT(*) FILTER (WHERE r.interaction_type = 'zap')::bigint,
    1
FROM (
    SELECT
        target.pubkey,
        to_timestamp(e.created_at) AT TIME ZONE 'UTC' AS engaged_at,
        'reply'::text AS interaction_type
    FROM event_references er
    INNER JOIN events e ON e.id = er.source_event_id
    INNER JOIN events target ON target.id = er.referenced_event_id
    WHERE er.relation = 'reply'
      AND target.kind = 1
      AND e.pubkey <> target.pubkey
      AND e.created_at >= extract(epoch FROM now() - interval '90 days')::bigint

    UNION ALL

    SELECT
        target.pubkey,
        to_timestamp(re.created_at) AT TIME ZONE 'UTC',
        'reaction'
    FROM reaction_events re
    INNER JOIN events target ON target.id = re.target_event_id
    WHERE target.kind = 1
      AND re.reactor_pubkey <> target.pubkey
      AND re.created_at >= extract(epoch FROM now() - interval '90 days')::bigint

    UNION ALL

    SELECT
        target.pubkey,
        to_timestamp(re.created_at) AT TIME ZONE 'UTC',
        'repost'
    FROM repost_events re
    INNER JOIN events target ON target.id = re.target_event_id
    WHERE target.kind = 1
      AND re.reposter_pubkey <> target.pubkey
      AND re.created_at >= extract(epoch FROM now() - interval '90 days')::bigint

    UNION ALL

    SELECT
        target.pubkey,
        to_timestamp(zr.created_at) AT TIME ZONE 'UTC',
        'zap'
    FROM zap_receipts zr
    INNER JOIN events target ON target.id = zr.event_id
    WHERE target.kind = 1
      AND zr.sender_pubkey IS NOT NULL
      AND zr.sender_pubkey <> target.pubkey
      AND zr.created_at >= extract(epoch FROM now() - interval '90 days')::bigint
) r
GROUP BY r.pubkey, r.engaged_at::date, EXTRACT(DOW FROM r.engaged_at), EXTRACT(HOUR FROM r.engaged_at)
ON CONFLICT (pubkey, activity_date, day_of_week, hour_of_day) DO UPDATE
SET engagement_received = EXCLUDED.engagement_received,
    reply_received = EXCLUDED.reply_received,
    reaction_received = EXCLUDED.reaction_received,
    repost_received = EXCLUDED.repost_received,
    zap_received = EXCLUDED.zap_received,
    updated_at = now();
