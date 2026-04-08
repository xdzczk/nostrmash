package derivation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var authorAnalyticsWindows = []int{7, 30, 90}

func (h *Handlers) ProjectAuthorAnalytics(ctx context.Context, eventID string) error {
	return h.projectAuthorAnalyticsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectAuthorAnalyticsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	if err := h.pool.QueryRow(ctx, `
		SELECT pubkey
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey); err != nil {
		return fmt.Errorf("load event for author analytics projection: %w", err)
	}
	return h.projectAuthorAnalyticsForPubkey(ctx, pubkey, versionOverride)
}

func (h *Handlers) rebuildAuthorAnalyticsWithVersion(ctx context.Context, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	rows, err := h.pool.Query(ctx, `
		SELECT pubkey
		FROM (
			SELECT DISTINCT pubkey AS pubkey
			FROM events
			UNION
			SELECT DISTINCT receiver_pubkey AS pubkey
			FROM zap_receipts
			WHERE receiver_pubkey <> ''
			UNION
			SELECT DISTINCT sender_pubkey AS pubkey
			FROM zap_receipts
			WHERE sender_pubkey IS NOT NULL AND sender_pubkey <> ''
			UNION
			SELECT DISTINCT reactor_pubkey AS pubkey
			FROM reaction_events
			WHERE reactor_pubkey <> ''
			UNION
			SELECT DISTINCT reposter_pubkey AS pubkey
			FROM repost_events
			WHERE reposter_pubkey <> ''
		) authors
		ORDER BY pubkey ASC
	`)
	if err != nil {
		return fmt.Errorf("list authors for analytics rebuild: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return fmt.Errorf("scan author pubkey for analytics rebuild: %w", err)
		}
		if err := h.projectAuthorAnalyticsForPubkey(ctx, pubkey, versionOverride); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate authors for analytics rebuild: %w", err)
	}
	return nil
}

func (h *Handlers) projectAuthorAnalyticsForPubkey(ctx context.Context, pubkey string, versionOverride *int) error {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return fmt.Errorf("pubkey is required")
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.projectAuthorAnalyticsForPubkeyTx(ctx, tx, pubkey, versionOverride); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit author analytics projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) projectAuthorAnalyticsForPubkeyTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	versionOverride *int,
) error {
	activityVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityDaily,
		AuthorActivityDailyVersion,
		"Project per-author daily post cadence and engagement aggregates",
		versionOverride,
	)
	if err != nil {
		return err
	}
	engagementVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorEngagementStats,
		AuthorEngagementStatsVersion,
		"Project windowed per-author engagement and cadence summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	topicVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorTopicStats,
		AuthorTopicStatsVersion,
		"Project windowed per-author hashtag usage summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	mediaVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorMediaMixStats,
		AuthorMediaMixStatsVersion,
		"Project windowed per-author media mix summaries",
		versionOverride,
	)
	if err != nil {
		return err
	}
	activityWindowVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorActivityWindows,
		AuthorActivityWindowsVersion,
		"Project windowed per-author engagement timing buckets by UTC day/hour",
		versionOverride,
	)
	if err != nil {
		return err
	}
	postingPatternVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorPostingPatterns,
		AuthorPostingPatternsVersion,
		"Project windowed per-author posting cadence buckets by UTC day/hour",
		versionOverride,
	)
	if err != nil {
		return err
	}

	if err := h.rebuildAuthorActivityDailyTx(ctx, tx, pubkey, activityVersion); err != nil {
		return err
	}
	if err := h.rebuildAuthorWindowedStatsTx(
		ctx,
		tx,
		pubkey,
		engagementVersion,
		topicVersion,
		mediaVersion,
		activityWindowVersion,
		postingPatternVersion,
	); err != nil {
		return err
	}
	return nil
}

func (h *Handlers) rebuildAuthorActivityDailyTx(ctx context.Context, tx pgx.Tx, pubkey string, version int) error {
	if _, err := tx.Exec(ctx, `DELETE FROM author_activity_daily WHERE pubkey = $1`, pubkey); err != nil {
		return fmt.Errorf("delete prior author activity daily rows for %s: %w", pubkey, err)
	}

	_, err := tx.Exec(ctx, `
		WITH post_daily AS (
			SELECT
				to_timestamp(e.created_at)::date AS activity_date,
				COUNT(*) AS post_count,
				COUNT(*) FILTER (
					WHERE NOT EXISTS (
						SELECT 1
						FROM event_references er
						WHERE er.source_event_id = e.id
						  AND er.relation = 'reply'
					)
				) AS note_count,
				COUNT(*) FILTER (
					WHERE EXISTS (
						SELECT 1
						FROM event_references er
						WHERE er.source_event_id = e.id
						  AND er.relation = 'reply'
					)
				) AS reply_count
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			GROUP BY to_timestamp(e.created_at)::date
		),
		received_sources AS (
			SELECT to_timestamp(e.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM event_references er
			INNER JOIN events e ON e.id = er.source_event_id
			INNER JOIN events target ON target.id = er.target_event_id
			WHERE er.relation = 'reply'
			  AND target.pubkey = $1
			  AND e.pubkey <> $1
			GROUP BY to_timestamp(e.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reaction_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND re.reactor_pubkey <> $1
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM repost_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND re.reposter_pubkey <> $1
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(zr.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM zap_receipts zr
			WHERE zr.receiver_pubkey = $1
			  AND zr.sender_pubkey IS NOT NULL
			  AND zr.sender_pubkey <> $1
			GROUP BY to_timestamp(zr.created_at)::date
		),
		received_daily AS (
			SELECT activity_date, SUM(count_value)::bigint AS engagement_received
			FROM received_sources
			GROUP BY activity_date
		),
		given_sources AS (
			SELECT to_timestamp(e.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM event_references er
			INNER JOIN events e ON e.id = er.source_event_id
			INNER JOIN events target ON target.id = er.target_event_id
			WHERE er.relation = 'reply'
			  AND e.pubkey = $1
			  AND target.pubkey <> $1
			GROUP BY to_timestamp(e.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM reaction_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE re.reactor_pubkey = $1
			  AND target.pubkey <> $1
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(re.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM repost_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE re.reposter_pubkey = $1
			  AND target.pubkey <> $1
			GROUP BY to_timestamp(re.created_at)::date
			UNION ALL
			SELECT to_timestamp(zr.created_at)::date AS activity_date, COUNT(*) AS count_value
			FROM zap_receipts zr
			WHERE zr.sender_pubkey = $1
			  AND zr.receiver_pubkey <> $1
			GROUP BY to_timestamp(zr.created_at)::date
		),
		given_daily AS (
			SELECT activity_date, SUM(count_value)::bigint AS engagement_given
			FROM given_sources
			GROUP BY activity_date
		),
		all_days AS (
			SELECT activity_date FROM post_daily
			UNION
			SELECT activity_date FROM received_daily
			UNION
			SELECT activity_date FROM given_daily
		)
		INSERT INTO author_activity_daily (
			pubkey,
			activity_date,
			post_count,
			note_count,
			reply_count,
			engagement_received,
			engagement_given,
			derivation_version
		)
		SELECT
			$1,
			d.activity_date,
			COALESCE(p.post_count, 0),
			COALESCE(p.note_count, 0),
			COALESCE(p.reply_count, 0),
			COALESCE(r.engagement_received, 0),
			COALESCE(g.engagement_given, 0),
			$2
		FROM all_days d
		LEFT JOIN post_daily p ON p.activity_date = d.activity_date
		LEFT JOIN received_daily r ON r.activity_date = d.activity_date
		LEFT JOIN given_daily g ON g.activity_date = d.activity_date
		ON CONFLICT (pubkey, activity_date) DO UPDATE
		SET post_count = EXCLUDED.post_count,
		    note_count = EXCLUDED.note_count,
		    reply_count = EXCLUDED.reply_count,
		    engagement_received = EXCLUDED.engagement_received,
		    engagement_given = EXCLUDED.engagement_given,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, version)
	if err != nil {
		return fmt.Errorf("rebuild author activity daily for %s: %w", pubkey, err)
	}
	return nil
}

func (h *Handlers) rebuildAuthorWindowedStatsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	engagementVersion int,
	topicVersion int,
	mediaVersion int,
	activityWindowVersion int,
	postingPatternVersion int,
) error {
	for _, windowDays := range authorAnalyticsWindows {
		cutoff := computeWindowCutoff(windowDays)
		if err := h.upsertAuthorEngagementWindowTx(ctx, tx, pubkey, windowDays, cutoff, engagementVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorTopicWindowTx(ctx, tx, pubkey, windowDays, cutoff, topicVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorMediaMixWindowTx(ctx, tx, pubkey, windowDays, cutoff, mediaVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorActivityWindowsTx(ctx, tx, pubkey, windowDays, cutoff, activityWindowVersion); err != nil {
			return err
		}
		if err := h.upsertAuthorPostingPatternsTx(ctx, tx, pubkey, windowDays, cutoff, postingPatternVersion); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handlers) upsertAuthorEngagementWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO author_engagement_stats (
			pubkey,
			window_days,
			post_count,
			note_count,
			reply_count,
			active_days,
			engagement_received,
			engagement_given,
			cadence_posts_per_day,
			cadence_posts_per_active_day,
			recent_activity_at,
			derivation_version
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			COALESCE(SUM(a.post_count), 0)::bigint AS post_count,
			COALESCE(SUM(a.note_count), 0)::bigint AS note_count,
			COALESCE(SUM(a.reply_count), 0)::bigint AS reply_count,
			COALESCE(COUNT(*) FILTER (WHERE a.post_count > 0), 0)::int AS active_days,
			COALESCE(SUM(a.engagement_received), 0)::bigint AS engagement_received,
			COALESCE(SUM(a.engagement_given), 0)::bigint AS engagement_given,
			COALESCE(SUM(a.post_count), 0)::double precision / $2::double precision AS cadence_posts_per_day,
			CASE
				WHEN COALESCE(COUNT(*) FILTER (WHERE a.post_count > 0), 0) = 0 THEN 0
				ELSE COALESCE(SUM(a.post_count), 0)::double precision
					/ (COUNT(*) FILTER (WHERE a.post_count > 0))::double precision
			END AS cadence_posts_per_active_day,
			(
				SELECT MAX(e.created_at)
				FROM events e
				WHERE e.pubkey = $1
				  AND e.kind = 1
				  AND e.created_at >= $3
			) AS recent_activity_at,
			$4 AS derivation_version
		FROM author_activity_daily a
		WHERE a.pubkey = $1
		  AND a.activity_date >= to_timestamp($3)::date
		ON CONFLICT (pubkey, window_days) DO UPDATE
		SET post_count = EXCLUDED.post_count,
		    note_count = EXCLUDED.note_count,
		    reply_count = EXCLUDED.reply_count,
		    active_days = EXCLUDED.active_days,
		    engagement_received = EXCLUDED.engagement_received,
		    engagement_given = EXCLUDED.engagement_given,
		    cadence_posts_per_day = EXCLUDED.cadence_posts_per_day,
		    cadence_posts_per_active_day = EXCLUDED.cadence_posts_per_active_day,
		    recent_activity_at = EXCLUDED.recent_activity_at,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, windowDays, cutoff, version)
	if err != nil {
		return fmt.Errorf("upsert author engagement stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorTopicWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM author_topic_stats
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays); err != nil {
		return fmt.Errorf("delete prior author topic stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_topic_stats (
			pubkey,
			window_days,
			hashtag,
			usage_count,
			active_days,
			derivation_version
		)
		SELECT
			$1,
			$2,
			eh.hashtag,
			COUNT(*)::bigint AS usage_count,
			COUNT(DISTINCT to_timestamp(eh.created_at)::date)::int AS active_days,
			$3
		FROM event_hashtags eh
		WHERE eh.author_pubkey = $1
		  AND eh.created_at >= $4
		GROUP BY eh.hashtag
		ON CONFLICT (pubkey, window_days, hashtag) DO UPDATE
		SET usage_count = EXCLUDED.usage_count,
		    active_days = EXCLUDED.active_days,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, windowDays, version, cutoff); err != nil {
		return fmt.Errorf("upsert author topic stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorMediaMixWindowTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	_, err := tx.Exec(ctx, `
		WITH media_events AS (
			SELECT
				nds.has_image,
				nds.has_video,
				nds.has_link,
				nds.has_article,
				nds.attachment_count
			FROM note_discovery_stats nds
			WHERE nds.author_pubkey = $1
			  AND nds.created_at >= $2
		),
		classified AS (
			SELECT
				has_image,
				has_video,
				has_link,
				has_article,
				attachment_count
			FROM media_events
		)
		INSERT INTO author_media_mix_stats (
			pubkey,
			window_days,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count,
			derivation_version
		)
		SELECT
			$1,
			$3,
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE has_image)::bigint,
			COUNT(*) FILTER (WHERE has_video)::bigint,
			COUNT(*) FILTER (WHERE has_link)::bigint,
			COUNT(*) FILTER (WHERE has_article)::bigint,
			COUNT(*) FILTER (WHERE NOT has_image AND NOT has_video AND NOT has_link AND NOT has_article)::bigint,
			COALESCE(SUM(attachment_count), 0)::bigint,
			$4
		FROM classified
		ON CONFLICT (pubkey, window_days) DO UPDATE
		SET total_posts = EXCLUDED.total_posts,
		    with_image_count = EXCLUDED.with_image_count,
		    with_video_count = EXCLUDED.with_video_count,
		    with_link_count = EXCLUDED.with_link_count,
		    with_article_count = EXCLUDED.with_article_count,
		    text_only_count = EXCLUDED.text_only_count,
		    total_attachment_count = EXCLUDED.total_attachment_count,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, cutoff, windowDays, version)
	if err != nil {
		return fmt.Errorf("upsert author media mix stats for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorActivityWindowsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM author_activity_windows
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays); err != nil {
		return fmt.Errorf("delete prior author activity windows for %s window=%dd: %w", pubkey, windowDays, err)
	}

	_, err := tx.Exec(ctx, `
		WITH received_events AS (
			SELECT
				to_timestamp(e.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'reply'::text AS interaction_type
			FROM event_references er
			INNER JOIN events e ON e.id = er.source_event_id
			INNER JOIN events target ON target.id = er.target_event_id
			WHERE er.relation = 'reply'
			  AND target.pubkey = $1
			  AND target.kind = 1
			  AND e.pubkey <> $1
			  AND e.created_at >= $2
			UNION ALL
			SELECT
				to_timestamp(re.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'reaction'::text AS interaction_type
			FROM reaction_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND target.kind = 1
			  AND re.reactor_pubkey <> $1
			  AND re.created_at >= $2
			UNION ALL
			SELECT
				to_timestamp(re.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'repost'::text AS interaction_type
			FROM repost_events re
			INNER JOIN events target ON target.id = re.target_event_id
			WHERE target.pubkey = $1
			  AND target.kind = 1
			  AND re.reposter_pubkey <> $1
			  AND re.created_at >= $2
			UNION ALL
			SELECT
				to_timestamp(zr.created_at) AT TIME ZONE 'UTC' AS engaged_at,
				'zap'::text AS interaction_type
			FROM zap_receipts zr
			INNER JOIN events target ON target.id = zr.target_event_id
			WHERE target.pubkey = $1
			  AND target.kind = 1
			  AND zr.sender_pubkey IS NOT NULL
			  AND zr.sender_pubkey <> $1
			  AND zr.created_at >= $2
		)
		INSERT INTO author_activity_windows (
			pubkey,
			window_days,
			day_of_week,
			hour_of_day,
			engagement_received,
			reply_received,
			reaction_received,
			repost_received,
			zap_received,
			derivation_version
		)
		SELECT
			$1,
			$3,
			EXTRACT(DOW FROM r.engaged_at)::smallint,
			EXTRACT(HOUR FROM r.engaged_at)::smallint,
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'reply')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'reaction')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'repost')::bigint,
			COUNT(*) FILTER (WHERE r.interaction_type = 'zap')::bigint,
			$4
		FROM received_events r
		GROUP BY
			EXTRACT(DOW FROM r.engaged_at),
			EXTRACT(HOUR FROM r.engaged_at)
		ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
		SET engagement_received = EXCLUDED.engagement_received,
		    reply_received = EXCLUDED.reply_received,
		    reaction_received = EXCLUDED.reaction_received,
		    repost_received = EXCLUDED.repost_received,
		    zap_received = EXCLUDED.zap_received,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, cutoff, windowDays, version)
	if err != nil {
		return fmt.Errorf("upsert author activity windows for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func (h *Handlers) upsertAuthorPostingPatternsTx(
	ctx context.Context,
	tx pgx.Tx,
	pubkey string,
	windowDays int,
	cutoff int64,
	version int,
) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM author_posting_patterns
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays); err != nil {
		return fmt.Errorf("delete prior author posting patterns for %s window=%dd: %w", pubkey, windowDays, err)
	}

	_, err := tx.Exec(ctx, `
		WITH authored AS (
			SELECT
				to_timestamp(e.created_at) AT TIME ZONE 'UTC' AS posted_at,
				EXISTS (
					SELECT 1
					FROM event_references er
					WHERE er.source_event_id = e.id
					  AND er.relation = 'reply'
				) AS is_reply
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
		)
		INSERT INTO author_posting_patterns (
			pubkey,
			window_days,
			day_of_week,
			hour_of_day,
			post_count,
			note_count,
			reply_count,
			derivation_version
		)
		SELECT
			$1,
			$3,
			EXTRACT(DOW FROM a.posted_at)::smallint,
			EXTRACT(HOUR FROM a.posted_at)::smallint,
			COUNT(*)::bigint,
			COUNT(*) FILTER (WHERE NOT a.is_reply)::bigint,
			COUNT(*) FILTER (WHERE a.is_reply)::bigint,
			$4
		FROM authored a
		GROUP BY
			EXTRACT(DOW FROM a.posted_at),
			EXTRACT(HOUR FROM a.posted_at)
		ON CONFLICT (pubkey, window_days, day_of_week, hour_of_day) DO UPDATE
		SET post_count = EXCLUDED.post_count,
		    note_count = EXCLUDED.note_count,
		    reply_count = EXCLUDED.reply_count,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
	`, pubkey, cutoff, windowDays, version)
	if err != nil {
		return fmt.Errorf("upsert author posting patterns for %s window=%dd: %w", pubkey, windowDays, err)
	}
	return nil
}

func computeWindowCutoff(windowDays int) int64 {
	if windowDays <= 1 {
		return 0
	}
	seconds := int64(windowDays * 24 * 60 * 60)
	return time.Now().UTC().Unix() - seconds
}
