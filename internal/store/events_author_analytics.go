package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	authorWeightedReplyWeight      = 3.0
	authorWeightedRepostWeight     = 2.0
	authorWeightedReactionWeight   = 1.0
	authorWeightedZapWeight        = 2.0
	authorWeightedZapDivisor       = 100000.0
	maxAuthorRecycleCandidateLimit = 50
)

func (s *PostgresStore) GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) ([]AuthorAnalyticsSummaryProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			es.pubkey,
			es.window_days,
			es.post_count,
			es.note_count,
			es.reply_count,
			es.active_days,
			es.engagement_received,
			es.engagement_given,
			es.cadence_posts_per_day,
			es.cadence_posts_per_active_day,
			es.recent_activity_at,
			COALESCE(mm.total_posts, 0),
			COALESCE(mm.with_image_count, 0),
			COALESCE(mm.with_video_count, 0),
			COALESCE(mm.with_link_count, 0),
			COALESCE(mm.with_article_count, 0),
			COALESCE(mm.text_only_count, 0),
			COALESCE(mm.total_attachment_count, 0),
			COALESCE((
				SELECT COUNT(*)::bigint
				FROM repost_events re
				WHERE re.reposter_pubkey = es.pubkey
				  AND re.created_at >= extract(epoch FROM now())::bigint - (es.window_days::bigint * 24 * 60 * 60)
				  AND NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NOT NULL
			), 0),
			COALESCE((
				SELECT COUNT(*)::bigint
				FROM repost_events re
				WHERE re.reposter_pubkey = es.pubkey
				  AND re.created_at >= extract(epoch FROM now())::bigint - (es.window_days::bigint * 24 * 60 * 60)
				  AND NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NULL
			), 0),
			COALESCE((
				SELECT COUNT(*)::bigint
				FROM repost_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE target.pubkey = es.pubkey
				  AND target.kind = 1
				  AND re.created_at >= extract(epoch FROM now())::bigint - (es.window_days::bigint * 24 * 60 * 60)
				  AND NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NOT NULL
			), 0),
			COALESCE((
				SELECT COUNT(*)::bigint
				FROM repost_events re
				INNER JOIN events target ON target.id = re.target_event_id
				WHERE target.pubkey = es.pubkey
				  AND target.kind = 1
				  AND re.created_at >= extract(epoch FROM now())::bigint - (es.window_days::bigint * 24 * 60 * 60)
				  AND NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NULL
			), 0)
		FROM author_engagement_stats es
		LEFT JOIN author_media_mix_stats mm
			ON mm.pubkey = es.pubkey
		   AND mm.window_days = es.window_days
		WHERE es.pubkey = $1
		ORDER BY es.window_days ASC
	`, pubkey)
	if err != nil {
		return nil, fmt.Errorf("get author analytics summary: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorAnalyticsSummaryProjection, 0, 3)
	for rows.Next() {
		var row AuthorAnalyticsSummaryProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
			&row.ActiveDays,
			&row.EngagementReceived,
			&row.EngagementGiven,
			&row.CadencePostsPerDay,
			&row.CadencePostsPerActiveDay,
			&row.RecentActivityAt,
			&row.MediaMix.TotalPosts,
			&row.MediaMix.WithImageCount,
			&row.MediaMix.WithVideoCount,
			&row.MediaMix.WithLinkCount,
			&row.MediaMix.WithArticleCount,
			&row.MediaMix.TextOnlyCount,
			&row.MediaMix.TotalAttachmentCount,
			&row.QuoteRepost.QuotesMade,
			&row.QuoteRepost.RepostsMade,
			&row.QuoteRepost.QuotesReceived,
			&row.QuoteRepost.RepostsReceived,
		); err != nil {
			return nil, fmt.Errorf("scan author analytics summary row: %w", err)
		}
		row.MediaMix.Pubkey = row.Pubkey
		row.MediaMix.WindowDays = row.WindowDays
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author analytics summary rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorQuoteRepostRecentActivity(
	ctx context.Context,
	pubkey string,
	limit int,
) ([]QuoteRepostActivityProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 8
	}
	if limit > 25 {
		limit = 25
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			re.event_id,
			re.reposter_pubkey,
			re.created_at,
			CASE
				WHEN NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NOT NULL THEN 'quote'
				ELSE 'repost'
			END AS action,
			COALESCE(re.quote, ''),
			target.id,
			target.pubkey,
			target.created_at,
			COALESCE(target.content, '')
		FROM repost_events re
		INNER JOIN events target ON target.id = re.target_event_id
		WHERE re.reposter_pubkey = $1
		  AND target.kind = 1
		ORDER BY re.created_at DESC, re.event_id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author quote/repost recent activity: %w", err)
	}
	defer rows.Close()

	out := make([]QuoteRepostActivityProjection, 0, limit)
	for rows.Next() {
		var row QuoteRepostActivityProjection
		if err := rows.Scan(
			&row.EventID,
			&row.ActorPubkey,
			&row.CreatedAt,
			&row.Action,
			&row.Quote,
			&row.LinkedNote.EventID,
			&row.LinkedNote.AuthorPubkey,
			&row.LinkedNote.CreatedAt,
			&row.LinkedNote.Content,
		); err != nil {
			return nil, fmt.Errorf("scan author quote/repost recent activity row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author quote/repost recent activity rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorTopicStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopicStatsProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, window_days, hashtag, usage_count, active_days
		FROM author_topic_stats
		WHERE pubkey = $1
		  AND window_days = $2
		ORDER BY usage_count DESC, hashtag ASC
		LIMIT $3
	`, pubkey, windowDays, limit)
	if err != nil {
		return nil, fmt.Errorf("get author topic stats: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorTopicStatsProjection, 0, limit)
	for rows.Next() {
		var row AuthorTopicStatsProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.Hashtag,
			&row.UsageCount,
			&row.ActiveDays,
		); err != nil {
			return nil, fmt.Errorf("scan author topic stats row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author topic stats rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorTopLanguages(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]LanguageSummary, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if windowDays <= 0 {
		windowDays = 30
	}
	if limit <= 0 {
		limit = 8
	}
	if limit > 20 {
		limit = 20
	}
	cutoff := time.Now().UTC().Unix() - int64(windowDays*24*60*60)
	rows, err := s.pool.Query(ctx, `
		SELECT
			COALESCE(primary_language, 'und') AS language,
			COUNT(*)::bigint AS count_value
		FROM note_discovery_stats
		WHERE author_pubkey = $1
		  AND created_at >= $2
		GROUP BY COALESCE(primary_language, 'und')
		ORDER BY count_value DESC, language ASC
		LIMIT $3
	`, pubkey, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("get author top languages: %w", err)
	}
	defer rows.Close()
	out := make([]LanguageSummary, 0, limit)
	for rows.Next() {
		var row LanguageSummary
		if err := rows.Scan(&row.Language, &row.Count); err != nil {
			return nil, fmt.Errorf("scan author top language row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author top language rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorMediaMixStats(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorMediaMixStatsProjection, error) {
	out := AuthorMediaMixStatsProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	err := s.pool.QueryRow(ctx, `
		SELECT
			pubkey,
			window_days,
			total_posts,
			with_image_count,
			with_video_count,
			with_link_count,
			with_article_count,
			text_only_count,
			total_attachment_count
		FROM author_media_mix_stats
		WHERE pubkey = $1
		  AND window_days = $2
	`, pubkey, windowDays).Scan(
		&out.Pubkey,
		&out.WindowDays,
		&out.TotalPosts,
		&out.WithImageCount,
		&out.WithVideoCount,
		&out.WithLinkCount,
		&out.WithArticleCount,
		&out.TextOnlyCount,
		&out.TotalAttachmentCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			out.Pubkey = pubkey
			out.WindowDays = windowDays
			return out, nil
		}
		return out, fmt.Errorf("get author media mix stats: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorActivityWindowBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]AuthorActivityWindowBucketProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}

	rows, err := s.pool.Query(ctx, `
		WITH calendar AS (
			SELECT d AS day_of_week, h AS hour_of_day
			FROM generate_series(0, 6) AS d
			CROSS JOIN generate_series(0, 23) AS h
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			c.day_of_week,
			c.hour_of_day,
			COALESCE(w.engagement_received, 0),
			COALESCE(w.reply_received, 0),
			COALESCE(w.reaction_received, 0),
			COALESCE(w.repost_received, 0),
			COALESCE(w.zap_received, 0)
		FROM calendar c
		LEFT JOIN author_activity_windows w
			ON w.pubkey = $1
		   AND w.window_days = $2
		   AND w.day_of_week = c.day_of_week
		   AND w.hour_of_day = c.hour_of_day
		ORDER BY c.day_of_week ASC, c.hour_of_day ASC
	`, pubkey, windowDays)
	if err != nil {
		return nil, fmt.Errorf("get author activity window buckets: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorActivityWindowBucketProjection, 0, 7*24)
	for rows.Next() {
		var row AuthorActivityWindowBucketProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.EngagementReceived,
			&row.ReplyReceived,
			&row.ReactionReceived,
			&row.RepostReceived,
			&row.ZapReceived,
		); err != nil {
			return nil, fmt.Errorf("scan author activity window bucket row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author activity window bucket rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorPostingPatternBuckets(
	ctx context.Context,
	pubkey string,
	windowDays int,
) ([]AuthorPostingPatternBucketProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}

	rows, err := s.pool.Query(ctx, `
		WITH calendar AS (
			SELECT d AS day_of_week, h AS hour_of_day
			FROM generate_series(0, 6) AS d
			CROSS JOIN generate_series(0, 23) AS h
		)
		SELECT
			$1 AS pubkey,
			$2 AS window_days,
			c.day_of_week,
			c.hour_of_day,
			COALESCE(p.post_count, 0),
			COALESCE(p.note_count, 0),
			COALESCE(p.reply_count, 0)
		FROM calendar c
		LEFT JOIN author_posting_patterns p
			ON p.pubkey = $1
		   AND p.window_days = $2
		   AND p.day_of_week = c.day_of_week
		   AND p.hour_of_day = c.hour_of_day
		ORDER BY c.day_of_week ASC, c.hour_of_day ASC
	`, pubkey, windowDays)
	if err != nil {
		return nil, fmt.Errorf("get author posting pattern buckets: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorPostingPatternBucketProjection, 0, 7*24)
	for rows.Next() {
		var row AuthorPostingPatternBucketProjection
		if err := rows.Scan(
			&row.Pubkey,
			&row.WindowDays,
			&row.DayOfWeek,
			&row.HourOfDay,
			&row.PostCount,
			&row.NoteCount,
			&row.ReplyCount,
		); err != nil {
			return nil, fmt.Errorf("scan author posting pattern bucket row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author posting pattern bucket rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorTopNotes(
	ctx context.Context,
	pubkey string,
	windowDays int,
	limit int,
) ([]AuthorTopNoteProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	cutoff := time.Now().UTC().Unix() - int64(windowDays*24*60*60)
	rows, err := s.pool.Query(ctx, `
		WITH notes AS (
			SELECT e.id, e.created_at, e.content, e.raw_json::jsonb AS raw_json
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
		),
		reply_counts AS (
			SELECT c.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reply_count_contributions c
			JOIN events src ON src.id = c.source_event_id
			JOIN notes n ON n.id = c.target_event_id
			WHERE src.created_at >= $2
			GROUP BY c.target_event_id
		),
		reaction_counts AS (
			SELECT re.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reaction_events re
			JOIN notes n ON n.id = re.target_event_id
			WHERE re.created_at >= $2
			GROUP BY re.target_event_id
		),
		repost_counts AS (
			SELECT rp.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM repost_events rp
			JOIN notes n ON n.id = rp.target_event_id
			WHERE rp.created_at >= $2
			GROUP BY rp.target_event_id
		),
		zap_counts AS (
			SELECT z.event_id, COUNT(*)::bigint AS count_value, COALESCE(SUM(z.amount_sats * 1000), 0)::bigint AS msats
			FROM zap_receipts z
			JOIN notes n ON n.id = z.event_id
			WHERE z.created_at >= $2
			GROUP BY z.event_id
		),
		note_topics AS (
			SELECT
				eh.event_id,
				eh.hashtag,
				ROW_NUMBER() OVER (PARTITION BY eh.event_id ORDER BY eh.hashtag ASC) AS rn
			FROM event_hashtags eh
			JOIN notes n ON n.id = eh.event_id
		)
		SELECT
			n.id,
			n.created_at,
			COALESCE(n.content, ''),
			COALESCE(rc.count_value, 0),
			COALESCE(rxc.count_value, 0),
			COALESCE(rpc.count_value, 0),
			COALESCE(zc.count_value, 0),
			COALESCE(zc.msats, 0),
			(
				(COALESCE(rc.count_value, 0)::double precision * $4) +
				(COALESCE(rpc.count_value, 0)::double precision * $5) +
				(COALESCE(rxc.count_value, 0)::double precision * $6) +
				(COALESCE(zc.count_value, 0)::double precision * $7) +
				(COALESCE(zc.msats, 0)::double precision / $8)
			) AS weighted_engagement,
			CASE
				WHEN COALESCE(nds.has_video, FALSE) THEN 'video'
				WHEN COALESCE(nds.has_image, FALSE) THEN 'image'
				WHEN COALESCE(nds.has_article, FALSE) THEN 'article'
				WHEN COALESCE(nds.has_link, FALSE) THEN 'link'
				ELSE 'text'
			END AS media_segment,
			t.hashtag
		FROM notes n
		LEFT JOIN reply_counts rc ON rc.event_id = n.id
		LEFT JOIN reaction_counts rxc ON rxc.event_id = n.id
		LEFT JOIN repost_counts rpc ON rpc.event_id = n.id
		LEFT JOIN zap_counts zc ON zc.event_id = n.id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = n.id
		LEFT JOIN note_topics t ON t.event_id = n.id AND t.rn = 1
		ORDER BY weighted_engagement DESC, n.created_at DESC, n.id ASC
		LIMIT $3
	`, pubkey, cutoff, limit,
		authorWeightedReplyWeight,
		authorWeightedRepostWeight,
		authorWeightedReactionWeight,
		authorWeightedZapWeight,
		authorWeightedZapDivisor,
	)
	if err != nil {
		return nil, fmt.Errorf("get author top notes: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorTopNoteProjection, 0, limit)
	for rows.Next() {
		var row AuthorTopNoteProjection
		if err := rows.Scan(
			&row.EventID,
			&row.CreatedAt,
			&row.Content,
			&row.ReplyCount,
			&row.ReactionCount,
			&row.RepostCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.WeightedEngagement,
			&row.MediaSegment,
			&row.PrimaryTopicHashtag,
		); err != nil {
			return nil, fmt.Errorf("scan author top note row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author top note rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorRecycleCandidates(
	ctx context.Context,
	pubkey string,
	windowDays int,
	minAgeDays int,
	minPerformancePercentile float64,
	includeReplies bool,
	excludeRecentlyReposted bool,
	recentRepostWindowDays int,
	limit int,
) ([]AuthorRecycleCandidateProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > maxAuthorRecycleCandidateLimit {
		limit = maxAuthorRecycleCandidateLimit
	}
	if windowDays <= 0 {
		windowDays = 90
	}
	if minAgeDays <= 0 {
		minAgeDays = 30
	}
	if minAgeDays >= windowDays {
		return nil, fmt.Errorf("min age must be less than window")
	}
	if minPerformancePercentile < 0 {
		minPerformancePercentile = 0
	}
	if minPerformancePercentile > 100 {
		minPerformancePercentile = 100
	}
	if recentRepostWindowDays <= 0 {
		recentRepostWindowDays = 30
	}

	nowUnix := time.Now().UTC().Unix()
	windowCutoff := nowUnix - int64(windowDays*24*60*60)
	maxCreatedAt := nowUnix - int64(minAgeDays*24*60*60)
	recentRepostCutoff := nowUnix - int64(recentRepostWindowDays*24*60*60)

	rows, err := s.pool.Query(ctx, `
		WITH notes AS (
			SELECT e.id, e.created_at, e.content
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
			  AND e.created_at <= $3
			  AND ($4 OR NOT EXISTS (
			  	SELECT 1
			  	FROM thread_edges te
			  	WHERE te.child_event_id = e.id
			  ))
		),
		reply_counts AS (
			SELECT c.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reply_count_contributions c
			JOIN events src ON src.id = c.source_event_id
			JOIN notes n ON n.id = c.target_event_id
			WHERE src.created_at >= $2
			GROUP BY c.target_event_id
		),
		reaction_counts AS (
			SELECT re.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reaction_events re
			JOIN notes n ON n.id = re.target_event_id
			WHERE re.created_at >= $2
			GROUP BY re.target_event_id
		),
		repost_counts AS (
			SELECT rp.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM repost_events rp
			JOIN notes n ON n.id = rp.target_event_id
			WHERE rp.created_at >= $2
			GROUP BY rp.target_event_id
		),
		zap_counts AS (
			SELECT z.event_id, COUNT(*)::bigint AS count_value, COALESCE(SUM(z.amount_sats * 1000), 0)::bigint AS msats
			FROM zap_receipts z
			JOIN notes n ON n.id = z.event_id
			WHERE z.created_at >= $2
			GROUP BY z.event_id
		),
		note_topics AS (
			SELECT
				eh.event_id,
				eh.hashtag,
				ROW_NUMBER() OVER (PARTITION BY eh.event_id ORDER BY eh.hashtag ASC) AS rn
			FROM event_hashtags eh
			JOIN notes n ON n.id = eh.event_id
		),
		scored AS (
			SELECT
				n.id,
				n.created_at,
				COALESCE(n.content, '') AS content,
				COALESCE(rc.count_value, 0)::bigint AS reply_count,
				COALESCE(rxc.count_value, 0)::bigint AS reaction_count,
				COALESCE(rpc.count_value, 0)::bigint AS repost_count,
				COALESCE(zc.count_value, 0)::bigint AS zap_count,
				COALESCE(zc.msats, 0)::bigint AS zap_msats,
				(
					(COALESCE(rc.count_value, 0)::double precision * $8) +
					(COALESCE(rpc.count_value, 0)::double precision * $9) +
					(COALESCE(rxc.count_value, 0)::double precision * $10) +
					(COALESCE(zc.count_value, 0)::double precision * $11) +
					(COALESCE(zc.msats, 0)::double precision / $12)
				) AS weighted_engagement,
				EXISTS (
					SELECT 1
					FROM repost_events rp_recent
					WHERE rp_recent.target_event_id = n.id
					  AND rp_recent.created_at >= $5
				) AS has_recent_repost_marker,
				EXISTS (
					SELECT 1
					FROM thread_edges te
					WHERE te.child_event_id = n.id
				) AS is_reply,
				CASE
					WHEN COALESCE(nds.has_video, FALSE) THEN 'video'
					WHEN COALESCE(nds.has_image, FALSE) THEN 'image'
					WHEN COALESCE(nds.has_article, FALSE) THEN 'article'
					WHEN COALESCE(nds.has_link, FALSE) THEN 'link'
					ELSE 'text'
				END AS media_segment,
				t.hashtag
			FROM notes n
			LEFT JOIN reply_counts rc ON rc.event_id = n.id
			LEFT JOIN reaction_counts rxc ON rxc.event_id = n.id
			LEFT JOIN repost_counts rpc ON rpc.event_id = n.id
			LEFT JOIN zap_counts zc ON zc.event_id = n.id
			LEFT JOIN note_discovery_stats nds ON nds.event_id = n.id
			LEFT JOIN note_topics t ON t.event_id = n.id AND t.rn = 1
		),
		ranked AS (
			SELECT
				s.*,
				100.0 - (
					PERCENT_RANK() OVER (
						ORDER BY s.weighted_engagement DESC, s.created_at DESC, s.id ASC
					) * 100.0
				) AS performance_percentile
			FROM scored s
		)
		SELECT
			r.id,
			r.created_at,
			r.content,
			r.reply_count,
			r.reaction_count,
			r.repost_count,
			r.zap_count,
			r.zap_msats,
			r.weighted_engagement,
			r.performance_percentile,
			r.has_recent_repost_marker,
			r.is_reply,
			r.media_segment,
			r.hashtag
		FROM ranked r
		WHERE r.performance_percentile >= $6
		  AND (NOT $7 OR NOT r.has_recent_repost_marker)
		ORDER BY r.weighted_engagement DESC, r.created_at DESC, r.id ASC
		LIMIT $13
	`, pubkey, windowCutoff, maxCreatedAt, includeReplies, recentRepostCutoff,
		minPerformancePercentile, excludeRecentlyReposted,
		authorWeightedReplyWeight,
		authorWeightedRepostWeight,
		authorWeightedReactionWeight,
		authorWeightedZapWeight,
		authorWeightedZapDivisor,
		limit)
	if err != nil {
		return nil, fmt.Errorf("get author recycle candidates: %w", err)
	}
	defer rows.Close()

	out := make([]AuthorRecycleCandidateProjection, 0, limit)
	for rows.Next() {
		var row AuthorRecycleCandidateProjection
		if err := rows.Scan(
			&row.EventID,
			&row.CreatedAt,
			&row.Content,
			&row.ReplyCount,
			&row.ReactionCount,
			&row.RepostCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.WeightedEngagement,
			&row.PerformancePercentile,
			&row.HasRecentRepostMarker,
			&row.IsReply,
			&row.MediaSegment,
			&row.PrimaryTopicHashtag,
		); err != nil {
			return nil, fmt.Errorf("scan author recycle candidate row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author recycle candidate rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetAuthorPerformanceAggregate(
	ctx context.Context,
	pubkey string,
	windowDays int,
) (AuthorPerformanceAggregateProjection, AuthorPerformanceAggregateProjection, error) {
	current := AuthorPerformanceAggregateProjection{}
	previous := AuthorPerformanceAggregateProjection{}
	if s == nil || s.pool == nil {
		return current, previous, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return current, previous, fmt.Errorf("pubkey is required")
	}
	nowUnix := time.Now().UTC().Unix()
	windowSeconds := int64(windowDays * 24 * 60 * 60)
	currentStart := nowUnix - windowSeconds
	previousStart := currentStart - windowSeconds

	var err error
	current, err = s.getAuthorPerformanceAggregateForRange(ctx, pubkey, currentStart, nowUnix)
	if err != nil {
		return current, previous, err
	}
	previous, err = s.getAuthorPerformanceAggregateForRange(ctx, pubkey, previousStart, currentStart)
	if err != nil {
		return current, previous, err
	}
	return current, previous, nil
}

func (s *PostgresStore) getAuthorPerformanceAggregateForRange(
	ctx context.Context,
	pubkey string,
	startInclusive int64,
	endExclusive int64,
) (AuthorPerformanceAggregateProjection, error) {
	out := AuthorPerformanceAggregateProjection{}
	err := s.pool.QueryRow(ctx, `
		WITH notes AS (
			SELECT e.id
			FROM events e
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
			  AND e.created_at < $3
		),
		reply_counts AS (
			SELECT c.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reply_count_contributions c
			JOIN events src ON src.id = c.source_event_id
			JOIN notes n ON n.id = c.target_event_id
			WHERE src.created_at >= $2
			  AND src.created_at < $3
			GROUP BY c.target_event_id
		),
		reaction_counts AS (
			SELECT re.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM reaction_events re
			JOIN notes n ON n.id = re.target_event_id
			WHERE re.created_at >= $2
			  AND re.created_at < $3
			GROUP BY re.target_event_id
		),
		repost_counts AS (
			SELECT rp.target_event_id AS event_id, COUNT(*)::bigint AS count_value
			FROM repost_events rp
			JOIN notes n ON n.id = rp.target_event_id
			WHERE rp.created_at >= $2
			  AND rp.created_at < $3
			GROUP BY rp.target_event_id
		),
		zap_counts AS (
			SELECT z.event_id, COUNT(*)::bigint AS count_value, COALESCE(SUM(z.amount_sats * 1000), 0)::bigint AS msats
			FROM zap_receipts z
			JOIN notes n ON n.id = z.event_id
			WHERE z.created_at >= $2
			  AND z.created_at < $3
			GROUP BY z.event_id
		),
		note_scores AS (
			SELECT
				n.id,
				COALESCE(rc.count_value, 0)::bigint AS reply_count,
				COALESCE(rxc.count_value, 0)::bigint AS reaction_count,
				COALESCE(rpc.count_value, 0)::bigint AS repost_count,
				COALESCE(zc.count_value, 0)::bigint AS zap_count,
				COALESCE(zc.msats, 0)::bigint AS zap_msats,
				(
					(COALESCE(rc.count_value, 0)::double precision * $4) +
					(COALESCE(rpc.count_value, 0)::double precision * $5) +
					(COALESCE(rxc.count_value, 0)::double precision * $6) +
					(COALESCE(zc.count_value, 0)::double precision * $7) +
					(COALESCE(zc.msats, 0)::double precision / $8)
				) AS weighted_engagement
			FROM notes n
			LEFT JOIN reply_counts rc ON rc.event_id = n.id
			LEFT JOIN reaction_counts rxc ON rxc.event_id = n.id
			LEFT JOIN repost_counts rpc ON rpc.event_id = n.id
			LEFT JOIN zap_counts zc ON zc.event_id = n.id
		)
		SELECT
			COALESCE(COUNT(*), 0)::bigint AS note_count,
			COALESCE(SUM(ns.weighted_engagement), 0)::double precision AS total_weighted_engagement,
			COALESCE(AVG(ns.weighted_engagement), 0)::double precision AS average_weighted_engagement,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ns.weighted_engagement), 0)::double precision AS median_weighted_engagement,
			COALESCE(SUM(ns.reply_count), 0)::bigint AS total_reply_count,
			COALESCE(SUM(ns.reaction_count), 0)::bigint AS total_reaction_count,
			COALESCE(SUM(ns.repost_count), 0)::bigint AS total_repost_count,
			COALESCE(SUM(ns.zap_count), 0)::bigint AS total_zap_count,
			COALESCE(SUM(ns.zap_msats), 0)::bigint AS total_zap_msats,
			COALESCE(AVG(ns.reply_count), 0)::double precision AS average_reply_count,
			COALESCE(AVG(ns.reaction_count), 0)::double precision AS average_reaction_count,
			COALESCE(AVG(ns.repost_count), 0)::double precision AS average_repost_count,
			COALESCE(AVG(ns.zap_count), 0)::double precision AS average_zap_count,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ns.reply_count), 0)::double precision AS median_reply_count,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ns.reaction_count), 0)::double precision AS median_reaction_count,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ns.repost_count), 0)::double precision AS median_repost_count,
			COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ns.zap_count), 0)::double precision AS median_zap_count
		FROM note_scores ns
	`, pubkey, startInclusive, endExclusive,
		authorWeightedReplyWeight,
		authorWeightedRepostWeight,
		authorWeightedReactionWeight,
		authorWeightedZapWeight,
		authorWeightedZapDivisor,
	).Scan(
		&out.NoteCount,
		&out.TotalWeightedEngagement,
		&out.AverageWeightedEngagement,
		&out.MedianWeightedEngagement,
		&out.TotalReplyCount,
		&out.TotalReactionCount,
		&out.TotalRepostCount,
		&out.TotalZapCount,
		&out.TotalZapMSats,
		&out.AverageReplyCount,
		&out.AverageReactionCount,
		&out.AverageRepostCount,
		&out.AverageZapCount,
		&out.MedianReplyCount,
		&out.MedianReactionCount,
		&out.MedianRepostCount,
		&out.MedianZapCount,
	)
	if err != nil {
		return out, fmt.Errorf("get author performance aggregate range: %w", err)
	}
	return out, nil
}
