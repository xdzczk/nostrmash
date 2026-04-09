package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
