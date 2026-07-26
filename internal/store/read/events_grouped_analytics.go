package read

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

func (s *Read) GetGroupedNoteAnalytics(
	ctx context.Context,
	query GroupedNoteAnalyticsQuery,
) (GroupedNoteAnalyticsProjection, error) {
	out := GroupedNoteAnalyticsProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	query.Pubkey = strings.TrimSpace(query.Pubkey)
	query.GroupKind = strings.ToLower(strings.TrimSpace(query.GroupKind))
	query.GroupKey = strings.TrimSpace(query.GroupKey)
	query.MetadataTag = strings.ToLower(strings.TrimSpace(query.MetadataTag))
	if query.Pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}
	if query.GroupKind == "" {
		query.GroupKind = "hashtag"
	}
	if query.GroupKey == "" {
		return out, fmt.Errorf("group_key is required")
	}
	if query.WindowDays <= 0 {
		query.WindowDays = 30
	}
	if query.TopNotesLimit <= 0 {
		query.TopNotesLimit = 5
	}
	if query.TopNotesLimit > 20 {
		query.TopNotesLimit = 20
	}
	if query.TopicsLimit <= 0 {
		query.TopicsLimit = 5
	}
	if query.TopicsLimit > 20 {
		query.TopicsLimit = 20
	}
	switch query.GroupKind {
	case "hashtag":
	case "metadata":
		if query.MetadataTag == "" {
			return out, fmt.Errorf("metadata_tag is required for metadata grouping")
		}
	default:
		return out, fmt.Errorf("group_by must be one of: hashtag, metadata")
	}

	out.Pubkey = query.Pubkey
	out.WindowDays = query.WindowDays
	out.GroupKind = query.GroupKind
	out.GroupKey = query.GroupKey
	out.MetadataTag = query.MetadataTag

	cutoff := time.Now().UTC().Unix() - int64(query.WindowDays*24*60*60)
	notesCTE, args := groupedNotesCTE(query, cutoff)

	aggregateSQL := notesCTE + `
		, reply_counts AS (
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
		)
		SELECT
			COUNT(*)::bigint AS note_count,
			COALESCE(SUM(COALESCE(rc.count_value, 0)), 0)::bigint AS reply_count,
			COALESCE(SUM(COALESCE(rxc.count_value, 0)), 0)::bigint AS reaction_count,
			COALESCE(SUM(COALESCE(rpc.count_value, 0)), 0)::bigint AS repost_count,
			COALESCE(SUM(COALESCE(zc.count_value, 0)), 0)::bigint AS zap_count,
			COALESCE(SUM(COALESCE(zc.msats, 0)), 0)::bigint AS zap_msats,
			COALESCE(SUM(CASE WHEN COALESCE(nds.has_image, FALSE) THEN 1 ELSE 0 END), 0)::bigint AS with_image_count,
			COALESCE(SUM(CASE WHEN COALESCE(nds.has_video, FALSE) THEN 1 ELSE 0 END), 0)::bigint AS with_video_count,
			COALESCE(SUM(CASE WHEN COALESCE(nds.has_link, FALSE) THEN 1 ELSE 0 END), 0)::bigint AS with_link_count,
			COALESCE(SUM(CASE WHEN COALESCE(nds.has_article, FALSE) THEN 1 ELSE 0 END), 0)::bigint AS with_article_count,
			COALESCE(SUM(CASE
				WHEN COALESCE(nds.has_image, FALSE) OR COALESCE(nds.has_video, FALSE) OR COALESCE(nds.has_link, FALSE) OR COALESCE(nds.has_article, FALSE)
				THEN 0
				ELSE 1
			END), 0)::bigint AS text_only_count,
			COALESCE(SUM(COALESCE(nds.attachment_count, 0)), 0)::bigint AS total_attachment_count
		FROM notes n
		LEFT JOIN reply_counts rc ON rc.event_id = n.id
		LEFT JOIN reaction_counts rxc ON rxc.event_id = n.id
		LEFT JOIN repost_counts rpc ON rpc.event_id = n.id
		LEFT JOIN zap_counts zc ON zc.event_id = n.id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = n.id
	`
	if err := s.pool.QueryRow(ctx, aggregateSQL, args...).Scan(
		&out.NoteCount,
		&out.Engagement.ReplyCount,
		&out.Engagement.ReactionCount,
		&out.Engagement.RepostCount,
		&out.Engagement.ZapCount,
		&out.Engagement.ZapMSats,
		&out.Media.WithImageCount,
		&out.Media.WithVideoCount,
		&out.Media.WithLinkCount,
		&out.Media.WithArticleCount,
		&out.Media.TextOnlyCount,
		&out.Media.TotalAttachmentCount,
	); err != nil {
		return out, fmt.Errorf("get grouped note analytics aggregates: %w", err)
	}
	out.Media.TotalPosts = out.NoteCount

	topNotesSQL := notesCTE + fmt.Sprintf(`
		, reply_counts AS (
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
				(COALESCE(rc.count_value, 0)::double precision * %f) +
				(COALESCE(rpc.count_value, 0)::double precision * %f) +
				(COALESCE(rxc.count_value, 0)::double precision * %f) +
				(COALESCE(zc.count_value, 0)::double precision * %f) +
				(COALESCE(zc.msats, 0)::double precision / %f)
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
		LIMIT $%d
	`,
		authorWeightedReplyWeight,
		authorWeightedRepostWeight,
		authorWeightedReactionWeight,
		authorWeightedZapWeight,
		authorWeightedZapDivisor,
		len(args)+1,
	)
	topNotesArgs := append(append([]any{}, args...), query.TopNotesLimit)
	rows, err := s.pool.Query(ctx, topNotesSQL, topNotesArgs...)
	if err != nil {
		return out, fmt.Errorf("get grouped top notes: %w", err)
	}
	defer rows.Close()
	out.TopNotes = make([]GroupedTopNoteProjection, 0, query.TopNotesLimit)
	for rows.Next() {
		var row GroupedTopNoteProjection
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
			return out, fmt.Errorf("scan grouped top note row: %w", err)
		}
		out.TopNotes = append(out.TopNotes, row)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("read grouped top note rows: %w", err)
	}

	topicsSQL := notesCTE + fmt.Sprintf(`
		SELECT
			eh.hashtag,
			COUNT(*)::bigint AS usage_count,
			COUNT(DISTINCT to_timestamp(eh.created_at)::date)::int AS active_days
		FROM event_hashtags eh
		JOIN notes n ON n.id = eh.event_id
		WHERE eh.created_at <= %d
		GROUP BY eh.hashtag
		ORDER BY usage_count DESC, eh.hashtag ASC
		LIMIT $%d
	`, nostr.MaxUnixCreatedAt, len(args)+1)
	topicsArgs := append(append([]any{}, args...), query.TopicsLimit)
	topicRows, err := s.pool.Query(ctx, topicsSQL, topicsArgs...)
	if err != nil {
		return out, fmt.Errorf("get grouped top topics: %w", err)
	}
	defer topicRows.Close()
	out.TopTopics = make([]GroupedTopicSummaryProjection, 0, query.TopicsLimit)
	for topicRows.Next() {
		var row GroupedTopicSummaryProjection
		if err := topicRows.Scan(&row.Hashtag, &row.UsageCount, &row.ActiveDays); err != nil {
			return out, fmt.Errorf("scan grouped topic row: %w", err)
		}
		out.TopTopics = append(out.TopTopics, row)
	}
	if err := topicRows.Err(); err != nil {
		return out, fmt.Errorf("read grouped topic rows: %w", err)
	}

	return out, nil
}

func groupedNotesCTE(query GroupedNoteAnalyticsQuery, cutoff int64) (string, []any) {
	joinClause := ""
	filterClause := ""
	args := []any{query.Pubkey, cutoff}
	if query.GroupKind == "hashtag" {
		joinClause = "INNER JOIN event_hashtags gh ON gh.event_id = e.id"
		filterClause = "AND gh.hashtag = $3"
		args = append(args, query.GroupKey)
	} else {
		filterClause = "AND EXISTS (SELECT 1 FROM event_tags gt WHERE gt.event_id = e.id AND gt.tag_name = $3 AND gt.value = $4)"
		args = append(args, query.MetadataTag, query.GroupKey)
	}
	cte := fmt.Sprintf(`
		WITH notes AS (
			SELECT DISTINCT
				e.id,
				e.created_at,
				COALESCE(e.content, '') AS content
			FROM events e
			%s
			WHERE e.pubkey = $1
			  AND e.kind = 1
			  AND e.created_at >= $2
			  %s
		)
	`, joinClause, filterClause)
	return cte, args
}
