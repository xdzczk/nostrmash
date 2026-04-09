package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

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
		),
		note_media_tags AS (
			SELECT
				et.event_id,
				BOOL_OR(et.tag_name IN ('video', 'm') AND COALESCE(NULLIF(BTRIM(et.value), ''), '') <> '') AS has_video_tag,
				BOOL_OR(et.tag_name = 'image') AS has_image_tag,
				BOOL_OR(et.tag_name IN ('article', 'a')) AS has_article_tag,
				BOOL_OR(et.tag_name IN ('r', 'url', 'u')) AS has_link_tag
			FROM event_tags et
			JOIN notes n ON n.id = et.event_id
			GROUP BY et.event_id
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
				WHEN COALESCE(nds.has_video, nmt.has_video_tag, FALSE) THEN 'video'
				WHEN COALESCE(nds.has_image, nmt.has_image_tag, FALSE) THEN 'image'
				WHEN COALESCE(nds.has_article, nmt.has_article_tag, FALSE) THEN 'article'
				WHEN COALESCE(nds.has_link, nmt.has_link_tag, FALSE) THEN 'link'
				ELSE 'text'
			END AS media_segment,
			t.hashtag
		FROM notes n
		LEFT JOIN reply_counts rc ON rc.event_id = n.id
		LEFT JOIN reaction_counts rxc ON rxc.event_id = n.id
		LEFT JOIN repost_counts rpc ON rpc.event_id = n.id
		LEFT JOIN zap_counts zc ON zc.event_id = n.id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = n.id
		LEFT JOIN note_media_tags nmt ON nmt.event_id = n.id
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
