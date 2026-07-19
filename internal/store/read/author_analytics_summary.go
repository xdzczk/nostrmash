package read

import (
	"context"
	"fmt"
	"strings"
)

func (s *Read) GetAuthorAnalyticsSummary(ctx context.Context, pubkey string) ([]AuthorAnalyticsSummaryProjection, error) {
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

func (s *Read) GetAuthorQuoteRepostRecentActivity(
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
