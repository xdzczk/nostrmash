package read

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"strings"
)

type NoteStats = readmodel.NoteStats

type NoteConversationVelocity = readmodel.NoteConversationVelocity

type RelatedNote = readmodel.RelatedNote

// GetNoteStats returns projection-backed interaction counters for a note page.
func (s *Read) GetNoteStats(ctx context.Context, eventID string) (NoteStats, error) {
	out := NoteStats{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return out, err
	}
	if !exists {
		return out, ErrNotFound
	}
	out.EventID = eventID
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE((SELECT count FROM reply_counts WHERE event_id = $1), 0),
			COALESCE((SELECT count FROM reaction_counts WHERE event_id = $1), 0),
			COALESCE((SELECT count FROM repost_counts WHERE event_id = $1), 0),
			COALESCE((SELECT zap_count FROM note_discovery_stats WHERE event_id = $1), 0),
			COALESCE((SELECT zap_msats FROM note_discovery_stats WHERE event_id = $1), 0),
			COALESCE((SELECT has_image FROM note_discovery_stats WHERE event_id = $1), FALSE),
			COALESCE((SELECT has_video FROM note_discovery_stats WHERE event_id = $1), FALSE),
			COALESCE((SELECT has_link FROM note_discovery_stats WHERE event_id = $1), FALSE),
			COALESCE((SELECT has_article FROM note_discovery_stats WHERE event_id = $1), FALSE),
			COALESCE((SELECT attachment_count FROM note_discovery_stats WHERE event_id = $1), 0)
	`, eventID).Scan(
		&out.ReplyCount,
		&out.ReactionCount,
		&out.RepostCount,
		&out.ZapCount,
		&out.ZapMSats,
		&out.HasImage,
		&out.HasVideo,
		&out.HasLink,
		&out.HasArticle,
		&out.AttachmentCount,
	); err != nil {
		return out, fmt.Errorf("get note stats: %w", err)
	}
	return out, nil
}

// GetNoteConversationVelocity returns bounded recent reply velocity for one note.
func (s *Read) GetNoteConversationVelocity(ctx context.Context, eventID string) (NoteConversationVelocity, error) {
	out := NoteConversationVelocity{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return out, err
	}
	if !exists {
		return out, ErrNotFound
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (
				WHERE te.child_created_at >= extract(epoch FROM now())::bigint - (24 * 60 * 60)
			) AS replies_24h,
			COUNT(*) FILTER (
				WHERE te.child_created_at >= extract(epoch FROM now())::bigint - (7 * 24 * 60 * 60)
			) AS replies_7d
		FROM thread_edges te
		WHERE te.parent_event_id = $1 OR te.root_event_id = $1
	`, eventID).Scan(&out.Replies24h, &out.Replies7d); err != nil {
		return out, fmt.Errorf("get note conversation velocity: %w", err)
	}
	return out, nil
}

// GetNoteQuoteRepostLinkage returns bounded quote/repost linkage rollups for one focal note.
func (s *Read) GetNoteQuoteRepostLinkage(
	ctx context.Context,
	eventID string,
	recentLimit int,
) (NoteQuoteRepostLinkageProjection, error) {
	out := NoteQuoteRepostLinkageProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	if recentLimit <= 0 {
		recentLimit = 5
	}
	if recentLimit > 20 {
		recentLimit = 20
	}
	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return out, err
	}
	if !exists {
		return out, ErrNotFound
	}
	out.EventID = eventID
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COALESCE(COUNT(*) FILTER (WHERE NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NOT NULL), 0)::bigint AS quote_count,
			COALESCE(COUNT(*) FILTER (WHERE NULLIF(BTRIM(COALESCE(re.quote, '')), '') IS NULL), 0)::bigint AS repost_count
		FROM repost_events re
		WHERE re.target_event_id = $1
	`, eventID).Scan(&out.QuoteCount, &out.RepostCount); err != nil {
		return out, fmt.Errorf("get note quote/repost rollup: %w", err)
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
			src.id,
			src.pubkey,
			src.created_at,
			COALESCE(src.content, '')
		FROM repost_events re
		INNER JOIN events src ON src.id = re.event_id
		WHERE re.target_event_id = $1
		ORDER BY re.created_at DESC, re.event_id DESC
		LIMIT $2
	`, eventID, recentLimit)
	if err != nil {
		return out, fmt.Errorf("get note quote/repost recent activity: %w", err)
	}
	defer rows.Close()

	out.RecentActivity = make([]QuoteRepostActivityProjection, 0, recentLimit)
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
			return out, fmt.Errorf("scan note quote/repost recent row: %w", err)
		}
		out.RecentActivity = append(out.RecentActivity, row)
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("read note quote/repost recent rows: %w", err)
	}
	return out, nil
}

// GetRelatedNotes returns bounded related-note candidates for one focal note.
func (s *Read) GetRelatedNotes(ctx context.Context, eventID string, limit int) ([]RelatedNote, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNotFound
	}

	rows, err := s.pool.Query(ctx, `
		WITH focal AS (
			SELECT
				e.id,
				e.pubkey,
				COALESCE(te.root_event_id, e.id) AS root_event_id
			FROM events e
			LEFT JOIN thread_edges te ON te.child_event_id = e.id
			WHERE e.id = $1
		),
		tag_overlap AS (
			SELECT
				h2.event_id,
				'shared_hashtag'::text AS reason,
				(30 + (COUNT(*) * 5))::bigint AS score
			FROM event_hashtags h1
			JOIN event_hashtags h2 ON h1.hashtag = h2.hashtag AND h2.event_id <> h1.event_id
			WHERE h1.event_id = $1
			GROUP BY h2.event_id
			ORDER BY score DESC, h2.event_id ASC
			LIMIT 60
		),
		author_adj AS (
			SELECT
				nds.event_id,
				'same_author'::text AS reason,
				(20 + LEAST(20, (nds.reply_count + nds.repost_count + nds.reaction_count + nds.zap_count))::bigint)::bigint AS score
			FROM note_discovery_stats nds
			JOIN focal f ON f.pubkey = nds.author_pubkey
			WHERE nds.event_id <> $1
			ORDER BY nds.score_7d DESC, nds.created_at DESC, nds.event_id ASC
			LIMIT 30
		),
		repost_link AS (
			SELECT
				re.event_id,
				'repost_linkage'::text AS reason,
				50::bigint AS score
			FROM repost_events re
			WHERE re.target_event_id = $1
			UNION ALL
			SELECT
				re.target_event_id,
				'repost_linkage'::text AS reason,
				45::bigint AS score
			FROM repost_events re
			WHERE re.event_id = $1
		),
		thread_direct AS (
			SELECT
				te.child_event_id AS event_id,
				'thread_neighborhood'::text AS reason,
				35::bigint AS score
			FROM thread_edges te
			WHERE te.parent_event_id = $1
			ORDER BY te.child_created_at DESC, te.child_event_id DESC
			LIMIT 30
		),
		thread_parent AS (
			SELECT
				te.parent_event_id AS event_id,
				'thread_neighborhood'::text AS reason,
				30::bigint AS score
			FROM thread_edges te
			WHERE te.child_event_id = $1
			LIMIT 1
		),
		thread_root AS (
			SELECT
				te.child_event_id AS event_id,
				'thread_neighborhood'::text AS reason,
				28::bigint AS score
			FROM focal f
			JOIN thread_edges te ON te.root_event_id = f.root_event_id
			WHERE te.child_event_id <> $1
			ORDER BY te.child_created_at DESC, te.child_event_id DESC
			LIMIT 30
		),
		unioned AS (
			SELECT * FROM tag_overlap
			UNION ALL SELECT * FROM author_adj
			UNION ALL SELECT * FROM repost_link
			UNION ALL SELECT * FROM thread_direct
			UNION ALL SELECT * FROM thread_parent
			UNION ALL SELECT * FROM thread_root
		),
		ranked AS (
			SELECT
				u.event_id,
				ARRAY_AGG(DISTINCT u.reason) AS reasons,
				(MAX(u.score) + COUNT(*))::bigint AS rank_score
			FROM unioned u
			WHERE u.event_id <> $1
			GROUP BY u.event_id
			ORDER BY rank_score DESC, u.event_id ASC
			LIMIT $2
		)
		SELECT
			r.event_id,
			e.pubkey,
			e.created_at,
			e.content,
			e.raw_json::text,
			COALESCE(nds.reply_count, 0),
			COALESCE(nds.reaction_count, 0),
			COALESCE(nds.repost_count, 0),
			COALESCE(nds.zap_count, 0),
			COALESCE(nds.zap_msats, 0),
			r.reasons,
			r.rank_score
		FROM ranked r
		JOIN events e ON e.id = r.event_id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = r.event_id
		ORDER BY r.rank_score DESC, e.created_at DESC, r.event_id ASC
	`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("get related notes: %w", err)
	}
	defer rows.Close()

	out := make([]RelatedNote, 0, limit)
	for rows.Next() {
		var row RelatedNote
		var raw string
		if err := rows.Scan(
			&row.EventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&raw,
			&row.ReplyCount,
			&row.ReactionCount,
			&row.RepostCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.Reasons,
			&row.RankScore,
		); err != nil {
			return nil, fmt.Errorf("scan related note row: %w", err)
		}
		row.Event = json.RawMessage(raw)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read related note rows: %w", err)
	}
	return out, nil
}
