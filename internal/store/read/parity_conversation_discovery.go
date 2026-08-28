package read

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// GetHotConversations returns projection-backed active conversation hotspots.
func (s *Read) GetHotConversations(ctx context.Context, window time.Duration, limit int, offset int) ([]HotConversation, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	repliesColumn, weightColumn, windowDuration, err := resolveConversationWindow(window)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	activityCutoff := time.Now().UTC().Add(-windowDuration).Unix()
	// Velocity is scored from reply_weight_* (unique repliers excluding the
	// root author, optionally trust-weighted at projection time) instead of
	// the raw replies_* display counters, so one account replying in a loop
	// buys zero velocity. Rows projected before thread summary derivation v2
	// have no weights yet and fall back to the raw counters until the rebuild
	// sweep reaches them, keeping the surface populated mid-deploy.
	velocityBase := fmt.Sprintf(`
		CASE
			WHEN COALESCE(ts.derivation_version, 0) >= 2 THEN COALESCE(ts.%s, 0)::double precision
			ELSE COALESCE(ts.%s, 0)::double precision
		END`, weightColumn, repliesColumn)
	query := fmt.Sprintf(`
		SELECT
			ts.root_event_id,
			e.pubkey,
			e.created_at,
			e.content,
			COALESCE(ts.reply_count, 0),
			COALESCE(nds.repost_count, 0),
			COALESCE(nds.reaction_count, 0),
			COALESCE(nds.zap_count, 0),
			COALESCE(nds.zap_msats, 0),
			COALESCE(ts.participant_count, 1),
			COALESCE(ts.last_activity_at, e.created_at),
			COALESCE(ts.replies_24h, 0),
			COALESCE(ts.replies_7d, 0),
			(
				%s +
				(LEAST(%s, 50.0) * 0.15)
			) AS velocity_score
		FROM thread_summaries ts
		INNER JOIN events e ON e.id = ts.root_event_id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = ts.root_event_id
		WHERE e.kind = 1
		  AND COALESCE(ts.last_activity_at, e.created_at) >= $1
		  AND %s > 0
		ORDER BY velocity_score DESC, COALESCE(ts.last_activity_at, e.created_at) DESC, ts.root_event_id ASC
		LIMIT $2 OFFSET $3
	`, velocityBase, velocityBase, velocityBase)
	rows, err := s.pool.Query(ctx, query, activityCutoff, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get hot conversations: %w", err)
	}
	defer rows.Close()

	out := make([]HotConversation, 0, limit)
	for rows.Next() {
		var row HotConversation
		row.Consistency = "eventual"
		if err := rows.Scan(
			&row.RootEventID,
			&row.AuthorPubkey,
			&row.CreatedAt,
			&row.Content,
			&row.ReplyCount,
			&row.RepostCount,
			&row.ReactionCount,
			&row.ZapCount,
			&row.ZapMSats,
			&row.ParticipantCount,
			&row.LastActivityAt,
			&row.Replies24h,
			&row.Replies7d,
			&row.VelocityScore,
		); err != nil {
			return nil, fmt.Errorf("scan hot conversation row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read hot conversation rows: %w", err)
	}
	return out, nil
}

func resolveConversationWindow(window time.Duration) (string, string, time.Duration, error) {
	switch window {
	case 24 * time.Hour:
		return "replies_24h", "reply_weight_24h", 24 * time.Hour, nil
	case 7 * 24 * time.Hour:
		return "replies_7d", "reply_weight_7d", 7 * 24 * time.Hour, nil
	default:
		return "", "", 0, fmt.Errorf("unsupported conversation window: %s", strings.TrimSpace(window.String()))
	}
}
