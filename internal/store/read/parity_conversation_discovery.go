package read

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	conversationReplyBoost           = 0.15
	conversationBoostCap             = 50.0
	conversationSmallGroupMaxAuthors = 5
	conversationRepliesPerAuthor     = 8
	// conversationWindowAuthorVersion matches derivation.ThreadSummaryVersion
	// once authors_24h/authors_7d are written. Older stamps keep ranking from
	// lifetime participant_count.
	conversationWindowAuthorVersion = 4
)

// conversationVelocity is the hot-conversation rank for one window.
// Groups of one author score zero. Groups of two to five score the number of
// other authors, scaled down once replies exceed about eight per author.
// Larger groups keep broadVelocity, the reply-weight score.
func conversationVelocity(authors int, replies int64, broadVelocity float64) float64 {
	if authors <= 1 || replies <= 0 {
		return 0
	}
	if authors <= conversationSmallGroupMaxAuthors {
		allowance := float64(conversationRepliesPerAuthor * authors)
		factor := 1.0
		if float64(replies) > allowance {
			factor = allowance / float64(replies)
		}
		return float64(authors-1) * factor
	}
	return broadVelocity
}

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
	// Broad threads score reply_weight_* (unique repliers excluding the root
	// author, optionally trust-weighted, with repeated greetings discounted).
	// Rows projected before thread summary derivation v2 have no weights yet
	// and fall back to the raw counters until the rebuild sweep reaches them.
	//
	// Small threads never use that raw volume. One author scores zero. Two to
	// five authors score (authors - 1), and each author can carry about eight
	// replies before more messages stop raising the rank. Author headcount is
	// the windowed authors_* column once a row is on derivation v4; older rows
	// use lifetime participant_count so a three-person pile-on drops on the
	// next read, before the rebuild.
	authorsColumn := "authors_24h"
	if windowDuration == 7*24*time.Hour {
		authorsColumn = "authors_7d"
	}
	velocityBase := fmt.Sprintf(`
		CASE
			WHEN COALESCE(ts.derivation_version, 0) >= 2 THEN COALESCE(ts.%s, 0)::double precision
			ELSE COALESCE(ts.%s, 0)::double precision
		END`, weightColumn, repliesColumn)
	velocityCore := fmt.Sprintf(`(%s + (LEAST(%s, %.1f) * %.2f))`, velocityBase, velocityBase, conversationBoostCap, conversationReplyBoost)
	authorsExpr := fmt.Sprintf(`
		CASE
			WHEN COALESCE(ts.derivation_version, 0) >= %d THEN COALESCE(ts.%s, 0)
			ELSE COALESCE(ts.participant_count, 1)
		END`, conversationWindowAuthorVersion, authorsColumn)
	repliesExpr := fmt.Sprintf(`COALESCE(ts.%s, 0)`, repliesColumn)
	velocityScore := fmt.Sprintf(`
		CASE
			WHEN (%[1]s) <= 1 THEN 0.0
			WHEN (%[1]s) <= %[2]d THEN
				CASE
					WHEN (%[3]s) <= 0 THEN 0.0
					ELSE ((%[1]s) - 1)::double precision * LEAST(
						1.0,
						(%[4]g * (%[1]s))::double precision / GREATEST((%[3]s), 1)::double precision
					)
				END
			ELSE %[5]s
		END`,
		authorsExpr,
		conversationSmallGroupMaxAuthors,
		repliesExpr,
		float64(conversationRepliesPerAuthor),
		velocityCore,
	)
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
			%s AS velocity_score
		FROM thread_summaries ts
		INNER JOIN events e ON e.id = ts.root_event_id
		LEFT JOIN note_discovery_stats nds ON nds.event_id = ts.root_event_id
		WHERE e.kind = 1
		  AND COALESCE(ts.last_activity_at, e.created_at) >= $1
		  AND %s > 0
		ORDER BY velocity_score DESC, COALESCE(ts.last_activity_at, e.created_at) DESC, ts.root_event_id ASC
		LIMIT $2 OFFSET $3
	`, velocityScore, velocityScore)
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
