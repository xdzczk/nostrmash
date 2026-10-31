package read

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// ListEngagementNotifications returns one keyset page of engagement for pubkey:
// likes, reposts, quote reposts, zaps, replies, mentions, and new follows.
// event_id is the note a client should open: the viewer's note for likes,
// plain reposts, and zaps; the incoming event for replies, quotes, and mentions.
// Follows have an empty event id. Order is created_at DESC, type DESC,
// actor pubkey DESC, event id DESC.
func (s *Read) ListEngagementNotifications(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *readmodel.NotificationCursor,
) ([]readmodel.EngagementNotification, *readmodel.NotificationCursor, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	hasCursor := cursor != nil && strings.TrimSpace(cursor.Type) != "" && strings.TrimSpace(cursor.ActorPubkey) != ""
	createdAt := int64(0)
	cursorType := ""
	actor := ""
	eventID := ""
	if hasCursor {
		createdAt = cursor.CreatedAt
		cursorType = cursor.Type
		actor = cursor.ActorPubkey
		eventID = cursor.EventID
	}
	rows, err := s.pool.Query(ctx, engagementNotificationPageSQL(), pubkey, hasCursor, createdAt, cursorType, actor, eventID, limit+1)
	if err != nil {
		return nil, nil, fmt.Errorf("list engagement notifications: %w", err)
	}
	defer rows.Close()
	out := make([]readmodel.EngagementNotification, 0, limit+1)
	for rows.Next() {
		var row readmodel.EngagementNotification
		if err := rows.Scan(&row.Type, &row.CreatedAt, &row.ActorPubkey, &row.EventID); err != nil {
			return nil, nil, fmt.Errorf("scan engagement notification: %w", err)
		}
		row.Type = strings.TrimSpace(row.Type)
		row.ActorPubkey = strings.TrimSpace(row.ActorPubkey)
		row.EventID = strings.TrimSpace(row.EventID)
		if row.Type == "" || row.ActorPubkey == "" {
			continue
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read engagement notifications: %w", err)
	}
	hasMore := len(out) > limit
	if hasMore {
		out = out[:limit]
	}
	if !hasMore || len(out) == 0 {
		return out, nil, nil
	}
	last := out[len(out)-1]
	return out, &readmodel.NotificationCursor{
		CreatedAt:   last.CreatedAt,
		Type:        last.Type,
		ActorPubkey: last.ActorPubkey,
		EventID:     last.EventID,
	}, nil
}

func engagementNotificationPageSQL() string {
	// Each branch keeps the newest limit+1 rows that sort after the cursor.
	// The merged top is the same page a single sort would return, and every
	// branch uses an index that starts at the recipient.
	branches := []string{
		notificationBranch(
			"'like'::text",
			"r.created_at",
			"r.reactor_pubkey",
			"r.target_event_id",
			`FROM reaction_events r
			 WHERE r.target_pubkey = $1
			   AND r.reactor_pubkey <> $1`,
			"r.created_at DESC, r.reactor_pubkey DESC, r.target_event_id DESC",
		),
		notificationBranch(
			"'repost'::text",
			"r.created_at",
			"r.reposter_pubkey",
			"r.target_event_id",
			`FROM repost_events r
			 WHERE r.target_pubkey = $1
			   AND r.reposter_pubkey <> $1
			   AND NULLIF(BTRIM(COALESCE(r.quote, '')), '') IS NULL`,
			"r.created_at DESC, r.reposter_pubkey DESC, r.target_event_id DESC",
		),
		// Kind 6 quotes live on repost_events.quote. event_id is the quote
		// event so the client can open the quote rather than the quoted note.
		notificationBranch(
			"'quote'::text",
			"r.created_at",
			"r.reposter_pubkey",
			"r.event_id",
			`FROM repost_events r
			 WHERE r.target_pubkey = $1
			   AND r.reposter_pubkey <> $1
			   AND NULLIF(BTRIM(COALESCE(r.quote, '')), '') IS NOT NULL`,
			"r.created_at DESC, r.reposter_pubkey DESC, r.event_id DESC",
		),
		notificationBranch(
			"'zap'::text",
			"z.created_at",
			"z.sender_pubkey",
			"z.event_id",
			`FROM zap_receipts z
			 WHERE z.receiver_pubkey = $1
			   AND z.sender_pubkey <> $1
			   AND z.event_id IS NOT NULL`,
			"z.created_at DESC, z.sender_pubkey DESC, z.event_id DESC",
		),
		notificationBranch(
			"'reply'::text",
			"c.source_created_at",
			"c.source_pubkey",
			"c.source_event_id",
			`FROM reply_count_contributions c
			 WHERE c.target_pubkey = $1
			   AND c.source_pubkey IS NOT NULL
			   AND c.source_pubkey <> $1
			   AND c.source_created_at IS NOT NULL`,
			"c.source_created_at DESC, c.source_pubkey DESC, c.source_event_id DESC",
		),
		notificationBranch(
			"'follow'::text",
			"g.gained_at",
			"g.follower_pubkey",
			"''::text",
			`FROM follower_gain_events g
			 WHERE g.followed_pubkey = $1`,
			"g.gained_at DESC, g.follower_pubkey DESC",
		),
		// p-tag mentions that are not already a reply to this pubkey. A kind-1
		// #p filter alone would also return those replies.
		notificationBranch(
			"'mention'::text",
			"e.created_at",
			"e.pubkey",
			"e.id",
			`FROM event_tags t
			 INNER JOIN events e ON e.id = t.event_id
			 WHERE t.tag_name = 'p'
			   AND t.value_index = 0
			   AND t.value = $1
			   AND e.pubkey <> $1
			   AND e.kind NOT IN (3, 6, 7, 16, 9735)
			   AND NOT EXISTS (
			     SELECT 1
			     FROM reply_count_contributions c
			     WHERE c.source_event_id = e.id
			       AND c.target_pubkey = $1
			   )
			   AND NOT EXISTS (
			     SELECT 1
			     FROM event_references er
			     INNER JOIN author_recent_events are
			             ON are.event_id = er.referenced_event_id
			            AND are.author_pubkey = $1
			     WHERE er.source_event_id = e.id
			       AND er.relation = 'mention'
			   )`,
			"e.created_at DESC, e.pubkey DESC, e.id DESC",
		),
		// Kind 1 quotes of this pubkey's retained notes. event_references
		// stores the mention marker that event_tags no longer keeps.
		notificationBranch(
			"'quote'::text",
			"e.created_at",
			"e.pubkey",
			"e.id",
			`FROM author_recent_events are
			 INNER JOIN event_references er
			         ON er.referenced_event_id = are.event_id
			        AND er.relation = 'mention'
			 INNER JOIN events e ON e.id = er.source_event_id
			 WHERE are.author_pubkey = $1
			   AND e.pubkey <> $1
			   AND NOT EXISTS (
			     SELECT 1 FROM repost_events r WHERE r.event_id = e.id
			   )`,
			"e.created_at DESC, e.pubkey DESC, e.id DESC",
		),
		// Kind 16 generic reposts are projected onto primal_notifications and
		// are not rows in repost_events. The dedupe key ends with the source id.
		notificationBranch(
			"'quote'::text",
			"n.created_at",
			"COALESCE(NULLIF(n.args->>'who_reposted_it', ''), src.pubkey)",
			"src.id",
			`FROM primal_notifications n
			 INNER JOIN events src ON src.id = split_part(n.dedupe_key, ':', 3)
			 WHERE n.pubkey = $1
			   AND n.type = 5
			   AND src.pubkey <> $1
			   AND NOT EXISTS (
			     SELECT 1 FROM repost_events r WHERE r.event_id = src.id
			   )`,
			"n.created_at DESC, COALESCE(NULLIF(n.args->>'who_reposted_it', ''), src.pubkey) DESC, src.id DESC",
		),
	}
	return `
		SELECT DISTINCT type, created_at, actor_pubkey, event_id
		FROM (
			` + strings.Join(branches, "\n\t\t\tUNION ALL\n\t\t\t") + `
		) page
		WHERE actor_pubkey <> ''
		ORDER BY created_at DESC, type DESC, actor_pubkey DESC, event_id DESC
		LIMIT $7`
}

func notificationBranch(typeExpr, createdExpr, actorExpr, eventExpr, fromWhere, orderBy string) string {
	return fmt.Sprintf(`(
			SELECT %s AS type, %s AS created_at, %s AS actor_pubkey, %s AS event_id
			%s
			  AND %s
			ORDER BY %s
			LIMIT $7
		)`, typeExpr, createdExpr, actorExpr, eventExpr, fromWhere, notificationCursorPredicate(createdExpr, typeExpr, actorExpr, eventExpr), orderBy)
}

func notificationCursorPredicate(createdExpr, typeExpr, actorExpr, eventExpr string) string {
	return fmt.Sprintf(`(
			NOT $2::bool
			OR %s < $3::bigint
			OR (%s = $3::bigint AND %s < $4::text)
			OR (%s = $3::bigint AND %s = $4::text AND %s < $5::text)
			OR (%s = $3::bigint AND %s = $4::text AND %s = $5::text AND %s < $6::text)
		)`,
		createdExpr,
		createdExpr, typeExpr,
		createdExpr, typeExpr, actorExpr,
		createdExpr, typeExpr, actorExpr, eventExpr,
	)
}
