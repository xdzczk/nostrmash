package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const authorEventCountsSelect = `
			COALESCE(
				(SELECT reply_count FROM thread_summaries WHERE root_event_id = e.id),
				(SELECT count FROM reply_counts WHERE event_id = e.id),
				0
			),
			COALESCE((SELECT count FROM reaction_counts WHERE event_id = e.id), 0),
			COALESCE((SELECT count FROM repost_counts WHERE event_id = e.id), 0),
			COALESCE((SELECT zap_count FROM note_discovery_stats WHERE event_id = e.id), 0),
			COALESCE((SELECT zap_msats FROM note_discovery_stats WHERE event_id = e.id), 0)
`

// GetAuthorRecentEvents returns projected recent event payloads for one author.
func (s *PostgresStore) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			e.raw_json::text,
			`+authorEventCountsSelect+`
		FROM author_recent_events are
		INNER JOIN events e ON e.id = are.event_id
		WHERE are.author_pubkey = $1
		ORDER BY are.created_at DESC, are.event_id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author recent events: %w", err)
	}
	defer rows.Close()

	return scanAuthorEventsWithCounts(rows)
}

// GetAuthorRecentEventsPage returns one keyset page of projected recent events
// for an author, ordered by created_at desc, event_id desc. A nil cursor
// returns the first page; the returned cursor is nil when no further rows
// exist. No cursor-existence check is performed: retention may prune the
// cursor row between pages, and keyset comparison still resumes correctly.
func (s *PostgresStore) GetAuthorRecentEventsPage(
	ctx context.Context,
	pubkey string,
	limit int,
	cursor *EventOrderCursor,
) ([]json.RawMessage, *EventOrderCursor, error) {
	return s.getAuthorRecentEventsPage(ctx, pubkey, nil, limit, cursor)
}

// GetAuthorRecentEventsByKindPage is GetAuthorRecentEventsPage filtered to one kind.
func (s *PostgresStore) GetAuthorRecentEventsByKindPage(
	ctx context.Context,
	pubkey string,
	kind int,
	limit int,
	cursor *EventOrderCursor,
) ([]json.RawMessage, *EventOrderCursor, error) {
	if kind < 0 {
		return nil, nil, fmt.Errorf("kind must be >= 0")
	}
	return s.getAuthorRecentEventsPage(ctx, pubkey, &kind, limit, cursor)
}

func (s *PostgresStore) getAuthorRecentEventsPage(
	ctx context.Context,
	pubkey string,
	kind *int,
	limit int,
	cursor *EventOrderCursor,
) ([]json.RawMessage, *EventOrderCursor, error) {
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

	query := `
		SELECT
			e.raw_json::text,
			are.created_at,
			are.event_id,
			` + authorEventCountsSelect + `
		FROM author_recent_events are
		INNER JOIN events e ON e.id = are.event_id
		WHERE are.author_pubkey = $1
	`
	args := []any{pubkey}
	if kind != nil {
		args = append(args, *kind)
		query += fmt.Sprintf(" AND e.kind = $%d", len(args))
	}
	if cursor != nil {
		cursorID := strings.TrimSpace(cursor.ID)
		if cursorID == "" {
			return []json.RawMessage{}, nil, nil
		}
		args = append(args, cursor.CreatedAt, cursorID)
		query += fmt.Sprintf(" AND (are.created_at, are.event_id) < ($%d, $%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(`
		ORDER BY are.created_at DESC, are.event_id DESC
		LIMIT $%d
	`, len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("get author recent events page: %w", err)
	}
	defer rows.Close()

	type pageRow struct {
		raw       json.RawMessage
		createdAt int64
		eventID   string
	}
	pageRows := make([]pageRow, 0, limit+1)
	for rows.Next() {
		var raw string
		var createdAt int64
		var eventID string
		var counts EventCounts
		counts.Consistency = "eventual"
		if err := rows.Scan(
			&raw,
			&createdAt,
			&eventID,
			&counts.ReplyCount,
			&counts.ReactionCount,
			&counts.RepostCount,
			&counts.ZapCount,
			&counts.ZapMSats,
		); err != nil {
			return nil, nil, fmt.Errorf("scan author event page row: %w", err)
		}
		enriched, err := mergeEventCountsIntoRaw(json.RawMessage(raw), counts)
		if err != nil {
			return nil, nil, err
		}
		pageRows = append(pageRows, pageRow{raw: enriched, createdAt: createdAt, eventID: eventID})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read author event page rows: %w", err)
	}

	hasMore := len(pageRows) > limit
	if hasMore {
		pageRows = pageRows[:limit]
	}
	out := make([]json.RawMessage, 0, len(pageRows))
	for _, row := range pageRows {
		out = append(out, row.raw)
	}
	var nextCursor *EventOrderCursor
	if hasMore && len(pageRows) > 0 {
		last := pageRows[len(pageRows)-1]
		nextCursor = &EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.eventID,
		}
	}
	return out, nextCursor, nil
}

// GetAuthorReplies returns replies authored by one pubkey sorted by created_at desc, id desc.
func (s *PostgresStore) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
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
		SELECT
			e.raw_json::text,
			`+authorEventCountsSelect+`
		FROM events e
		WHERE e.pubkey = $1
		  AND EXISTS (
		      SELECT 1
		      FROM thread_edges te
		      WHERE te.child_event_id = e.id
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author replies: %w", err)
	}
	defer rows.Close()

	return scanAuthorEventsWithCounts(rows)
}

// GetAuthorRecentEventsByKind returns projected recent events for one author filtered by kind.
func (s *PostgresStore) GetAuthorRecentEventsByKind(
	ctx context.Context,
	pubkey string,
	kind int,
	limit int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if kind < 0 {
		return nil, fmt.Errorf("kind must be >= 0")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			e.raw_json::text,
			`+authorEventCountsSelect+`
		FROM author_recent_events are
		INNER JOIN events e ON e.id = are.event_id
		WHERE are.author_pubkey = $1
		  AND e.kind = $2
		ORDER BY are.created_at DESC, are.event_id DESC
		LIMIT $3
	`, pubkey, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("get author recent events by kind: %w", err)
	}
	defer rows.Close()

	return scanAuthorEventsWithCounts(rows)
}

type authorEventRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanAuthorEventsWithCounts(rows authorEventRows) ([]json.RawMessage, error) {
	out := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw string
		var counts EventCounts
		counts.Consistency = "eventual"
		if err := rows.Scan(
			&raw,
			&counts.ReplyCount,
			&counts.ReactionCount,
			&counts.RepostCount,
			&counts.ZapCount,
			&counts.ZapMSats,
		); err != nil {
			return nil, fmt.Errorf("scan author event row: %w", err)
		}
		enriched, err := mergeEventCountsIntoRaw(json.RawMessage(raw), counts)
		if err != nil {
			return nil, err
		}
		out = append(out, enriched)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author event rows: %w", err)
	}
	return out, nil
}
