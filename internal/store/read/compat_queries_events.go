package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

func (s *Read) GetRecentEventsByKindAndPubkey(
	ctx context.Context,
	kind int,
	pubkey string,
	limit int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if kind < 0 {
		return nil, fmt.Errorf("kind must be >= 0")
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
		SELECT raw_json::text
		FROM events
		WHERE kind = $1 AND pubkey = $2
		ORDER BY created_at DESC, id DESC
		LIMIT $3
	`, kind, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get recent events by kind and pubkey: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan recent events by kind row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recent events by kind rows: %w", err)
	}
	return s.EnrichEventsWithCounts(ctx, out)
}

// GetRecentEventsByKindAndPubkeyPage is the keyset form of
// GetRecentEventsByKindAndPubkey. Order is created_at DESC, id DESC. A nil
// cursor is the first page. The extra row is consumed here.
func (s *Read) GetRecentEventsByKindAndPubkeyPage(
	ctx context.Context,
	kind int,
	pubkey string,
	limit int,
	cursor *EventOrderCursor,
) ([]json.RawMessage, *EventOrderCursor, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	if kind < 0 {
		return nil, nil, fmt.Errorf("kind must be >= 0")
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
		SELECT raw_json::text, created_at, id
		FROM events
		WHERE pubkey = $1
		  AND kind = $2`
	args := []any{pubkey, kind}
	if cursor != nil {
		cursorID := strings.TrimSpace(cursor.ID)
		if cursorID == "" {
			return []json.RawMessage{}, nil, nil
		}
		args = append(args, cursor.CreatedAt, cursorID)
		query += ` AND (created_at, id) < ($3, $4)`
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(`
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("get recent events by kind page: %w", err)
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
		var row pageRow
		if err := rows.Scan(&raw, &row.createdAt, &row.eventID); err != nil {
			return nil, nil, fmt.Errorf("scan recent events by kind page: %w", err)
		}
		row.raw = json.RawMessage(raw)
		pageRows = append(pageRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read recent events by kind page: %w", err)
	}
	hasMore := len(pageRows) > limit
	if hasMore {
		pageRows = pageRows[:limit]
	}
	out := make([]json.RawMessage, 0, len(pageRows))
	for _, row := range pageRows {
		out = append(out, row.raw)
	}
	enriched, err := s.EnrichEventsWithCounts(ctx, out)
	if err != nil {
		return nil, nil, err
	}
	if !hasMore || len(pageRows) == 0 {
		return enriched, nil, nil
	}
	last := pageRows[len(pageRows)-1]
	return enriched, &EventOrderCursor{CreatedAt: last.createdAt, ID: last.eventID}, nil
}

// GetEventsReferencingPubkey returns events that mention target pubkey in p-tags.
// Served directly from canonical event_tags (idx_event_tags_p_lookup) since the
// derived pubkey_references table was dropped in migration 000053. EXISTS keeps
// events with multiple p-tags to the same target from appearing twice.
//
// Kind 3 contact lists are excluded: a follow is not a mention, and kind-3
// p-tags are no longer persisted into event_tags (see internal/eventtags).
// Follower relationships live in follower_edges.
func (s *Read) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	targetPubkey = strings.TrimSpace(targetPubkey)
	if targetPubkey == "" {
		return nil, fmt.Errorf("target pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		WHERE e.kind <> 3
		  AND EXISTS (
			SELECT 1
			FROM event_tags t
			WHERE t.tag_name = 'p'
			  AND t.value_index = 0
			  AND t.value = $1
			  AND t.event_id = e.id
		)
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, targetPubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get events referencing pubkey: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan events referencing pubkey row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events referencing pubkey rows: %w", err)
	}
	return s.EnrichEventsWithCounts(ctx, out)
}

func (s *Read) GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if kind <= 0 {
		return nil, fmt.Errorf("kind must be positive")
	}
	aTagValue = strings.TrimSpace(aTagValue)
	if aTagValue == "" {
		return nil, fmt.Errorf("a tag value is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 5000 {
		limit = 5000
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM event_tags et
		INNER JOIN events e ON e.id = et.event_id
		WHERE et.tag_name = 'a'
		  AND et.value_index = 0
		  AND et.value = $1
		  AND e.kind = $2
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $3
	`, aTagValue, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("get events by a tag and kind: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan events by a tag and kind row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events by a tag and kind rows: %w", err)
	}
	return out, nil
}
