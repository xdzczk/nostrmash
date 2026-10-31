package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// ListEventActions returns one keyset page of the action events that target
// eventID. kind 6 is reposts, kind 7 is reactions, and kind 9735 is zap
// receipts. The rows are the action events themselves (id, pubkey, kind,
// created_at, tags, content), newest first. A missing target is ErrNotFound.
// Any other kind is ErrUnsupportedEventActionKind so callers do not mistake
// an empty page for a successful read of that kind.
func (s *Read) ListEventActions(
	ctx context.Context,
	eventID string,
	kind int,
	limit int,
	cursor *EventOrderCursor,
) ([]json.RawMessage, *EventOrderCursor, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil, fmt.Errorf("event id is required")
	}
	if kind != 6 && kind != 7 && kind != 9735 {
		return nil, nil, readmodel.ErrUnsupportedEventActionKind
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	exists, err := s.eventExists(ctx, eventID)
	if err != nil {
		return nil, nil, err
	}
	if !exists {
		return nil, nil, ErrNotFound
	}

	query, args := eventActionPageSQL(eventID, kind, limit, cursor)
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("list event actions: %w", err)
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
		var id string
		if err := rows.Scan(&raw, &createdAt, &id); err != nil {
			return nil, nil, fmt.Errorf("scan event action: %w", err)
		}
		pageRows = append(pageRows, pageRow{
			raw:       json.RawMessage(raw),
			createdAt: createdAt,
			eventID:   id,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read event actions: %w", err)
	}
	hasMore := len(pageRows) > limit
	if hasMore {
		pageRows = pageRows[:limit]
	}
	out := make([]json.RawMessage, 0, len(pageRows))
	for _, row := range pageRows {
		out = append(out, row.raw)
	}
	if !hasMore || len(pageRows) == 0 {
		return out, nil, nil
	}
	last := pageRows[len(pageRows)-1]
	return out, &EventOrderCursor{CreatedAt: last.createdAt, ID: last.eventID}, nil
}

func eventActionPageSQL(eventID string, kind int, limit int, cursor *EventOrderCursor) (string, []any) {
	// Projection rows point at the stored action event. Ordering matches the
	// target indexes: created_at DESC, action id DESC.
	var query string
	switch kind {
	case 7:
		query = `
			SELECT e.raw_json::text, r.created_at, r.event_id
			FROM reaction_events r
			INNER JOIN events e ON e.id = r.event_id
			WHERE r.target_event_id = $1`
	case 6:
		query = `
			SELECT e.raw_json::text, r.created_at, r.event_id
			FROM repost_events r
			INNER JOIN events e ON e.id = r.event_id
			WHERE r.target_event_id = $1`
	default:
		query = `
			SELECT e.raw_json::text, z.created_at, z.zap_receipt_id
			FROM zap_receipts z
			INNER JOIN events e ON e.id = z.zap_receipt_id
			WHERE z.event_id = $1`
	}
	args := []any{eventID}
	if cursor != nil && strings.TrimSpace(cursor.ID) != "" {
		args = append(args, cursor.CreatedAt, strings.TrimSpace(cursor.ID))
		if kind == 9735 {
			query += ` AND (z.created_at, z.zap_receipt_id) < ($2, $3)`
		} else {
			query += ` AND (r.created_at, r.event_id) < ($2, $3)`
		}
	}
	args = append(args, limit+1)
	if kind == 9735 {
		query += fmt.Sprintf(` ORDER BY z.created_at DESC, z.zap_receipt_id DESC LIMIT $%d`, len(args))
	} else {
		query += fmt.Sprintf(` ORDER BY r.created_at DESC, r.event_id DESC LIMIT $%d`, len(args))
	}
	return query, args
}
