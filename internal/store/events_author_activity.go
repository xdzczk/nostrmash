package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const authorReactionsSelect = `
	SELECT json_build_object(
		'event_id', re.event_id,
		'target_event_id', re.target_event_id,
		'reaction', re.content,
		'created_at', re.created_at,
		'event', e.raw_json,
		'target_event', te.raw_json
	)::text,
	re.created_at,
	re.event_id
	FROM reaction_events re
	INNER JOIN events e ON e.id = re.event_id
	LEFT JOIN events te ON te.id = re.target_event_id
`

// GetAuthorReactions returns kind 7 reaction events authored by pubkey.
func (s *PostgresStore) GetAuthorReactions(
	ctx context.Context,
	pubkey string,
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

	type reactionRow struct {
		raw       string
		createdAt int64
		id        string
	}
	rowsOut := make([]reactionRow, 0, limit+1)

	var rows pgx.Rows
	var err error
	if cursor == nil {
		rows, err = s.pool.Query(ctx, authorReactionsSelect+`
			WHERE re.reactor_pubkey = $1
			ORDER BY re.created_at DESC, re.event_id DESC
			LIMIT $2
		`, pubkey, limit+1)
	} else {
		cursorID := strings.TrimSpace(cursor.ID)
		if cursorID == "" {
			return []json.RawMessage{}, nil, nil
		}
		var cursorExists bool
		if err := s.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM reaction_events re
				WHERE re.reactor_pubkey = $1
				  AND re.created_at = $2
				  AND re.event_id = $3
			)
		`, pubkey, cursor.CreatedAt, cursorID).Scan(&cursorExists); err != nil {
			return nil, nil, fmt.Errorf("lookup author reactions cursor existence: %w", err)
		}
		if !cursorExists {
			return []json.RawMessage{}, nil, nil
		}
		rows, err = s.pool.Query(ctx, authorReactionsSelect+`
			WHERE re.reactor_pubkey = $1
			  AND (re.created_at, re.event_id) < ($2, $3)
			ORDER BY re.created_at DESC, re.event_id DESC
			LIMIT $4
		`, pubkey, cursor.CreatedAt, cursorID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get author reactions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row reactionRow
		if err := rows.Scan(&row.raw, &row.createdAt, &row.id); err != nil {
			return nil, nil, fmt.Errorf("scan author reactions row: %w", err)
		}
		rowsOut = append(rowsOut, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read author reactions rows: %w", err)
	}

	hasMore := len(rowsOut) > limit
	if hasMore {
		rowsOut = rowsOut[:limit]
	}
	out := make([]json.RawMessage, 0, len(rowsOut))
	for _, row := range rowsOut {
		out = append(out, json.RawMessage(row.raw))
	}

	var nextCursor *EventOrderCursor
	if hasMore && len(rowsOut) > 0 {
		last := rowsOut[len(rowsOut)-1]
		nextCursor = &EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return out, nextCursor, nil
}
