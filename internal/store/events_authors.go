package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

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
		SELECT e.raw_json::text
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

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan author recent event row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author recent event rows: %w", err)
	}
	return out, nil
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
		SELECT e.raw_json::text
		FROM events e
		WHERE e.pubkey = $1
		  AND EXISTS (
		      SELECT 1
		      FROM event_references er
		      WHERE er.source_event_id = e.id
		        AND er.relation = 'reply'
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get author replies: %w", err)
	}
	defer rows.Close()

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan author reply row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author replies rows: %w", err)
	}
	return out, nil
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
		SELECT e.raw_json::text
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

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan author recent event by kind row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author recent event by kind rows: %w", err)
	}
	return out, nil
}
