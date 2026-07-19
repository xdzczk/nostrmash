package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *Read) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if kind <= 0 {
		return nil, fmt.Errorf("kind must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		INNER JOIN events e ON e.id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		ORDER BY rs.created_at DESC, rs.event_id DESC
		LIMIT $3
	`, pubkey, kind, limit)
	if err != nil {
		return nil, fmt.Errorf("get parameterized replaceable list: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan parameterized replaceable row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read parameterized replaceable rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetParameterizedReplaceableEvent(
	ctx context.Context,
	pubkey string,
	kind int,
	dTag string,
) (json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	dTag = strings.TrimSpace(dTag)
	if pubkey == "" || kind <= 0 {
		return nil, fmt.Errorf("pubkey and kind are required")
	}
	var raw string
	err := s.pool.QueryRow(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		INNER JOIN events e ON e.id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		  AND rs.d_tag = $3
		LIMIT 1
	`, pubkey, kind, dTag).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get parameterized replaceable event: %w", err)
	}
	return json.RawMessage(raw), nil
}

func (s *Read) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	dTag = strings.TrimSpace(dTag)
	if kind <= 0 {
		return nil, fmt.Errorf("kind must be positive")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		INNER JOIN events e ON e.id = rs.event_id
		WHERE rs.kind = $1
		  AND rs.d_tag = $2
		ORDER BY rs.created_at DESC, rs.event_id DESC
		LIMIT $3
	`, kind, dTag, limit)
	if err != nil {
		return nil, fmt.Errorf("get parameterized replaceable events: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan parameterized replaceable events row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read parameterized replaceable events rows: %w", err)
	}
	return out, nil
}
func (s *Read) GetParameterizedReplaceableListByIdentifier(
	ctx context.Context,
	pubkey string,
	kind int,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	identifier = strings.TrimSpace(identifier)
	if pubkey == "" || identifier == "" || kind <= 0 {
		return nil, fmt.Errorf("pubkey, kind and identifier are required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		INNER JOIN events e ON e.id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		  AND rs.d_tag = $3
		ORDER BY rs.created_at DESC, rs.event_id DESC
		LIMIT $4
	`, pubkey, kind, identifier, limit)
	if err != nil {
		return nil, fmt.Errorf("get parameterized replaceable list by identifier: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan parameterized replaceable by identifier row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read parameterized replaceable by identifier rows: %w", err)
	}
	return out, nil
}
