package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// GetFollowersByPubkey returns follower edges derived from latest kind:3 contact lists.
func (s *Read) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
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
		SELECT json_build_object(
			'follower_pubkey', follower_pubkey,
			'source_event_id', source_event_id,
			'contact_list_created_at', contact_list_created_at
		)::text
		FROM follower_edges
		WHERE followed_pubkey = $1
		ORDER BY contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC
		LIMIT $2
	`, targetPubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get followers by pubkey: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan followers row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read followers rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM event_tags et
		INNER JOIN events e ON e.id = et.event_id
		WHERE et.tag_name = 'e'
		  AND et.value_index = 0
		  AND et.value = $1
		  AND e.kind = 9802
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("get highlights by event id: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan highlights by event id row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read highlights by event id rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	identifier = strings.TrimSpace(identifier)
	if kind <= 0 {
		return nil, fmt.Errorf("kind is required")
	}
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	target := fmt.Sprintf("%d:%s:%s", kind, pubkey, identifier)
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM event_tags et
		INNER JOIN events e ON e.id = et.event_id
		WHERE et.tag_name = 'a'
		  AND et.value_index = 0
		  AND et.value = $1
		  AND e.kind = 9802
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $2
	`, target, limit)
	if err != nil {
		return nil, fmt.Errorf("get highlights by a target: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan highlights by a target row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read highlights by a target rows: %w", err)
	}
	return out, nil
}
