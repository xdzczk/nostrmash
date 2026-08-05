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
