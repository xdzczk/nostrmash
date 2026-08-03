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
