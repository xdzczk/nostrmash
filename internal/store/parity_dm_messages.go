package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (s *PostgresStore) GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	peer = strings.TrimSpace(peer)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if peer == "" {
		rows, err := s.pool.Query(ctx, `
			SELECT e.raw_json::text
			FROM events e
			INNER JOIN event_tags et ON et.event_id = e.id
			WHERE e.kind = 4
			  AND et.tag_name = 'p'
			  AND et.value_index = 0
			  AND (e.pubkey = $1 OR et.value = $1)
		  AND NOT EXISTS (
			SELECT 1
			FROM deletion_events d
			WHERE d.target_event_id = e.id
			  AND d.deleter_pubkey = e.pubkey
		  )
			ORDER BY e.created_at DESC, e.id DESC
			LIMIT $2
		`, pubkey, limit)
		if err != nil {
			return nil, fmt.Errorf("get direct messages: %w", err)
		}
		defer rows.Close()
		out := make([]json.RawMessage, 0, limit)
		for rows.Next() {
			var raw string
			if err := rows.Scan(&raw); err != nil {
				return nil, fmt.Errorf("scan direct message row: %w", err)
			}
			out = append(out, json.RawMessage(raw))
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("read direct message rows: %w", err)
		}
		return out, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		INNER JOIN event_tags et ON et.event_id = e.id
		WHERE e.kind = 4
		  AND et.tag_name = 'p'
		  AND et.value_index = 0
		  AND NOT EXISTS (
			SELECT 1
			FROM deletion_events d
			WHERE d.target_event_id = e.id
			  AND d.deleter_pubkey = e.pubkey
		  )
		  AND (
			(e.pubkey = $1 AND et.value = $2)
			OR
			(e.pubkey = $2 AND et.value = $1)
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $3
	`, pubkey, peer, limit)
	if err != nil {
		return nil, fmt.Errorf("get direct messages for pair: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan direct message pair row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct message pair rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetDirectMessagesWithRange(
	ctx context.Context,
	pubkey string,
	peer string,
	since int64,
	until int64,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	peer = strings.TrimSpace(peer)
	if pubkey == "" || peer == "" {
		return nil, fmt.Errorf("pubkey and peer are required")
	}
	if since < 0 {
		since = 0
	}
	if until <= 0 {
		until = time.Now().Unix()
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT e.raw_json::text
		FROM events e
		INNER JOIN event_tags et ON et.event_id = e.id
		WHERE e.kind = 4
		  AND et.tag_name = 'p'
		  AND et.value_index = 0
		  AND NOT EXISTS (
			SELECT 1
			FROM deletion_events d
			WHERE d.target_event_id = e.id
			  AND d.deleter_pubkey = e.pubkey
		  )
		  AND e.created_at >= $3
		  AND e.created_at <= $4
		  AND (
			(e.pubkey = $1 AND et.value = $2)
			OR
			(e.pubkey = $2 AND et.value = $1)
		  )
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT $5 OFFSET $6
	`, pubkey, peer, since, until, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get direct messages by range: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan direct messages by range row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct messages by range rows: %w", err)
	}
	return out, nil
}
