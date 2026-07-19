package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (s *Read) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
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
	if limit > 200 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		WITH peers AS (
			SELECT et.value AS peer_pubkey, max(e.created_at) AS recent_ts
			FROM events e
			INNER JOIN event_tags et
			        ON et.event_id = e.id
			WHERE e.kind = 4
			  AND e.pubkey = $1
			  AND et.tag_name = 'p'
			  AND et.value_index = 0
			GROUP BY et.value

			UNION ALL

			SELECT e.pubkey AS peer_pubkey, max(e.created_at) AS recent_ts
			FROM events e
			INNER JOIN event_tags et
			        ON et.event_id = e.id
			WHERE e.kind = 4
			  AND et.tag_name = 'p'
			  AND et.value_index = 0
			  AND et.value = $1
			  AND e.pubkey <> $1
			GROUP BY e.pubkey
		)
		SELECT peer_pubkey
		FROM peers
		GROUP BY peer_pubkey
		ORDER BY max(recent_ts) DESC, peer_pubkey ASC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get direct message contacts: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var peer string
		if err := rows.Scan(&peer); err != nil {
			return nil, fmt.Errorf("scan direct message contact row: %w", err)
		}
		out = append(out, peer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct message contact rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetDirectMessageContactsDetailed(
	ctx context.Context,
	receiver string,
	limit int,
	offset int,
	since int64,
	until int64,
) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	receiver = strings.TrimSpace(receiver)
	if receiver == "" {
		return nil, fmt.Errorf("receiver pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}
	if until <= 0 {
		until = time.Now().Unix()
	}
	rows, err := s.pool.Query(ctx, `
		WITH peers AS (
			SELECT sender_pubkey
			FROM dm_unread_counts
			WHERE receiver_pubkey = $1
			  AND sender_pubkey <> ''
			UNION
			SELECT DISTINCT e.pubkey AS sender_pubkey
			FROM events e
			INNER JOIN event_tags et
			        ON et.event_id = e.id
			       AND et.tag_name = 'p'
			       AND et.value_index = 0
			WHERE e.kind = 4
			  AND et.value = $1
			  AND e.pubkey <> $1
			  AND NOT EXISTS (
				SELECT 1
				FROM deletion_events d
				WHERE d.target_event_id = e.id
				  AND d.deleter_pubkey = e.pubkey
			  )
		)
		SELECT json_build_object(
			'peer_pubkey', p.sender_pubkey,
			'cnt', COALESCE(c.cnt, 0),
			'latest_at', COALESCE(c.latest_at, 0),
			'latest_event_id', COALESCE(c.latest_event_id, '')
		)::text
		FROM peers p
		LEFT JOIN dm_unread_counts c
		       ON c.receiver_pubkey = $1
		      AND c.sender_pubkey = p.sender_pubkey
		WHERE COALESCE(c.latest_at, 0) >= $2
		  AND COALESCE(c.latest_at, 0) <= $3
		ORDER BY COALESCE(c.latest_at, 0) DESC, p.sender_pubkey ASC
		LIMIT $4 OFFSET $5
	`, receiver, since, until, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("get direct message contacts detailed: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan direct message contacts detailed row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct message contacts detailed rows: %w", err)
	}
	return out, nil
}
