package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (s *Read) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
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
		SELECT json_build_object(
			'peer_pubkey', e.pubkey,
			'unread_count', count(*)
		)::text
		FROM events e
		INNER JOIN event_tags et
		        ON et.event_id = e.id
		       AND et.tag_name = 'p'
		       AND et.value_index = 0
		LEFT JOIN dm_read_cursors c
		       ON c.user_pubkey = $1
		      AND c.peer_pubkey = e.pubkey
		WHERE e.kind = 4
		  AND et.value = $1
		  AND e.pubkey <> $1
		  AND (
			c.user_pubkey IS NULL
			OR e.created_at > c.last_read_created_at
			OR (
				e.created_at = c.last_read_created_at
				AND e.id > c.last_read_event_id
			)
		  )
		GROUP BY e.pubkey
		ORDER BY count(*) DESC, e.pubkey ASC
		LIMIT $2
	`, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get direct message unread counts: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan direct message unread row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read direct message unread rows: %w", err)
	}
	return out, nil
}

func (s *Read) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	receiver = strings.TrimSpace(receiver)
	sender = strings.TrimSpace(sender)
	if receiver == "" {
		return 0, fmt.Errorf("receiver pubkey is required")
	}
	var count int64
	if sender == "" {
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(cnt, 0)
			FROM dm_unread_counts
			WHERE receiver_pubkey = $1
			  AND sender_pubkey = ''
		`, receiver).Scan(&count)
		if err == nil {
			return count, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("get direct message aggregate count: %w", err)
		}
	}
	if sender != "" {
		err := s.pool.QueryRow(ctx, `
			SELECT COALESCE(cnt, 0)
			FROM dm_unread_counts
			WHERE receiver_pubkey = $1
			  AND sender_pubkey = $2
		`, receiver, sender).Scan(&count)
		if err == nil {
			return count, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("get direct message sender count: %w", err)
		}
	}

	err := s.pool.QueryRow(ctx, `
		SELECT count(*)
		FROM events e
		INNER JOIN event_tags et
		        ON et.event_id = e.id
		       AND et.tag_name = 'p'
		       AND et.value_index = 0
		LEFT JOIN dm_read_cursors c
		       ON c.user_pubkey = $1
		      AND c.peer_pubkey = e.pubkey
		WHERE e.kind = 4
		  AND et.value = $1
		  AND e.pubkey <> $1
		  AND ($2 = '' OR e.pubkey = $2)
		  AND NOT EXISTS (
			SELECT 1
			FROM deletion_events d
			WHERE d.target_event_id = e.id
			  AND d.deleter_pubkey = e.pubkey
		  )
		  AND (
			c.user_pubkey IS NULL
			OR e.created_at > c.last_read_created_at
			OR (
				e.created_at = c.last_read_created_at
				AND e.id > c.last_read_event_id
			)
		  )
	`, receiver, sender).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("fallback direct message count: %w", err)
	}
	return count, nil
}

func (s *Read) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	receiver = strings.TrimSpace(receiver)
	sender = strings.TrimSpace(sender)
	if receiver == "" || sender == "" {
		return fmt.Errorf("receiver and sender are required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var senderCount int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(cnt, 0)
		FROM dm_unread_counts
		WHERE receiver_pubkey = $1
		  AND sender_pubkey = $2
		FOR UPDATE
	`, receiver, sender).Scan(&senderCount); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("load sender dm count: %w", err)
		}
		senderCount = 0
	}
	if senderCount > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE dm_unread_counts
			SET cnt = 0, updated_at = now()
			WHERE receiver_pubkey = $1
			  AND sender_pubkey = $2
		`, receiver, sender); err != nil {
			return fmt.Errorf("reset sender dm count: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			UPDATE dm_unread_counts
			SET cnt = GREATEST(0, cnt - $3), updated_at = now()
			WHERE receiver_pubkey = $1
			  AND sender_pubkey = ''
		`, receiver, sender, senderCount); err != nil {
			return fmt.Errorf("decrement aggregate dm count: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reset dm count tx: %w", err)
	}
	return nil
}

func (s *Read) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	receiver = strings.TrimSpace(receiver)
	if receiver == "" {
		return fmt.Errorf("receiver is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE dm_unread_counts
		SET cnt = 0, updated_at = now()
		WHERE receiver_pubkey = $1
	`, receiver)
	if err != nil {
		return fmt.Errorf("reset all dm counts: %w", err)
	}
	return nil
}

func (s *Read) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	peer = strings.TrimSpace(peer)
	if pubkey == "" || peer == "" {
		return fmt.Errorf("pubkey and peer are required")
	}
	var lastID string
	var lastCreated int64
	err := s.pool.QueryRow(ctx, `
		SELECT e.id, e.created_at
		FROM events e
		INNER JOIN event_tags et
		        ON et.event_id = e.id
		       AND et.tag_name = 'p'
		       AND et.value_index = 0
		WHERE e.kind = 4
		  AND e.pubkey = $1
		  AND et.value = $2
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT 1
	`, peer, pubkey).Scan(&lastID, &lastCreated)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			lastID = ""
			lastCreated = 0
		} else {
			return fmt.Errorf("load latest incoming dm: %w", err)
		}
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO dm_read_cursors (
			user_pubkey, peer_pubkey, last_read_created_at, last_read_event_id
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_pubkey, peer_pubkey) DO UPDATE
		SET last_read_created_at = EXCLUDED.last_read_created_at,
		    last_read_event_id = EXCLUDED.last_read_event_id,
		    updated_at = now()
	`, pubkey, peer, lastCreated, lastID)
	if err != nil {
		return fmt.Errorf("upsert dm read cursor: %w", err)
	}
	return nil
}
