package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

type NetworkStats struct {
	Events   int64 `json:"events"`
	Profiles int64 `json:"profiles"`
	Relays   int64 `json:"relays"`
}

type CuratedRecommendedRead struct {
	EventID string `json:"event_id"`
	Title   string `json:"title"`
	URL     string `json:"url"`
	Rank    int    `json:"rank"`
}

type CuratedReadsTopic struct {
	Topic string `json:"topic"`
	Rank  int    `json:"rank"`
}

type CuratedFeaturedAuthor struct {
	Pubkey string `json:"pubkey"`
	Rank   int    `json:"rank"`
}

func (s *PostgresStore) GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error) {
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
	orderBy := "zr.created_at DESC, zr.zap_receipt_id DESC"
	if sortBySats {
		orderBy = "zr.amount_sats DESC, zr.created_at DESC, zr.zap_receipt_id DESC"
	}
	q := `
		SELECT json_build_object(
			'event_id', zr.zap_receipt_id,
			'sender_pubkey', zr.sender_pubkey,
			'receiver_pubkey', zr.receiver_pubkey,
			'target_event_id', zr.event_id,
			'sats', zr.amount_sats,
			'created_at', zr.created_at,
			'event', e.raw_json
		)::text
		FROM zap_receipts zr
		INNER JOIN events e ON e.id = zr.zap_receipt_id
		WHERE zr.receiver_pubkey = $1
		ORDER BY ` + orderBy + `
		LIMIT $2
	`
	rows, err := s.pool.Query(ctx, q, pubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get user zaps: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan user zaps row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read user zaps rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
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
		SELECT json_build_object(
			'event_id', zr.zap_receipt_id,
			'sender_pubkey', zr.sender_pubkey,
			'target_event_id', zr.event_id,
			'sats', zr.amount_sats,
			'created_at', zr.created_at,
			'event', e.raw_json
		)::text
		FROM zap_receipts zr
		INNER JOIN events e ON e.id = zr.zap_receipt_id
		WHERE zr.event_id = $1
		ORDER BY zr.amount_sats DESC, zr.created_at DESC, zr.zap_receipt_id DESC
		LIMIT $2
	`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("get event zaps: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan event zaps row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event zaps rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("store is not initialized")
	}
	followerPubkey = strings.TrimSpace(followerPubkey)
	followedPubkey = strings.TrimSpace(followedPubkey)
	if followerPubkey == "" || followedPubkey == "" {
		return false, fmt.Errorf("follower and followed pubkeys are required")
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM follower_edges
			WHERE follower_pubkey = $1 AND followed_pubkey = $2
		)
	`, followerPubkey, followedPubkey).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("is user following: %w", err)
	}
	return exists, nil
}

func (s *PostgresStore) GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	leftPubkey = strings.TrimSpace(leftPubkey)
	rightPubkey = strings.TrimSpace(rightPubkey)
	if leftPubkey == "" || rightPubkey == "" {
		return nil, fmt.Errorf("both pubkeys are required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT fe1.followed_pubkey
		FROM follower_edges fe1
		INNER JOIN follower_edges fe2
		        ON fe2.followed_pubkey = fe1.followed_pubkey
		WHERE fe1.follower_pubkey = $1
		  AND fe2.follower_pubkey = $2
		ORDER BY fe1.followed_pubkey ASC
		LIMIT $3
	`, leftPubkey, rightPubkey, limit)
	if err != nil {
		return nil, fmt.Errorf("get mutual follows: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan mutual follow row: %w", err)
		}
		out = append(out, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mutual follows rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
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

func (s *PostgresStore) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
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

func (s *PostgresStore) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
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

func (s *PostgresStore) GetDirectMessageContactsDetailed(
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

func (s *PostgresStore) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
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

func (s *PostgresStore) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
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

func (s *PostgresStore) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
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

func (s *PostgresStore) GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT et.value
		FROM replaceable_state rs
		INNER JOIN event_tags et
		        ON et.event_id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		  AND rs.d_tag = ''
		  AND et.value_index = 0
		  AND et.tag_name IN ('p', 'e', 't', 'word')
		ORDER BY et.tag_name ASC, et.value ASC
	`, pubkey, kind)
	if err != nil {
		return nil, fmt.Errorf("get moderation list: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan moderation list row: %w", err)
		}
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read moderation list rows: %w", err)
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

func (s *PostgresStore) IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error) {
	viewerPubkey = strings.TrimSpace(viewerPubkey)
	eventID = strings.TrimSpace(eventID)
	if viewerPubkey == "" || eventID == "" {
		return false, "", fmt.Errorf("viewer pubkey and event id are required")
	}
	var content string
	var authorPubkey string
	err := s.pool.QueryRow(ctx, `SELECT content, pubkey FROM events WHERE id = $1`, eventID).Scan(&content, &authorPubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, "", ErrNotFound
		}
		return false, "", fmt.Errorf("load moderation target event: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT et.tag_name, et.value
		FROM replaceable_state rs
		INNER JOIN event_tags et
		        ON et.event_id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = 10000
		  AND rs.d_tag = ''
		  AND et.value_index = 0
		  AND et.tag_name IN ('p', 'e', 't', 'word')
	`, viewerPubkey)
	if err != nil {
		return false, "", fmt.Errorf("load moderation entries: %w", err)
	}
	defer rows.Close()

	type moderationEntry struct {
		tagName string
		value   string
	}
	entries := make([]moderationEntry, 0)
	for rows.Next() {
		var tagName string
		var value string
		if err := rows.Scan(&tagName, &value); err != nil {
			return false, "", fmt.Errorf("scan moderation entry row: %w", err)
		}
		tagName = strings.ToLower(strings.TrimSpace(tagName))
		value = strings.TrimSpace(value)
		if tagName == "" || value == "" {
			continue
		}
		entries = append(entries, moderationEntry{tagName: tagName, value: value})
	}
	if err := rows.Err(); err != nil {
		return false, "", fmt.Errorf("read moderation entry rows: %w", err)
	}
	lowerContent := strings.ToLower(content)
	for _, entry := range entries {
		switch entry.tagName {
		case "p":
			if strings.EqualFold(authorPubkey, entry.value) {
				return true, "muted_pubkey:" + entry.value, nil
			}
		case "e":
			if strings.EqualFold(eventID, entry.value) {
				return true, "muted_event:" + entry.value, nil
			}
		case "word", "t":
			needle := strings.ToLower(strings.TrimSpace(entry.value))
			if needle == "" {
				continue
			}
			if strings.Contains(lowerContent, needle) {
				return true, "muted_term:" + entry.value, nil
			}
		default:
			continue
		}
	}
	return false, "", nil
}

func (s *PostgresStore) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
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

func (s *PostgresStore) GetParameterizedReplaceableEvent(
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

func (s *PostgresStore) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
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

func (s *PostgresStore) GetNetworkStats(ctx context.Context) (NetworkStats, error) {
	out := NetworkStats{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM events`).Scan(&out.Events); err != nil {
		return out, fmt.Errorf("count events: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM profiles_latest`).Scan(&out.Profiles); err != nil {
		return out, fmt.Errorf("count profiles: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM ingest_checkpoints`).Scan(&out.Relays); err != nil {
		return out, fmt.Errorf("count relays: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedValues(ctx context.Context, tableName string, valueColumn string, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	tableName = strings.TrimSpace(tableName)
	valueColumn = strings.TrimSpace(valueColumn)
	if tableName == "" || valueColumn == "" {
		return nil, fmt.Errorf("table and value column are required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	allowed := map[string]map[string]struct{}{
		"curated_recommended_reads": {"event_id": {}},
		"curated_reads_topics":      {"topic": {}},
		"curated_featured_authors":  {"pubkey": {}},
	}
	columns, ok := allowed[tableName]
	if !ok {
		return nil, fmt.Errorf("unsupported curated table: %s", tableName)
	}
	if _, ok := columns[valueColumn]; !ok {
		return nil, fmt.Errorf("unsupported curated value column: %s", valueColumn)
	}
	query := fmt.Sprintf(`SELECT %s FROM %s ORDER BY rank DESC, %s ASC LIMIT $1`, valueColumn, tableName, valueColumn)
	rows, err := s.pool.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated values from %s: %w", tableName, err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan curated value row: %w", err)
		}
		out = append(out, value)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated values rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]CuratedRecommendedRead, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, title, url, rank
		FROM curated_recommended_reads
		ORDER BY rank DESC, event_id ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated recommended reads: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedRecommendedRead, 0, limit)
	for rows.Next() {
		var row CuratedRecommendedRead
		if err := rows.Scan(&row.EventID, &row.Title, &row.URL, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated recommended read row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated recommended read rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedReadsTopics(ctx context.Context, limit int) ([]CuratedReadsTopic, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT topic, rank
		FROM curated_reads_topics
		ORDER BY rank DESC, topic ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated reads topics: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedReadsTopic, 0, limit)
	for rows.Next() {
		var row CuratedReadsTopic
		if err := rows.Scan(&row.Topic, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated reads topic row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated reads topic rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]CuratedFeaturedAuthor, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, rank
		FROM curated_featured_authors
		ORDER BY rank DESC, pubkey ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get curated featured authors: %w", err)
	}
	defer rows.Close()
	out := make([]CuratedFeaturedAuthor, 0, limit)
	for rows.Next() {
		var row CuratedFeaturedAuthor
		if err := rows.Scan(&row.Pubkey, &row.Rank); err != nil {
			return nil, fmt.Errorf("scan curated featured author row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read curated featured author rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetCreatorPaidTiers(ctx context.Context, pubkey string) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return []json.RawMessage{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT json_build_object(
			'tier_id', tier_id,
			'title', title,
			'price_sats', price_sats
		)::text
		FROM curated_creator_paid_tiers
		WHERE pubkey = $1
		ORDER BY price_sats ASC, tier_id ASC
	`, pubkey)
	if err != nil {
		return nil, fmt.Errorf("get creator paid tiers: %w", err)
	}
	defer rows.Close()
	out := make([]json.RawMessage, 0)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan creator paid tier row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read creator paid tier rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetPubkeyByLNAddress(ctx context.Context, lnAddress string) (string, error) {
	if s == nil || s.pool == nil {
		return "", fmt.Errorf("store is not initialized")
	}
	normalized := strings.ToLower(strings.TrimSpace(lnAddress))
	if normalized == "" {
		return "", ErrNotFound
	}
	var pubkey string
	err := s.pool.QueryRow(ctx, `
		SELECT pubkey
		FROM profiles_latest
		WHERE lower(coalesce(nip05, '')) = $1
		LIMIT 1
	`, normalized).Scan(&pubkey)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("get pubkey by ln address: %w", err)
	}
	return pubkey, nil
}

func (s *PostgresStore) GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	identifier = strings.TrimSpace(identifier)
	if pubkey == "" || identifier == "" {
		return nil, fmt.Errorf("pubkey and identifier are required")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT et.value
		FROM replaceable_state rs
		INNER JOIN event_tags et
		        ON et.event_id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = 30000
		  AND rs.d_tag = $2
		  AND et.value_index = 0
		  AND et.tag_name IN ('p', 'e', 't', 'word')
		ORDER BY et.tag_name ASC, et.value ASC
	`, pubkey, identifier)
	if err != nil {
		return nil, fmt.Errorf("get moderation list by identifier: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0)
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, fmt.Errorf("scan moderation list by identifier row: %w", err)
		}
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read moderation list by identifier rows: %w", err)
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	return out, nil
}

func (s *PostgresStore) GetParameterizedReplaceableListByIdentifier(
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
