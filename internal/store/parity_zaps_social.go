package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const authorSentZapsSelect = `
	SELECT json_build_object(
		'event_id', zr.zap_receipt_id,
		'sender_pubkey', zr.sender_pubkey,
		'receiver_pubkey', zr.receiver_pubkey,
		'target_event_id', zr.event_id,
		'sats', zr.amount_sats,
		'msats', zr.amount_sats * 1000,
		'created_at', zr.created_at,
		'event', e.raw_json,
		'target_event', te.raw_json
	)::text,
	zr.created_at,
	zr.zap_receipt_id
	FROM zap_receipts zr
	INNER JOIN events e ON e.id = zr.zap_receipt_id
	LEFT JOIN events te ON te.id = zr.event_id
`

func (s *PostgresStore) GetAuthorSentZaps(
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

	type zapRow struct {
		raw       string
		createdAt int64
		id        string
	}
	rowsOut := make([]zapRow, 0, limit+1)

	var rows pgx.Rows
	var err error
	if cursor == nil {
		rows, err = s.pool.Query(ctx, authorSentZapsSelect+`
			WHERE zr.sender_pubkey = $1
			ORDER BY zr.created_at DESC, zr.zap_receipt_id DESC
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
				FROM zap_receipts zr
				WHERE zr.sender_pubkey = $1
				  AND zr.created_at = $2
				  AND zr.zap_receipt_id = $3
			)
		`, pubkey, cursor.CreatedAt, cursorID).Scan(&cursorExists); err != nil {
			return nil, nil, fmt.Errorf("lookup author sent zaps cursor existence: %w", err)
		}
		if !cursorExists {
			return []json.RawMessage{}, nil, nil
		}
		rows, err = s.pool.Query(ctx, authorSentZapsSelect+`
			WHERE zr.sender_pubkey = $1
			  AND (zr.created_at, zr.zap_receipt_id) < ($2, $3)
			ORDER BY zr.created_at DESC, zr.zap_receipt_id DESC
			LIMIT $4
		`, pubkey, cursor.CreatedAt, cursorID, limit+1)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("get author sent zaps: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var row zapRow
		if err := rows.Scan(&row.raw, &row.createdAt, &row.id); err != nil {
			return nil, nil, fmt.Errorf("scan author sent zaps row: %w", err)
		}
		rowsOut = append(rowsOut, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read author sent zaps rows: %w", err)
	}

	hasMore := len(rowsOut) > limit
	if hasMore {
		rowsOut = rowsOut[:limit]
	}
	out := make([]json.RawMessage, 0, len(rowsOut))
	for _, row := range rowsOut {
		out = append(out, json.RawMessage(row.raw))
	}
	out = enrichZapRecords(out)

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
