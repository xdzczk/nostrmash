package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

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
