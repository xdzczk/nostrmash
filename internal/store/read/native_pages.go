package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// GetModerationListTags returns the latest kind list for pubkey with each
// entry's tag name preserved, plus the raw replaceable event when one is stored.
func (s *Read) GetModerationListTags(
	ctx context.Context,
	pubkey string,
	kind int,
) ([]readmodel.ModerationTag, json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, nil, fmt.Errorf("pubkey is required")
	}
	var raw string
	err := s.pool.QueryRow(ctx, `
		SELECT e.raw_json::text
		FROM replaceable_state rs
		INNER JOIN events e ON e.id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		  AND rs.d_tag = ''
		LIMIT 1
	`, pubkey, kind).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("get moderation list event: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
		SELECT et.tag_name, et.value
		FROM replaceable_state rs
		INNER JOIN event_tags et ON et.event_id = rs.event_id
		WHERE rs.pubkey = $1
		  AND rs.kind = $2
		  AND rs.d_tag = ''
		  AND et.value_index = 0
		  AND et.tag_name IN ('p', 'e', 't', 'word')
		ORDER BY et.tag_index ASC, et.tag_name ASC, et.value ASC
	`, pubkey, kind)
	if err != nil {
		return nil, nil, fmt.Errorf("get moderation list tags: %w", err)
	}
	defer rows.Close()
	tags := make([]readmodel.ModerationTag, 0)
	for rows.Next() {
		var tag readmodel.ModerationTag
		if err := rows.Scan(&tag.Name, &tag.Value); err != nil {
			return nil, nil, fmt.Errorf("scan moderation list tag: %w", err)
		}
		tag.Name = strings.TrimSpace(tag.Name)
		tag.Value = strings.TrimSpace(tag.Value)
		if tag.Name == "" || tag.Value == "" {
			continue
		}
		tags = append(tags, tag)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read moderation list tags: %w", err)
	}
	return tags, json.RawMessage(raw), nil
}

// GetFollowersByPubkeyPage returns one keyset page of follower edges.
// Order is contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC,
// which matches idx_follower_edges_lookup. A nil cursor is the first page.
func (s *Read) GetFollowersByPubkeyPage(
	ctx context.Context,
	targetPubkey string,
	limit int,
	cursor *readmodel.FollowerCursor,
) ([]json.RawMessage, *readmodel.FollowerCursor, error) {
	if s == nil || s.pool == nil {
		return nil, nil, fmt.Errorf("store is not initialized")
	}
	targetPubkey = strings.TrimSpace(targetPubkey)
	if targetPubkey == "" {
		return nil, nil, fmt.Errorf("target pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	query := `
		SELECT json_build_object(
			'follower_pubkey', follower_pubkey,
			'source_event_id', source_event_id,
			'contact_list_created_at', contact_list_created_at
		)::text,
		contact_list_created_at,
		source_event_id,
		follower_pubkey
		FROM follower_edges
		WHERE followed_pubkey = $1`
	args := []any{targetPubkey}
	if cursor != nil && strings.TrimSpace(cursor.FollowerPubkey) != "" && strings.TrimSpace(cursor.SourceEventID) != "" {
		args = append(args, cursor.ContactListCreatedAt, strings.TrimSpace(cursor.SourceEventID), strings.TrimSpace(cursor.FollowerPubkey))
		query += `
		  AND (
		    contact_list_created_at < $2
		    OR (contact_list_created_at = $2 AND source_event_id < $3)
		    OR (contact_list_created_at = $2 AND source_event_id = $3 AND follower_pubkey > $4)
		  )`
	}
	args = append(args, limit+1)
	query += fmt.Sprintf(`
		ORDER BY contact_list_created_at DESC, source_event_id DESC, follower_pubkey ASC
		LIMIT $%d`, len(args))
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, fmt.Errorf("get followers page: %w", err)
	}
	defer rows.Close()
	type pageRow struct {
		raw     json.RawMessage
		created int64
		source  string
		pubkey  string
	}
	pageRows := make([]pageRow, 0, limit+1)
	for rows.Next() {
		var raw string
		var row pageRow
		if err := rows.Scan(&raw, &row.created, &row.source, &row.pubkey); err != nil {
			return nil, nil, fmt.Errorf("scan followers page: %w", err)
		}
		row.raw = json.RawMessage(raw)
		pageRows = append(pageRows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("read followers page: %w", err)
	}
	hasMore := len(pageRows) > limit
	if hasMore {
		pageRows = pageRows[:limit]
	}
	out := make([]json.RawMessage, 0, len(pageRows))
	for _, row := range pageRows {
		out = append(out, row.raw)
	}
	if !hasMore || len(pageRows) == 0 {
		return out, nil, nil
	}
	last := pageRows[len(pageRows)-1]
	return out, &readmodel.FollowerCursor{
		ContactListCreatedAt: last.created,
		SourceEventID:        last.source,
		FollowerPubkey:       last.pubkey,
	}, nil
}

// GetMutualFollowsPage returns one page of pubkeys ordered by pubkey ASC.
// When left and right are the same account, the page is that account's mutuals:
// people they follow who follow them back. When the accounts differ, the page
// is the intersection of the two outbound follow lists, matching mutual_follows
// on the compatibility socket.
func (s *Read) GetMutualFollowsPage(
	ctx context.Context,
	leftPubkey string,
	rightPubkey string,
	limit int,
	afterPubkey string,
) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	leftPubkey = strings.TrimSpace(leftPubkey)
	rightPubkey = strings.TrimSpace(rightPubkey)
	afterPubkey = strings.TrimSpace(afterPubkey)
	if leftPubkey == "" || rightPubkey == "" {
		return nil, fmt.Errorf("both pubkeys are required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	var (
		query string
		args  []any
	)
	if strings.EqualFold(leftPubkey, rightPubkey) {
		query = `
			SELECT fe1.followed_pubkey
			FROM follower_edges fe1
			WHERE fe1.follower_pubkey = $1
			  AND fe1.followed_pubkey <> $1
			  AND ($2 = '' OR fe1.followed_pubkey > $2)
			  AND EXISTS (
			    SELECT 1
			    FROM follower_edges fe2
			    WHERE fe2.followed_pubkey = $1
			      AND fe2.follower_pubkey = fe1.followed_pubkey
			  )
			ORDER BY fe1.followed_pubkey ASC
			LIMIT $3`
		args = []any{leftPubkey, afterPubkey, limit + 1}
	} else {
		query = `
			SELECT fe1.followed_pubkey
			FROM follower_edges fe1
			INNER JOIN follower_edges fe2
			        ON fe2.followed_pubkey = fe1.followed_pubkey
			       AND fe2.follower_pubkey = $3
			WHERE fe1.follower_pubkey = $1
			  AND ($2 = '' OR fe1.followed_pubkey > $2)
			ORDER BY fe1.followed_pubkey ASC
			LIMIT $4`
		args = []any{leftPubkey, afterPubkey, rightPubkey, limit + 1}
	}
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get mutual follows page: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit+1)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan mutual follow page: %w", err)
		}
		pubkey = strings.TrimSpace(pubkey)
		if pubkey != "" {
			out = append(out, pubkey)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read mutual follows page: %w", err)
	}
	return out, nil
}
