package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type ContactListProjection struct {
	Pubkey          string
	EventID         string
	CreatedAt       int64
	DerivationVer   int
	ContactsJSONRaw json.RawMessage
}

type RelayListProjection struct {
	Pubkey        string
	EventID       string
	CreatedAt     int64
	DerivationVer int
	RelaysJSONRaw json.RawMessage
}

// GetContactListByPubkey returns latest projected contact list for one pubkey.
func (s *PostgresStore) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactListProjection, error) {
	out := ContactListProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	var contactsText string
	err := s.pool.QueryRow(ctx, `
		SELECT pubkey, event_id, created_at, derivation_version, contacts_json::text
		FROM contact_lists_latest
		WHERE pubkey = $1
	`, pubkey).Scan(
		&out.Pubkey,
		&out.EventID,
		&out.CreatedAt,
		&out.DerivationVer,
		&contactsText,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrNotFound
		}
		return out, fmt.Errorf("get contact list by pubkey: %w", err)
	}
	out.ContactsJSONRaw = json.RawMessage(contactsText)
	return out, nil
}

// GetRelayListByPubkey returns latest projected relay list for one pubkey.
func (s *PostgresStore) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayListProjection, error) {
	out := RelayListProjection{}
	if s == nil || s.pool == nil {
		return out, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return out, fmt.Errorf("pubkey is required")
	}

	var relaysText string
	err := s.pool.QueryRow(ctx, `
		SELECT pubkey, event_id, created_at, derivation_version, relays_json::text
		FROM relay_lists_latest
		WHERE pubkey = $1
	`, pubkey).Scan(
		&out.Pubkey,
		&out.EventID,
		&out.CreatedAt,
		&out.DerivationVer,
		&relaysText,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, ErrNotFound
		}
		return out, fmt.Errorf("get relay list by pubkey: %w", err)
	}
	out.RelaysJSONRaw = json.RawMessage(relaysText)
	return out, nil
}

// SearchEventsByContent returns note-like events filtered by content text.
func (s *PostgresStore) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []json.RawMessage{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT
				raw_json::text AS raw_text,
				created_at,
				id,
				ts_rank_cd(
					to_tsvector('simple', coalesce(content, '')),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM events
			WHERE kind = 1
			  AND (
				to_tsvector('simple', coalesce(content, '')) @@ websearch_to_tsquery('simple', $1)
				OR content ILIKE '%' || $1 || '%'
			  )
		)
		SELECT raw_text
		FROM ranked
		ORDER BY rank DESC, created_at DESC, id DESC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search events by content: %w", err)
	}
	defer rows.Close()

	out := make([]json.RawMessage, 0, limit)
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan searched event row: %w", err)
		}
		out = append(out, json.RawMessage(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read searched event rows: %w", err)
	}
	return out, nil
}

// SearchProfiles returns latest profile projections matching query.
func (s *PostgresStore) SearchProfiles(ctx context.Context, query string, limit int) ([]ProfileProjection, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return []ProfileProjection{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	rows, err := s.pool.Query(ctx, `
		WITH ranked AS (
			SELECT
				pubkey,
				metadata_event_id,
				metadata_created_at,
				profile_json::text AS profile_text,
				ts_rank_cd(
					to_tsvector(
						'simple',
						coalesce(pubkey, '') || ' ' ||
						coalesce(name, '') || ' ' ||
						coalesce(display_name, '') || ' ' ||
						coalesce(about, '') || ' ' ||
						coalesce(nip05, '')
					),
					websearch_to_tsquery('simple', $1)
				) AS rank
			FROM profiles_latest
			WHERE
				to_tsvector(
					'simple',
					coalesce(pubkey, '') || ' ' ||
					coalesce(name, '') || ' ' ||
					coalesce(display_name, '') || ' ' ||
					coalesce(about, '') || ' ' ||
					coalesce(nip05, '')
				) @@ websearch_to_tsquery('simple', $1)
				OR pubkey ILIKE '%' || $1 || '%'
				OR coalesce(name, '') ILIKE '%' || $1 || '%'
				OR coalesce(display_name, '') ILIKE '%' || $1 || '%'
				OR coalesce(about, '') ILIKE '%' || $1 || '%'
				OR coalesce(nip05, '') ILIKE '%' || $1 || '%'
		)
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_text
		FROM ranked
		ORDER BY rank DESC, metadata_created_at DESC, metadata_event_id DESC
		LIMIT $2
	`, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search profiles: %w", err)
	}
	defer rows.Close()

	out := make([]ProfileProjection, 0, limit)
	for rows.Next() {
		var row ProfileProjection
		var profileText string
		if err := rows.Scan(&row.Pubkey, &row.MetadataEventID, &row.MetadataCreatedAt, &profileText); err != nil {
			return nil, fmt.Errorf("scan searched profile row: %w", err)
		}
		row.ProfileJSON = json.RawMessage(profileText)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read searched profile rows: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) GetRecentEventsByKindAndPubkey(
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
	return out, nil
}

// GetEventsReferencingPubkey returns events that mention target pubkey in p-tags.
func (s *PostgresStore) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
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
		FROM pubkey_references pr
		INNER JOIN events e ON e.id = pr.source_event_id
		WHERE pr.referenced_pubkey = $1
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
	return out, nil
}

// GetFollowersByPubkey returns follower edges derived from latest kind:3 contact lists.
func (s *PostgresStore) GetFollowersByPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
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

func (s *PostgresStore) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
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

func (s *PostgresStore) GetHighlightsByATarget(
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

func (s *PostgresStore) GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error) {
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
