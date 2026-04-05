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
		SELECT raw_json::text
		FROM events
		WHERE kind = 1
		  AND content ILIKE '%' || $1 || '%'
		ORDER BY created_at DESC, id DESC
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
		SELECT pubkey, metadata_event_id, metadata_created_at, profile_json::text
		FROM profiles_latest
		WHERE pubkey ILIKE '%' || $1 || '%'
		   OR profile_json::text ILIKE '%' || $1 || '%'
		ORDER BY metadata_created_at DESC, metadata_event_id DESC
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
