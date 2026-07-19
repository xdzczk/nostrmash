package read

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"strings"

	"github.com/jackc/pgx/v5"
)

type ContactListProjection = readmodel.ContactListProjection

type RelayListProjection = readmodel.RelayListProjection

// GetContactListByPubkey returns latest projected contact list for one pubkey.
func (s *Read) GetContactListByPubkey(ctx context.Context, pubkey string) (ContactListProjection, error) {
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
func (s *Read) GetRelayListByPubkey(ctx context.Context, pubkey string) (RelayListProjection, error) {
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
