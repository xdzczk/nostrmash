package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

// EventsExist reports whether ANY of the supplied event ids exists in the
// canonical events table. It is used by the live ingest gate to keep
// engagement events (kinds 6/7/9735) only when they target already-stored
// (i.e. in-scope/trusted) content. Returns false for an empty id list.
//
// Note the "any exists" semantics: an engagement event is kept if it references
// at least one event we already store, which is the self-consistent rule the
// gate needs without an additional trust lookup.
func (s *PostgresStore) EventsExist(ctx context.Context, ids []string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("store is not initialized")
	}
	normalized := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}
	if len(normalized) == 0 {
		return false, nil
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM events WHERE id = ANY($1::text[])
		)
	`, normalized).Scan(&exists); err != nil {
		return false, fmt.Errorf("check events exist: %w", err)
	}
	return exists, nil
}

// ReplaceableAddressesExist reports whether any NIP-33 coordinate already has
// a current row in replaceable_state. Empty input is false.
func (s *PostgresStore) ReplaceableAddressesExist(ctx context.Context, addresses []nostr.AddressCoordinate) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("store is not initialized")
	}
	if len(addresses) == 0 {
		return false, nil
	}
	kinds := make([]int32, 0, len(addresses))
	pubkeys := make([]string, 0, len(addresses))
	dTags := make([]string, 0, len(addresses))
	seen := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		key := addr.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		kinds = append(kinds, int32(addr.Kind))
		pubkeys = append(pubkeys, addr.Pubkey)
		dTags = append(dTags, addr.DTag)
	}
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM replaceable_state rs
			JOIN unnest($1::int[], $2::text[], $3::text[]) AS a(kind, pubkey, d_tag)
			  ON rs.kind = a.kind
			 AND rs.pubkey = a.pubkey
			 AND rs.d_tag = a.d_tag
		)
	`, kinds, pubkeys, dTags).Scan(&exists); err != nil {
		return false, fmt.Errorf("check replaceable addresses exist: %w", err)
	}
	return exists, nil
}
