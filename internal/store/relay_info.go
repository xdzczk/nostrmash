package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// RelayInfoPubkey returns the NIP-11 pubkey stored for a source relay.
// ok is false when the registry has no document or no pubkey.
func (s *PostgresStore) RelayInfoPubkey(ctx context.Context, relayURL string) (string, bool, error) {
	if s == nil || s.pool == nil {
		return "", false, fmt.Errorf("store is not initialized")
	}
	normalized, err := relayurl.Normalize(relayURL, relayurl.NormalizeOptions{})
	if err != nil {
		return "", false, nil
	}
	var pubkey string
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(capability_summary_json->'info'->>'pubkey', '')
		FROM relay_registry
		WHERE url_key = $1
	`, relayurl.CanonicalKey(normalized)).Scan(&pubkey)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load relay info pubkey: %w", err)
	}
	return pubkey, pubkey != "", nil
}
