package store

import (
	"context"
	"fmt"
	"strings"
)

// normalizeSeedPubkeys trims, lowercases, and de-duplicates seed pubkeys,
// dropping blank entries. De-duplication matters because UpsertActiveSeeds
// feeds the slice into a single multi-row INSERT ... ON CONFLICT, which
// errors if the same conflict target appears twice in one statement.
func normalizeSeedPubkeys(pubkeys []string) []string {
	seen := make(map[string]struct{}, len(pubkeys))
	out := make([]string, 0, len(pubkeys))
	for _, p := range pubkeys {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

// UpsertActiveSeeds inserts the provided pubkeys as active trust seeds,
// reactivating any that were previously marked inactive. Input is normalized
// (trimmed, lowercased, de-duplicated); blank entries are skipped. The
// ON CONFLICT WHERE guard skips already-active rows so updated_at does not
// churn on every restart. Returns the number of rows inserted or reactivated.
func (s *PostgresStore) UpsertActiveSeeds(ctx context.Context, pubkeys []string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	normalized := normalizeSeedPubkeys(pubkeys)
	if len(normalized) == 0 {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO trust_seeds (pubkey, is_active)
		SELECT unnest($1::text[]), true
		ON CONFLICT (pubkey) DO UPDATE
		SET is_active = true,
		    updated_at = now()
		WHERE trust_seeds.is_active IS DISTINCT FROM true
	`, normalized)
	if err != nil {
		return 0, fmt.Errorf("upsert active trust seeds: %w", err)
	}
	return tag.RowsAffected(), nil
}

// LoadTrustedSnapshotPubkeys returns every pubkey in trust_graph_snapshot
// within maxHops of a seed (seeds are hop 0, already included in the snapshot).
// This is the authoritative trusted-author set the live ingest gate enforces
// against. Returned pubkeys are lowercased.
func (s *PostgresStore) LoadTrustedSnapshotPubkeys(ctx context.Context, maxHops int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if maxHops < 0 {
		maxHops = 0
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey
		FROM trust_graph_snapshot
		WHERE min_hops <= $1
	`, maxHops)
	if err != nil {
		return nil, fmt.Errorf("load trusted snapshot pubkeys: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, 1024)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan trusted snapshot pubkey: %w", err)
		}
		pubkey = strings.ToLower(strings.TrimSpace(pubkey))
		if pubkey == "" {
			continue
		}
		out = append(out, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read trusted snapshot pubkeys: %w", err)
	}
	return out, nil
}

// DeactivateMissingSeeds marks every currently-active seed that is NOT in the
// keep set as inactive, making the keep set authoritative for active seeds.
//
// IMPORTANT: callers must not pass an empty keep set when they intend "leave
// existing seeds alone" - an empty array makes the `<> ALL` predicate true for
// every row and would deactivate all seeds. The trust_worker reconcile guards
// against this by skipping reconciliation entirely when no seeds are
// configured. Returns the number of seeds deactivated.
func (s *PostgresStore) DeactivateMissingSeeds(ctx context.Context, keep []string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	normalized := normalizeSeedPubkeys(keep)
	tag, err := s.pool.Exec(ctx, `
		UPDATE trust_seeds
		SET is_active = false,
		    updated_at = now()
		WHERE is_active = true
		  AND pubkey <> ALL($1::text[])
	`, normalized)
	if err != nil {
		return 0, fmt.Errorf("deactivate missing trust seeds: %w", err)
	}
	return tag.RowsAffected(), nil
}
