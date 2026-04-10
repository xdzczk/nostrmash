package store

import (
	"context"
	"fmt"
)

// FindActiveAuthorsWithoutMetadata returns pubkeys that have authored kind-1
// notes but have no kind-0 metadata event anywhere in the database and no
// entry in profiles_latest. These are candidates for relay-based metadata
// discovery.
func (s *PostgresStore) FindActiveAuthorsWithoutMetadata(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT e.pubkey
		FROM events e
		WHERE e.kind = 1
		  AND NOT EXISTS (
			SELECT 1 FROM events e2
			WHERE e2.kind = 0 AND e2.pubkey = e.pubkey
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profiles_latest pl
			WHERE pl.pubkey = e.pubkey
		  )
		ORDER BY e.pubkey
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("find active authors without metadata: %w", err)
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, fmt.Errorf("scan author pubkey: %w", err)
		}
		out = append(out, pubkey)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read author pubkey rows: %w", err)
	}
	return out, nil
}

// CountActiveAuthorsWithoutMetadata returns the number of note authors that
// have no kind-0 metadata at all (neither in events nor profiles_latest).
func (s *PostgresStore) CountActiveAuthorsWithoutMetadata(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var count int64
	err := s.pool.QueryRow(ctx, `
		SELECT COUNT(DISTINCT e.pubkey)
		FROM events e
		WHERE e.kind = 1
		  AND NOT EXISTS (
			SELECT 1 FROM events e2
			WHERE e2.kind = 0 AND e2.pubkey = e.pubkey
		  )
		  AND NOT EXISTS (
			SELECT 1 FROM profiles_latest pl
			WHERE pl.pubkey = e.pubkey
		  )
	`).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count active authors without metadata: %w", err)
	}
	return count, nil
}
