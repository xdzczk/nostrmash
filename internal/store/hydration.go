package store

import (
	"context"
	"fmt"
	"strings"
)

// ListRecentRelayURLs returns up to limit relay URLs the ingestor has recently
// tracked checkpoints for, most-recently-updated first. Used as the fallback
// relay set for on-demand hydration when no explicit HYDRATION_RELAYS is
// configured.
func (s *PostgresStore) ListRecentRelayURLs(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		limit = 16
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url
		FROM ingest_checkpoints
		GROUP BY relay_url
		ORDER BY MAX(updated_at) DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list recent relay urls: %w", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var url string
		if err := rows.Scan(&url); err != nil {
			return nil, fmt.Errorf("scan relay url: %w", err)
		}
		if url = strings.TrimSpace(url); url != "" {
			out = append(out, url)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay urls: %w", err)
	}
	return out, nil
}

// HasInflightHydration reports whether a hydrate_account job for the given
// pubkey is currently pending or running. Used to dedupe enqueue requests and
// to surface the "hydrating" completeness status.
func (s *PostgresStore) HasInflightHydration(ctx context.Context, pubkey string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("store is not initialized")
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return false, fmt.Errorf("pubkey is required")
	}
	var exists bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM jobs
			WHERE job_type = 'hydrate_account'
			  AND status IN ('pending', 'running')
			  AND payload->>'pubkey' = $1
		)
	`, pubkey).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check inflight hydration: %w", err)
	}
	return exists, nil
}
