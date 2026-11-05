package relayregistry

import (
	"context"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// LatestPropagationShares reads the newest completed day of
// relay_propagation_daily. Keys are the stored relay URL, its normalized
// form, and the canonical registry key, so admission can match either
// relay_registry.normalized_url or url_key.
func (s *Store) LatestPropagationShares(ctx context.Context) (map[string]PropagationShare, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("relay registry store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url,
		       first_seen_count::float8 / GREATEST(events_seen, 1),
		       exclusive_count::float8 / GREATEST(events_seen, 1)
		FROM relay_propagation_daily
		WHERE day = (SELECT max(day) FROM relay_propagation_daily)
	`)
	if err != nil {
		return nil, fmt.Errorf("load relay propagation shares: %w", err)
	}
	defer rows.Close()
	out := make(map[string]PropagationShare)
	for rows.Next() {
		var relayURL string
		var share PropagationShare
		if err := rows.Scan(&relayURL, &share.FirstSeen, &share.Exclusive); err != nil {
			return nil, fmt.Errorf("scan relay propagation share: %w", err)
		}
		out[relayURL] = share
		normalized, err := relayurl.Normalize(relayURL, relayurl.NormalizeOptions{})
		if err != nil {
			continue
		}
		out[normalized] = share
		if key := relayurl.CanonicalKey(normalized); key != "" {
			out[key] = share
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay propagation shares: %w", err)
	}
	return out, nil
}

// BlockedRefCounts returns relays whose kind-10006 lists name them.
func (s *Store) BlockedRefCounts(ctx context.Context) (map[string]int, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("relay registry store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT url_key, normalized_url, blocked_ref_count
		FROM relay_registry
		WHERE blocked_ref_count > 0
	`)
	if err != nil {
		return nil, fmt.Errorf("load blocked relay refs: %w", err)
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var urlKey, normalized string
		var count int
		if err := rows.Scan(&urlKey, &normalized, &count); err != nil {
			return nil, fmt.Errorf("scan blocked relay refs: %w", err)
		}
		out[urlKey] = count
		out[normalized] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read blocked relay refs: %w", err)
	}
	return out, nil
}

// InsertHintRelays adds relays named in e/p tag hints that are not already
// in the registry. Existing rows keep the user-list ref count written by
// discovery. hint_count of at least 3 over the last 7 days is the bar.
func (s *Store) InsertHintRelays(ctx context.Context, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("relay registry store is not initialized")
	}
	if limit <= 0 {
		limit = 25
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url
		FROM relay_hint_daily
		WHERE day >= (now() AT TIME ZONE 'utc')::date - 7
		GROUP BY relay_url
		HAVING sum(hint_count) >= 3
		ORDER BY sum(hint_count) DESC, relay_url ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return 0, fmt.Errorf("list relay hints: %w", err)
	}
	defer rows.Close()
	inserted := 0
	for rows.Next() {
		var relayURL string
		if err := rows.Scan(&relayURL); err != nil {
			return inserted, fmt.Errorf("scan relay hint: %w", err)
		}
		normalized, err := relayurl.Normalize(relayURL, relayurl.NormalizeOptions{})
		if err != nil {
			continue
		}
		tag, err := s.pool.Exec(ctx, `
			INSERT INTO relay_registry (url_key, normalized_url, distinct_user_ref_count, weighted_user_ref_score)
			VALUES ($1, $2, 1, 1)
			ON CONFLICT (url_key) DO NOTHING
		`, relayurl.CanonicalKey(normalized), normalized)
		if err != nil {
			return inserted, fmt.Errorf("insert hinted relay: %w", err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := rows.Err(); err != nil {
		return inserted, fmt.Errorf("read relay hints: %w", err)
	}
	return inserted, nil
}
