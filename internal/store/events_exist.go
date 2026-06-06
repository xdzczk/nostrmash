package store

import (
	"context"
	"fmt"
	"strings"
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
