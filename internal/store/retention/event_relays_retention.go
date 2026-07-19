package store

import (
	"context"
	"fmt"
	"time"
)

// PurgeStaleEventRelays deletes a bounded batch of event_relays provenance
// rows seen before seenBefore, always retaining the earliest-seen row per
// event (ties broken by relay_url) so first-provenance survives forever.
// Windowed relay analytics read far inside any sane horizon and are
// unaffected; only long-tail duplicate provenance is reclaimed.
//
// The seen_at scan is served by idx_event_relays_seen_at_pubkey (migration
// 000045); the earliest-row check is served by the (event_id, relay_url)
// primary key.
func (s *PostgresStore) PurgeStaleEventRelays(
	ctx context.Context,
	seenBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if seenBefore.IsZero() {
		return 0, fmt.Errorf("seenBefore is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT er.event_id, er.relay_url
			FROM event_relays er
			WHERE er.seen_at < $1
			  AND EXISTS (
				SELECT 1
				FROM event_relays first
				WHERE first.event_id = er.event_id
				  AND (
					first.seen_at < er.seen_at
					OR (first.seen_at = er.seen_at AND first.relay_url < er.relay_url)
				  )
			  )
			LIMIT $2
		)
		DELETE FROM event_relays er
		USING candidates c
		WHERE er.event_id = c.event_id
		  AND er.relay_url = c.relay_url
	`, seenBefore.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("purge stale event relays: %w", err)
	}
	return tag.RowsAffected(), nil
}
