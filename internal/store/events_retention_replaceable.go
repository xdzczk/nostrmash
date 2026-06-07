package store

import (
	"context"
	"fmt"
	"time"
)

// supersededReplaceableKinds are the non-parameterized replaceable kinds whose
// older versions carry no value once a newer version wins: profile metadata
// (0), contact/follow lists (3), and relay lists (10002). Nostr semantics
// define these as latest-wins, so every superseded version is dead weight.
// Their d_tag is always empty, which the purge query relies on.
var supersededReplaceableKinds = []int{0, 3, 10002}

// PurgeSupersededReplaceableEvents deletes a bounded batch of raw replaceable
// events (kinds 0/3/10002) that have been strictly superseded by a newer
// winner recorded in replaceable_state. The current winner is never touched
// (replaceable_state.event_id points at it), and a newer event that has not
// yet been projected is also protected because it ranks above the recorded
// winner rather than below it.
//
// Cascade FKs (event_tags, event_relays, event_references, pubkey_references,
// follower_edges, …) clean the dependent rows. The latest-version projections
// (contact_lists_latest, relay_lists_latest, profiles_latest, replaceable_state)
// all reference the winner, so deleting older versions leaves the read models
// intact.
//
// Safety guards:
//   - supersededBefore filters on events.first_seen_at, so a version that was
//     ingested recently (even if its author-claimed created_at is ancient,
//     e.g. during a backfill) is left alone until it has been stable for the
//     operator's grace window.
//   - A candidate is skipped while its derive_event_bundle job is pending or
//     running, and while that job is dead but was last updated after
//     deadGraceBefore. Past deadGraceBefore a permanently-dead derivation no
//     longer pins disk.
//
// The (kind, created_at) scan is served by idx_events_kind_created_at; the
// per-candidate replaceable_state lookup is a primary-key probe on
// (pubkey, kind, d_tag); the jobs lookup is served by the unique index on
// jobs.idempotency_key.
func (s *PostgresStore) PurgeSupersededReplaceableEvents(
	ctx context.Context,
	supersededBefore time.Time,
	deadGraceBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if supersededBefore.IsZero() {
		return 0, fmt.Errorf("supersededBefore is required")
	}
	if deadGraceBefore.IsZero() {
		return 0, fmt.Errorf("deadGraceBefore is required")
	}

	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT e.id
			FROM events e
			JOIN replaceable_state rs
			  ON rs.pubkey = e.pubkey
			 AND rs.kind = e.kind
			 AND rs.d_tag = ''
			WHERE e.kind = ANY($1::int[])
			  AND e.first_seen_at < $2
			  AND (
				rs.created_at > e.created_at
				OR (rs.created_at = e.created_at AND rs.event_id > e.id)
			  )
			  AND NOT EXISTS (
				SELECT 1
				FROM jobs j
				WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
				  AND (
					j.status IN ('pending', 'running')
					OR (j.status = 'dead' AND j.updated_at > $3)
				  )
			  )
			ORDER BY e.created_at ASC, e.id ASC
			LIMIT $4
		)
		DELETE FROM events e
		USING candidates c
		WHERE e.id = c.id
	`,
		supersededReplaceableKinds,
		supersededBefore.UTC(),
		deadGraceBefore.UTC(),
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("purge superseded replaceable events: %w", err)
	}
	return tag.RowsAffected(), nil
}
