package store

import (
	"context"
	"fmt"
	"time"
)

// PurgeProcessedDeletionEvents deletes a bounded batch of raw deletion events
// (kind 5) whose author-claimed created_at is older than createdBefore, in
// ascending created_at order. The distilled deletion_events ledger row (deleter
// pubkey, target event id, created_at) is intentionally NOT removed: migration
// 000050 dropped the events FK cascade so the tombstone survives the purge.
// Cascade FKs still clean the raw event's tags/references/relays.
//
// Derivation safety: a kind-5 event is skipped while its derive_event_bundle
// job is pending or running, and also while that job is dead but was last
// updated after deadGraceBefore. Because the derive bundle job is enqueued in
// the same transaction as the event insert, a freshly-ingested deletion event
// always has a pending job and is therefore never purged before its
// deletion_events ledger row has been projected.
//
// The (kind, created_at) scan is served by idx_events_kind_created_at; the
// per-candidate jobs lookup is served by the unique index on
// jobs.idempotency_key.
func (s *PostgresStore) PurgeProcessedDeletionEvents(
	ctx context.Context,
	createdBefore time.Time,
	deadGraceBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if createdBefore.IsZero() {
		return 0, fmt.Errorf("createdBefore is required")
	}
	if deadGraceBefore.IsZero() {
		return 0, fmt.Errorf("deadGraceBefore is required")
	}

	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT e.id
			FROM events e
			WHERE e.kind = 5
			  AND e.created_at < $1
			  AND NOT EXISTS (
				SELECT 1
				FROM jobs j
				WHERE j.idempotency_key = 'derive_event_bundle:' || e.id
				  AND (
					j.status IN ('pending', 'running')
					OR (j.status = 'dead' AND j.updated_at > $2)
				  )
			  )
			ORDER BY e.created_at ASC, e.id ASC
			LIMIT $3
		)
		DELETE FROM events e
		USING candidates c
		WHERE e.id = c.id
	`,
		createdBefore.UTC().Unix(),
		deadGraceBefore.UTC(),
		limit,
	)
	if err != nil {
		return 0, fmt.Errorf("purge processed deletion events: %w", err)
	}
	return tag.RowsAffected(), nil
}
