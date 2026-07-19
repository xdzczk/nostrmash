package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// PurgeExpiredEngagementEvents deletes a bounded batch of raw engagement events
// (kinds 6/7/9735) whose author-claimed created_at is older than createdBefore,
// in ascending created_at order. Cascade FKs clean the dependent
// contribution/interaction rows; lifetime aggregate counters in
// reaction_counts/repost_counts have no FK to events and therefore survive.
//
// Derivation safety: an event is skipped while its derive_event_bundle job is
// pending or running, and also while that job is dead but was last updated
// after deadGraceBefore (i.e. within the operator's dead-grace window). Dead
// jobs older than deadGraceBefore no longer block the purge so a permanently
// broken derivation cannot pin disk forever.
//
// The (kind, created_at) scan is served by idx_events_kind_created_at; the
// per-candidate jobs lookup is served by the unique index on
// jobs.idempotency_key.
func (s *Retention) PurgeExpiredEngagementEvents(
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

	started := time.Now()
	tag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT e.id
			FROM events e
			WHERE e.kind IN (6, 7, 9735)
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
	metrics.ObserveDBOperation("purge_expired_engagement_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge expired engagement events: %w", err)
	}
	return tag.RowsAffected(), nil
}
