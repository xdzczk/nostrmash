package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
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
// The (kind, created_at) scan is served by idx_events_kind_created; the
// per-candidate jobs lookup is served by the unique index on
// jobs.idempotency_key.
func (s *Retention) PurgeProcessedDeletionEvents(
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
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PurgeProcessedDeletionEvents(ctx, retentiondb.PurgeProcessedDeletionEventsParams{
			CreatedBeforeUnix: createdBefore.UTC().Unix(),
			DeadGraceBefore:   tsz(deadGraceBefore.UTC()),
			RowLimit:          int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("purge_processed_deletion_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge processed deletion events: %w", err)
	}
	return rows, nil
}
