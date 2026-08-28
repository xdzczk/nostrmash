package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeOrphanDeletionLedger deletes one bounded keyset window of
// deletion_events tombstones older than createdBefore whose target event is
// not present in events. Tombstones whose target is stored are keepers: they
// still suppress the target in the DM read paths and survive regardless of
// age. See the query doc in queries/retention.sql for the retention-cost
// reasoning.
//
// The scan resumes strictly after the composite keyset cursor
// (cursorCreatedAt unix seconds, cursorEventID) and covers at most limit rows
// ordered by (created_at, event_id). It returns how many rows the window
// covered (scanned, deleted + keepers), how many orphan tombstones were
// removed (deleted), and the last scanned row's key (lastCreatedAt,
// lastEventID; zero values when the window was empty); callers loop windows
// by feeding the last key back in as the next cursor until scanned < limit.
// Plain values (not a struct) so internal/jobs can consume this through a
// local interface without importing this package.
func (s *Retention) PurgeOrphanDeletionLedger(
	ctx context.Context,
	cursorCreatedAt int64,
	cursorEventID string,
	createdBefore time.Time,
	limit int,
) (scanned, deleted, lastCreatedAt int64, lastEventID string, err error) {
	if s == nil || s.pool == nil {
		return 0, 0, 0, "", fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, 0, 0, "", fmt.Errorf("limit must be > 0")
	}
	if createdBefore.IsZero() {
		return 0, 0, 0, "", fmt.Errorf("createdBefore is required")
	}

	started := time.Now()
	var row retentiondb.PurgeOrphanDeletionLedgerRow
	err = s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		row, err = q.PurgeOrphanDeletionLedger(ctx, retentiondb.PurgeOrphanDeletionLedgerParams{
			CursorUnix:        cursorCreatedAt,
			CursorEventID:     cursorEventID,
			CreatedBeforeUnix: createdBefore.UTC().Unix(),
			RowLimit:          int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("purge_orphan_deletion_ledger", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, 0, 0, "", fmt.Errorf("purge orphan deletion ledger: %w", err)
	}
	return row.Scanned, row.Deleted, row.LastCreatedAt, row.LastEventID, nil
}
