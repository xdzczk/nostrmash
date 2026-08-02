package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PruneOrphanedAppliedStatDeltas deletes a bounded batch of applied_stat_deltas
// ledger rows (see migrations/000063_incremental_author_stats.sql) whose
// source event no longer exists in events.
//
// This is deliberately not an age-based purge: as long as an event row
// exists, its ledger rows must be kept, because a future retention purge
// that hard-deletes that event may still need to gate a decrement through
// unclaimStatDeltaTx (see reverseAndDeleteTx and
// derivation.Handlers.ReverseIncrementalAuthorStatsTx). Pruning a live
// event's ledger row early would silently disable that decrement and
// reintroduce the exact upward-drift bug the reversal path exists to
// prevent. Only once the event itself is gone — via a reversal-aware purge
// (which already deletes its own ledger rows) or via one of the two
// retention purges that don't touch incremental stats
// (PurgeSupersededReplaceableEvents, PurgeProcessedDeletionEvents) — is a
// remaining ledger row guaranteed to be a pure orphan safe to reclaim.
//
// appliedBefore is a conservative grace buffer, not a correctness
// requirement; it keeps the scan cheap via
// idx_applied_stat_deltas_applied_at and avoids touching freshly-inserted
// rows.
func (s *Retention) PruneOrphanedAppliedStatDeltas(
	ctx context.Context,
	appliedBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if appliedBefore.IsZero() {
		return 0, fmt.Errorf("appliedBefore is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	started := time.Now()
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PruneOrphanedAppliedStatDeltas(ctx, retentiondb.PruneOrphanedAppliedStatDeltasParams{
			AppliedBefore: tsz(appliedBefore.UTC()),
			RowLimit:      int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("prune_orphaned_applied_stat_deltas", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("prune orphaned applied stat deltas: %w", err)
	}
	return rows, nil
}
