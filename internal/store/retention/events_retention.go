package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
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
//
// Before deleting, each candidate's incremental author-stat deltas are
// reversed (see IncrementalStatsReverser) so profile_public_stats /
// author_activity_daily / author_hashtag_daily / author_media_daily /
// author_hourly_activity counters don't drift upward as engagement events
// age out. The candidate selection and the delete happen in the same
// transaction, so the exact set of ids that was reversed is the exact set
// deleted.
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
	var rows int64
	err := s.guardedWithTx(ctx, func(tx pgx.Tx, q *retentiondb.Queries) error {
		ids, err := q.SelectExpiredEngagementEventCandidates(ctx, retentiondb.SelectExpiredEngagementEventCandidatesParams{
			CreatedBeforeUnix: createdBefore.UTC().Unix(),
			DeadGraceBefore:   tsz(deadGraceBefore.UTC()),
			RowLimit:          int32(limit),
		})
		if err != nil {
			return fmt.Errorf("select expired engagement event candidates: %w", err)
		}
		rows, err = s.reverseAndDeleteTx(ctx, tx, q, ids)
		return err
	})
	metrics.ObserveDBOperation("purge_expired_engagement_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge expired engagement events: %w", err)
	}
	return rows, nil
}
