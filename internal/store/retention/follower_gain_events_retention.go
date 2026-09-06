package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PruneExpiredFollowerGainEvents deletes a bounded batch of
// follower_gain_events rows (see migrations/000086_follower_gain_events.sql)
// whose insert time is older than createdBefore. Nothing reads gains past
// the widest (7d) discovery window, so the horizon is a hygiene bound, not
// a correctness one — the only behavioral effect of pruning is that a
// pruned (followed, follower) pair may count again if that follower
// unfollows and re-follows after the horizon.
//
// The prune keys off the row's insert time (created_at), not the
// event-supplied gained_at: a hostile contact list could post-date its
// created_at, and rows planted with far-future gained_at would otherwise
// survive an age prune forever.
func (s *Retention) PruneExpiredFollowerGainEvents(
	ctx context.Context,
	createdBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if createdBefore.IsZero() {
		return 0, fmt.Errorf("createdBefore is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	started := time.Now()
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PruneExpiredFollowerGainEvents(ctx, retentiondb.PruneExpiredFollowerGainEventsParams{
			CreatedBefore: tsz(createdBefore.UTC()),
			RowLimit:      int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("prune_expired_follower_gain_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("prune expired follower gain events: %w", err)
	}
	return rows, nil
}
