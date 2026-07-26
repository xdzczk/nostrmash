package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeUntrustedAuthorEvents deletes a bounded batch of raw author-gated
// events (kinds 1/4/9802/10000/10003/30023) whose author is absent from
// trust_graph_snapshot and which are older than the operator's horizon. This
// is the months-scale complement to the ingest trust gate: the gate bounds
// future writes, this reclaims the untrusted residue the gate let through
// (open/shadow mode, pre-gate history, authors later dropped from the graph).
//
// Age is enforced on BOTH the author-claimed created_at (indexable via
// idx_events_kind_created_at) and the ingest-time first_seen_at, so a
// freshly-backfilled event with an ancient created_at is never purged before
// it has actually been resident for the horizon.
//
// Fail-safe: if trust_graph_snapshot is empty (never loaded, or wiped during
// a trust rebuild), nothing is deleted. Otherwise an empty graph would
// classify every author as untrusted and delete all authored content.
//
// Derivation safety mirrors the other retention purgers: an event is skipped
// while its derive_event_bundle job is pending/running, or dead and updated
// after deadGraceBefore.
//
// Trade-off (accepted, same as engagement retention): if an author becomes
// trusted later, their pre-trust history is gone locally and must be
// re-hydrated from relays.
func (s *Retention) PurgeUntrustedAuthorEvents(
	ctx context.Context,
	olderThan time.Time,
	deadGraceBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if olderThan.IsZero() {
		return 0, fmt.Errorf("olderThan is required")
	}
	if deadGraceBefore.IsZero() {
		return 0, fmt.Errorf("deadGraceBefore is required")
	}

	started := time.Now()
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PurgeUntrustedAuthorEvents(ctx, retentiondb.PurgeUntrustedAuthorEventsParams{
			CreatedBeforeUnix: olderThan.UTC().Unix(),
			FirstSeenBefore:   tsz(olderThan.UTC()),
			DeadGraceBefore:   tsz(deadGraceBefore.UTC()),
			RowLimit:          int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("purge_untrusted_author_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge untrusted author events: %w", err)
	}
	return rows, nil
}
