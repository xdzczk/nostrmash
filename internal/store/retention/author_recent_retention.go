package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// authorRecentCapTouchWindow bounds the cap-pass offender scan to authors
// whose projection was written recently. Authors who stop posting are drained
// by the age pass; only recently-touched authors can newly exceed the cap.
const authorRecentCapTouchWindow = 7 * 24 * time.Hour

// authorRecentCapAuthorScanLimit caps how many distinct recently-touched
// authors are inspected per cap pass before the perAuthorCap count filter.
// Sized well above authorBatchLimit so a hot tick still finds offenders.
const authorRecentCapAuthorScanLimit = 5000

// PruneAuthorRecentEvents bounds the author_recent_events projection with two
// passes executed in one call:
//
//  1. Age pass: rows whose event created_at is older than olderThan are
//     deleted (bounded by deleteBatchLimit, served by
//     idx_author_recent_events_created_at from migration 000054).
//  2. Cap pass: among authors touched recently (projected_at within
//     authorRecentCapTouchWindow), for up to authorBatchLimit authors holding
//     more than perAuthorCap rows, everything beyond the newest perAuthorCap
//     rows is deleted (bounded by deleteBatchLimit; offender discovery uses
//     idx_author_recent_events_projected_at, victim deletion uses
//     idx_author_recent_events_order).
//
// The projection is rebuildable from canonical events; recent-events reads
// serve at most 100 rows per request, so any cap >= 100 keeps unfiltered
// reads exact. Kind-filtered reads over a capped author may return fewer rows
// than an uncapped table would — an accepted trade-off documented in
// docs/design/storage-discipline.md.
func (s *Retention) PruneAuthorRecentEvents(
	ctx context.Context,
	olderThan time.Time,
	perAuthorCap int,
	authorBatchLimit int,
	deleteBatchLimit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if olderThan.IsZero() {
		return 0, fmt.Errorf("olderThan is required")
	}
	if perAuthorCap <= 0 || authorBatchLimit <= 0 || deleteBatchLimit <= 0 {
		return 0, fmt.Errorf("perAuthorCap, authorBatchLimit and deleteBatchLimit must be > 0")
	}

	ageStarted := time.Now()
	var ageDeleted int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		ageDeleted, err = q.PruneAuthorRecentEventsByAge(ctx, retentiondb.PruneAuthorRecentEventsByAgeParams{
			CreatedBeforeUnix: olderThan.UTC().Unix(),
			DeleteBatchLimit:  int32(deleteBatchLimit),
		})
		return err
	})
	metrics.ObserveDBOperation("prune_author_recent_events_age", dbResultFromErr(err), time.Since(ageStarted))
	if err != nil {
		return 0, fmt.Errorf("prune author recent events by age: %w", err)
	}

	capStarted := time.Now()
	var capDeleted int64
	touchedSince := time.Now().UTC().Add(-authorRecentCapTouchWindow)
	err = s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		capDeleted, err = q.PruneAuthorRecentEventsByCap(ctx, retentiondb.PruneAuthorRecentEventsByCapParams{
			TouchedSince:     tsz(touchedSince),
			AuthorScanLimit:  int32(authorRecentCapAuthorScanLimit),
			PerAuthorCap:     int64(perAuthorCap),
			AuthorBatchLimit: int32(authorBatchLimit),
			DeleteBatchLimit: int32(deleteBatchLimit),
		})
		return err
	})
	metrics.ObserveDBOperation("prune_author_recent_events_cap", dbResultFromErr(err), time.Since(capStarted))
	if err != nil {
		return ageDeleted, fmt.Errorf("prune author recent events by cap: %w", err)
	}
	return ageDeleted + capDeleted, nil
}
