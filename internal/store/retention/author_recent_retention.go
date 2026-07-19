package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// PruneAuthorRecentEvents bounds the author_recent_events projection with two
// passes executed in one call:
//
//  1. Age pass: rows whose event created_at is older than olderThan are
//     deleted (bounded by deleteBatchLimit, served by
//     idx_author_recent_events_created_at from migration 000054).
//  2. Cap pass: for up to authorBatchLimit authors holding more than
//     perAuthorCap rows, everything beyond the newest perAuthorCap rows is
//     deleted (bounded by deleteBatchLimit, served by
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
	ageTag, err := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT author_pubkey, event_id
			FROM author_recent_events
			WHERE created_at < $1
			LIMIT $2
		)
		DELETE FROM author_recent_events a
		USING candidates c
		WHERE a.author_pubkey = c.author_pubkey
		  AND a.event_id = c.event_id
	`, olderThan.UTC().Unix(), deleteBatchLimit)
	metrics.ObserveDBOperation("prune_author_recent_events_age", dbResultFromErr(err), time.Since(ageStarted))
	if err != nil {
		return 0, fmt.Errorf("prune author recent events by age: %w", err)
	}
	deleted := ageTag.RowsAffected()

	capStarted := time.Now()
	capTag, err := s.pool.Exec(ctx, `
		WITH offenders AS (
			SELECT author_pubkey
			FROM author_recent_events
			GROUP BY author_pubkey
			HAVING count(*) > $1
			LIMIT $2
		),
		victims AS (
			SELECT r.author_pubkey, r.event_id
			FROM offenders o
			CROSS JOIN LATERAL (
				SELECT a.author_pubkey, a.event_id
				FROM author_recent_events a
				WHERE a.author_pubkey = o.author_pubkey
				ORDER BY a.created_at DESC, a.event_id DESC
				OFFSET $1
			) r
			LIMIT $3
		)
		DELETE FROM author_recent_events a
		USING victims v
		WHERE a.author_pubkey = v.author_pubkey
		  AND a.event_id = v.event_id
	`, perAuthorCap, authorBatchLimit, deleteBatchLimit)
	metrics.ObserveDBOperation("prune_author_recent_events_cap", dbResultFromErr(err), time.Since(capStarted))
	if err != nil {
		return deleted, fmt.Errorf("prune author recent events by cap: %w", err)
	}
	return deleted + capTag.RowsAffected(), nil
}
