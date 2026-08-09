package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PruneFilteredEventTags deletes a bounded batch of event_tags rows that the
// ingest allowlist (internal/eventtags) would no longer persist:
// non-allowlisted tag names, kind-3 contact-list p-tags, and kind-10002
// relay-list r-tags. events.raw_json is untouched; the table is a derived
// join index and can be rebuilt from it.
//
// Runs two independently-timed deletes in one transaction rather than the
// single combined query this used to be:
//   - the disallowed-tag-name branch is backed by
//     idx_event_tags_disallowed_tag_name and stays cheap forever once the
//     historical backlog drains (see migrations/000071).
//   - the kind-scoped (kind-3 p-tag / kind-10002 r-tag) branch has no such
//     index — events carries raw_json, so both a seq scan and an
//     index+heap-fetch scan of the driving side are expensive — and costs
//     roughly the count of kind-3/kind-10002 events on every tick, drained
//     or not.
//
// Splitting them keeps that cost difference visible in
// nostrmash_db_operation_duration_seconds per operation instead of averaged
// together, and lets PruneFilteredEventTagsKindScoped alone justify why
// WORKER_RETENTION_EVENT_TAGS_RUN_INTERVAL must stay infrequent (ingest
// already refuses to write either category going forward, so there is no
// benefit to checking often — see internal/eventtags.ShouldPersist).
func (s *Retention) PruneFilteredEventTags(ctx context.Context, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	var disallowedNames, kindScoped int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		started := time.Now()
		rows, err := q.PruneFilteredEventTagsDisallowedNames(ctx, int32(limit))
		metrics.ObserveDBOperation("prune_filtered_event_tags_disallowed_names", dbResultFromErr(err), time.Since(started))
		if err != nil {
			return err
		}
		disallowedNames = rows

		started = time.Now()
		rows, err = q.PruneFilteredEventTagsKindScoped(ctx, int32(limit))
		metrics.ObserveDBOperation("prune_filtered_event_tags_kind_scoped", dbResultFromErr(err), time.Since(started))
		if err != nil {
			return err
		}
		kindScoped = rows
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("prune filtered event tags: %w", err)
	}
	return disallowedNames + kindScoped, nil
}
