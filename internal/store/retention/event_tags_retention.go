package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/eventtags"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PruneFilteredEventTags deletes a bounded batch of event_tags rows that the
// ingest allowlist (internal/eventtags) would no longer persist:
// non-allowlisted tag names, kind-3 contact-list p-tags, and kind-10002
// relay-list r-tags. events.raw_json is untouched; the table is a derived
// join index and can be rebuilt from it.
func (s *Retention) PruneFilteredEventTags(ctx context.Context, limit int) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	started := time.Now()
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PruneFilteredEventTags(ctx, retentiondb.PruneFilteredEventTagsParams{
			AllowedTagNames: eventtags.AllowedTagNamesCopy(),
			RowLimit:        int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("prune_filtered_event_tags", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("prune filtered event tags: %w", err)
	}
	return rows, nil
}
