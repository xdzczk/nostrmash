package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeStaleEventRelays deletes a bounded batch of event_relays provenance
// rows seen before seenBefore, always retaining the earliest-seen row per
// event (ties broken by relay_url) so first-provenance survives forever.
// Windowed relay analytics read far inside any sane horizon and are
// unaffected; only long-tail duplicate provenance is reclaimed.
//
// The seen_at scan is served by idx_event_relays_seen_at_pubkey (migration
// 000045); the earliest-row check is served by the (event_id, relay_url)
// primary key.
func (s *Retention) PurgeStaleEventRelays(
	ctx context.Context,
	seenBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if seenBefore.IsZero() {
		return 0, fmt.Errorf("seenBefore is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	started := time.Now()
	var rows int64
	err := s.guarded(ctx, func(q *retentiondb.Queries) error {
		var err error
		rows, err = q.PurgeStaleEventRelays(ctx, retentiondb.PurgeStaleEventRelaysParams{
			SeenBefore: tsz(seenBefore.UTC()),
			RowLimit:   int32(limit),
		})
		return err
	})
	metrics.ObserveDBOperation("purge_stale_event_relays", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge stale event relays: %w", err)
	}
	return rows, nil
}
