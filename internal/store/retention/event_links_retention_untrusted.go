package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeUntrustedAuthorEventURLs deletes a bounded batch of event_urls rows
// whose author is absent from trust_graph_snapshot. This is the retroactive
// complement to the write-time gate in
// internal/derivation/projection_urls.go (added 2026-08-01): that gate stops
// new rows from being recorded for untrusted authors going forward, this
// purge reclaims the residue written before the gate existed (or from an
// author later dropped from the trust graph).
//
// Unlike PurgeUntrustedAuthorEvents, there is no age/dead-grace gating here:
// correctness comes entirely from trust_graph_snapshot membership, not row
// age, and event_urls/event_hashtags rows are independently re-derivable
// (re-running ProjectEventURLs/ProjectEventHashtags for the underlying event
// naturally reflects current trust status either way).
//
// Fail-safe: if trust_graph_snapshot is empty (never loaded, mid-rebuild, or
// the trust worker isn't running on this deployment), nothing is deleted.
// Otherwise an empty graph would classify every author as untrusted and wipe
// every link and hashtag project-wide.
func (s *Retention) PurgeUntrustedAuthorEventURLs(ctx context.Context, limit int) (int64, error) {
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
		rows, err = q.PurgeUntrustedAuthorEventURLs(ctx, int32(limit))
		return err
	})
	metrics.ObserveDBOperation("purge_untrusted_author_event_urls", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge untrusted author event urls: %w", err)
	}
	return rows, nil
}

// PurgeUntrustedAuthorEventHashtags is the event_hashtags counterpart to
// PurgeUntrustedAuthorEventURLs — see its doc comment for the fail-safe and
// no-age-gating rationale shared by both.
func (s *Retention) PurgeUntrustedAuthorEventHashtags(ctx context.Context, limit int) (int64, error) {
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
		rows, err = q.PurgeUntrustedAuthorEventHashtags(ctx, int32(limit))
		return err
	})
	metrics.ObserveDBOperation("purge_untrusted_author_event_hashtags", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge untrusted author event hashtags: %w", err)
	}
	return rows, nil
}
