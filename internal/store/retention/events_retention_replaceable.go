package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// supersededReplaceableKinds are the replaceable kinds whose older versions
// carry no value once a newer version wins. Two families share this purge:
//   - non-parameterized (addressable by pubkey+kind, d_tag always ”):
//     profile metadata (0), contact/follow lists (3), relay lists (10002),
//     mute lists (10000), bookmark lists (10003).
//   - parameterized (addressable by pubkey+kind+d_tag): long-form articles
//     (30023), where each distinct d_tag is its own latest-wins address.
//
// Nostr semantics define all of these as latest-wins, so every superseded
// version is dead weight. The purge query derives each candidate's d_tag and
// joins replaceable_state on (pubkey, kind, d_tag), which reduces to d_tag=”
// for the non-parameterized family.
var supersededReplaceableKinds = []int{0, 3, 10000, 10002, 10003, 30023}

// PurgeSupersededReplaceableEvents deletes a bounded batch of raw replaceable
// events that have been strictly superseded by a newer winner recorded in
// replaceable_state. The current winner is never touched
// (replaceable_state.event_id points at it), and a newer event that has not
// yet been projected is also protected because it ranks above the recorded
// winner rather than below it.
//
// Cascade FKs (event_tags, event_relays, event_references,
// follower_edges, …) clean the dependent rows. The latest-version projections
// (contact_lists_latest, relay_lists_latest, profiles_latest, replaceable_state)
// all reference the winner, so deleting older versions leaves the read models
// intact.
//
// Safety guards:
//   - supersededBefore filters on events.first_seen_at, so a version that was
//     ingested recently (even if its author-claimed created_at is ancient,
//     e.g. during a backfill) is left alone until it has been stable for the
//     operator's grace window.
//   - A candidate is skipped while its derive_event_bundle job is pending or
//     running, and while that job is dead but was last updated after
//     deadGraceBefore. Past deadGraceBefore a permanently-dead derivation no
//     longer pins disk.
//
// The (kind, created_at) scan is served by idx_events_kind_created_at; the
// per-candidate d_tag derivation is a bounded lookup on event_tags by
// (event_id, tag_name) (matching the replaceable derivation's own d_tag rule);
// the replaceable_state lookup is a primary-key probe on (pubkey, kind, d_tag);
// the jobs lookup is served by the unique index on jobs.idempotency_key.
func (s *Retention) PurgeSupersededReplaceableEvents(
	ctx context.Context,
	supersededBefore time.Time,
	deadGraceBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	if supersededBefore.IsZero() {
		return 0, fmt.Errorf("supersededBefore is required")
	}
	if deadGraceBefore.IsZero() {
		return 0, fmt.Errorf("deadGraceBefore is required")
	}

	started := time.Now()
	rows, err := s.queries().PurgeSupersededReplaceableEvents(ctx, retentiondb.PurgeSupersededReplaceableEventsParams{
		Kinds:            int32Kinds(supersededReplaceableKinds),
		SupersededBefore: tsz(supersededBefore.UTC()),
		DeadGraceBefore:  tsz(deadGraceBefore.UTC()),
		RowLimit:         int32(limit),
	})
	metrics.ObserveDBOperation("purge_superseded_replaceable_events", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge superseded replaceable events: %w", err)
	}
	return rows, nil
}

// int32Kinds narrows the package-level int kind list to the []int32 the
// generated ANY($1::int[]) parameter expects.
func int32Kinds(kinds []int) []int32 {
	out := make([]int32, len(kinds))
	for i, k := range kinds {
		out[i] = int32(k)
	}
	return out
}
