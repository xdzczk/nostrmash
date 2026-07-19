package retention

import (
	"context"
	"fmt"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention/retentiondb"
)

// PurgeStaleTrustedDiscoveryCandidates deletes trusted discovery candidate
// rows (notes and profiles) whose projection is stale: graph-qualified rows
// (min_hops IS NOT NULL) older than trustedBefore, unqualified rows older
// than untrustedBefore. Candidates are bulk-refreshed with every trust
// snapshot, so steady-state rows keep a fresh projected_at and survive; only
// rows a refresh no longer touches decay out. Both tables are rebuildable
// from the trust snapshot plus discovery stats.
func (s *Retention) PurgeStaleTrustedDiscoveryCandidates(
	ctx context.Context,
	trustedBefore time.Time,
	untrustedBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if trustedBefore.IsZero() || untrustedBefore.IsZero() {
		return 0, fmt.Errorf("trustedBefore and untrustedBefore are required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	q := s.queries()

	var total int64
	noteStarted := time.Now()
	noteDeleted, err := q.PurgeStaleTrustedNoteDiscoveryCandidates(ctx, retentiondb.PurgeStaleTrustedNoteDiscoveryCandidatesParams{
		TrustedBefore:   tsz(trustedBefore.UTC()),
		UntrustedBefore: tsz(untrustedBefore.UTC()),
		RowLimit:        int32(limit),
	})
	metrics.ObserveDBOperation("purge_stale_trusted_note_discovery_candidates", dbResultFromErr(err), time.Since(noteStarted))
	if err != nil {
		return 0, fmt.Errorf("purge stale trusted note discovery candidates: %w", err)
	}
	total += noteDeleted

	profileStarted := time.Now()
	profileDeleted, err := q.PurgeStaleTrustedProfileDiscoveryCandidates(ctx, retentiondb.PurgeStaleTrustedProfileDiscoveryCandidatesParams{
		TrustedBefore:   tsz(trustedBefore.UTC()),
		UntrustedBefore: tsz(untrustedBefore.UTC()),
		RowLimit:        int32(limit),
	})
	metrics.ObserveDBOperation("purge_stale_trusted_profile_discovery_candidates", dbResultFromErr(err), time.Since(profileStarted))
	if err != nil {
		return total, fmt.Errorf("purge stale trusted profile discovery candidates: %w", err)
	}
	return total + profileDeleted, nil
}

// PurgeIdleAccountStates deletes low-value account_states rows: accounts still
// in the 'unknown' or 'observed' lifecycle states, with no manual override,
// whose last observation is older than the trust-aware horizon (trustedBefore
// for pubkeys in trust_graph_snapshot, untrustedBefore otherwise). Deleted
// rows are recreated with default state on the next observation; only the
// observation count signal is lost, which is the accepted cost of bounding a
// table that otherwise grows one row per pubkey ever seen on the firehose.
// Accounts at 'candidate' or above are never touched.
func (s *Retention) PurgeIdleAccountStates(
	ctx context.Context,
	trustedBefore time.Time,
	untrustedBefore time.Time,
	limit int,
) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if trustedBefore.IsZero() || untrustedBefore.IsZero() {
		return 0, fmt.Errorf("trustedBefore and untrustedBefore are required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}

	started := time.Now()
	rows, err := s.queries().PurgeIdleAccountStates(ctx, retentiondb.PurgeIdleAccountStatesParams{
		TrustedBefore:   tsz(trustedBefore.UTC()),
		UntrustedBefore: tsz(untrustedBefore.UTC()),
		RowLimit:        int32(limit),
	})
	metrics.ObserveDBOperation("purge_idle_account_states", dbResultFromErr(err), time.Since(started))
	if err != nil {
		return 0, fmt.Errorf("purge idle account states: %w", err)
	}
	return rows, nil
}
