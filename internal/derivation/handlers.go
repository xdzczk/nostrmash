package derivation

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

type Handlers struct {
	pool  *pgxpool.Pool
	meili MeilisearchSyncer
	// authorAnalyticsWindows is the per-instance window_days list the live
	// author-analytics sweeper rebuilds. Empty means use the package default.
	authorAnalyticsWindows []int
	// incrementalProfilePublicStats maintains note/reply/follower counters
	// with O(1) deltas instead of full COUNT(*) recomputes.
	incrementalProfilePublicStats bool
	// incrementalAuthorActivityDaily maintains author_activity_daily (and
	// fine-grained daily helpers) with O(1) deltas instead of multi-CTE
	// full-window rebuilds.
	incrementalAuthorActivityDaily bool
	// incrementalWindowedRollups rolls topics/media/hourly windows from the
	// fine-grained daily tables instead of scanning raw event tables.
	incrementalWindowedRollups bool
	// incrementalProfileDiscoveryStats rolls profile_discovery_stats 24h/7d
	// windows from author_activity_daily / author_hourly_activity /
	// follower_gains_daily instead of rescanning raw engagement tables.
	incrementalProfileDiscoveryStats bool
}

type EventJobPayload = jobs.EventJobPayload

type MeilisearchSyncer interface {
	Enabled() bool
	SyncEvent(ctx context.Context, pool *pgxpool.Pool, eventID string) error
	SyncEventsBatch(ctx context.Context, pool *pgxpool.Pool, eventIDs []string) error
}

type HandlersOptions struct {
	MeiliClient MeilisearchSyncer
	// AuthorAnalyticsWindows overrides the live author-analytics sweeper's
	// window_days list. Values outside the schema CHECK ({7, 30, 90}) are
	// dropped; an entirely-invalid list falls back to the package default.
	AuthorAnalyticsWindows []int
	// IncrementalProfilePublicStats enables O(1) profile_public_stats deltas.
	// When nil, defaults to enabled.
	IncrementalProfilePublicStats *bool
	// IncrementalAuthorActivityDaily enables O(1) author_activity_daily deltas.
	// When nil, defaults to enabled.
	IncrementalAuthorActivityDaily *bool
	// IncrementalWindowedRollups rolls windowed topic/media/hourly stats from
	// fine-grained daily tables. When nil, defaults to enabled.
	IncrementalWindowedRollups *bool
	// IncrementalProfileDiscoveryStats rolls profile_discovery_stats from
	// incremental daily/hourly tables. When nil, defaults to enabled.
	// Requires IncrementalAuthorActivityDaily + IncrementalProfilePublicStats
	// for complete inputs (enforced at worker config validation).
	IncrementalProfileDiscoveryStats *bool
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return NewHandlersWithOptions(pool, HandlersOptions{})
}

func boolOptionOrDefault(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}

func NewHandlersWithOptions(pool *pgxpool.Pool, options HandlersOptions) *Handlers {
	return &Handlers{
		pool:                             pool,
		meili:                            options.MeiliClient,
		authorAnalyticsWindows:           normalizeAuthorAnalyticsWindows(options.AuthorAnalyticsWindows),
		incrementalProfilePublicStats:    boolOptionOrDefault(options.IncrementalProfilePublicStats, true),
		incrementalAuthorActivityDaily:   boolOptionOrDefault(options.IncrementalAuthorActivityDaily, true),
		incrementalWindowedRollups:       boolOptionOrDefault(options.IncrementalWindowedRollups, true),
		incrementalProfileDiscoveryStats: boolOptionOrDefault(options.IncrementalProfileDiscoveryStats, true),
	}
}

// DeriveEventBundle runs low-cost per-event derivations as a single queued job.
func (h *Handlers) DeriveEventBundle(ctx context.Context, eventID string) error {
	steps := []func(context.Context, string) error{
		h.DeriveEventRelationships,
		h.UpdateReplaceableState,
		h.ProjectProfilesLatest,
		h.ProjectAuthorRecentEvent,
		h.ProjectEventHashtags,
		h.ProjectEventURLs,
		h.ProjectReplyCounts,
		h.ProjectReactionCounts,
		h.ProjectRepostCounts,
		h.ProjectReactionEvents,
		h.ProjectRepostEvents,
		h.ProjectDeletionEvents,
		h.ProjectContactListsLatest,
		h.ProjectRelayListsLatest,
		h.ProjectDMUnreadCounts,
		h.ProjectZapReceipts,
		h.UpdateThreadProjection,
		h.RepairUnresolvedReferences,
		h.ProjectNoteDiscoveryStats,
		// Apply O(1) incremental counter deltas for profile_public_stats and
		// author_activity_daily (plus fine-grained daily helpers). This is
		// the steady-state path; full recomputes remain available as a
		// rebuild/reconciliation backstop when incremental flags are off.
		h.ApplyIncrementalAuthorStats,
		// ProjectProfileDiscoveryStats still needs an out-of-band recompute
		// (and profile_public_stats does too when incremental is disabled).
		// kind=3 is skipped here because ProjectContactListsLatest already
		// marks the same affected set.
		h.MarkProfileStatsDirty,
		// Windowed author-analytics roll-ups still run out-of-band via the
		// author-analytics sweeper. When incremental author_activity_daily
		// is enabled the sweeper skips the expensive daily rebuild and only
		// rolls windows from the already-maintained daily tables.
		h.MarkAuthorAnalyticsDirty,
		// Meilisearch index sync (HTTP round-trip per event, bounded by
		// a 30s timeout) runs out-of-band via the meilisearch sweeper.
		// The bundle just records that the event needs indexing so a
		// transient Meili slowdown can never cap live-pool throughput
		// at live_concurrency * 2/min the way the inline sync did.
		h.MarkMeilisearchDirty,
	}
	for _, step := range steps {
		if err := step(ctx, eventID); err != nil {
			return err
		}
	}
	return nil
}

// Per-pubkey advisory lock namespaces. Locks are partitioned per
// projection so that the slow author-analytics rebuild (which can hold
// its lock for many seconds while running 9-CTE history aggregations)
// does not block much faster bundle-time updates to unrelated tables
// like profile_public_stats and profile_discovery_stats.
//
// Production observed exactly this failure mode after introducing a
// single shared lock: a sweeper holding the lock for hot pubkey X to
// rebuild author_activity_daily would block bundle workers trying to
// upsert the same pubkey's profile_public_stats row, even though the
// two operations write to disjoint tables and have no real conflict.
//
// Within a namespace the lock is still strictly per-pubkey, so the
// row-contention failure mode the lock was originally introduced to
// prevent (concurrent DELETE+INSERT chains on author_activity_daily)
// remains addressed.
const (
	pubkeyLockNamespaceAuthorAnalytics       = "author_analytics"
	pubkeyLockNamespaceProfilePublicStats    = "profile_public_stats"
	pubkeyLockNamespaceProfileDiscoveryStats = "profile_discovery_stats"
)

// lockPubkeyForWriteTx acquires a transaction-scoped PostgreSQL advisory
// lock keyed on (pubkey, namespace). It must be called at the very start
// of any transaction that performs heavy multi-row writes against
// per-author projection tables.
//
// Without this lock, two workers processing different events from the
// same hot pubkey can enter the same DELETE/INSERT path concurrently and
// end up fighting for row-level locks for many minutes — observed in
// production as transactionid waits stretching past 10 minutes that
// effectively serialize the entire derivation pipeline on a handful of
// high-volume authors.
//
// The lock is transaction-scoped (pg_advisory_xact_lock), so it
// auto-releases on commit or rollback. The 64-bit lock key is computed
// as hashtextextended(pubkey) XOR hashtextextended(namespace, seed=1) so
// different namespaces yield independent key spaces without any string
// concatenation — concatenation broke in production because Postgres
// rejects NUL bytes (0x00) inside TEXT parameters with SQLSTATE 22021,
// failing every bundle that touched a per-pubkey lock. A hash collision
// merely causes brief unnecessary serialization of two unrelated keys,
// never a correctness issue.
func lockPubkeyForWriteTx(ctx context.Context, tx pgx.Tx, pubkey, namespace string) error {
	if pubkey == "" {
		return nil
	}
	if namespace == "" {
		return fmt.Errorf("lockPubkeyForWriteTx: namespace is required")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtextextended($1, 0) # hashtextextended($2, 1)
		)
	`, pubkey, namespace); err != nil {
		return fmt.Errorf("acquire per-pubkey write lock for %s ns=%s: %w", pubkey, namespace, err)
	}
	return nil
}
