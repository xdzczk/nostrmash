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
}

type EventJobPayload = jobs.EventJobPayload

type MeilisearchSyncer interface {
	Enabled() bool
	SyncEvent(ctx context.Context, pool *pgxpool.Pool, eventID string) error
}

type HandlersOptions struct {
	MeiliClient MeilisearchSyncer
}

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return NewHandlersWithOptions(pool, HandlersOptions{})
}

func NewHandlersWithOptions(pool *pgxpool.Pool, options HandlersOptions) *Handlers {
	return &Handlers{
		pool:  pool,
		meili: options.MeiliClient,
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
		h.ProjectProfileDiscoveryStats,
		h.ProjectProfilePublicStats,
		// Heavy per-author analytics rebuild (author_activity_daily +
		// 5 windowed projections × 3 windows) runs out-of-band via the
		// author-analytics sweeper. The bundle just marks the affected
		// pubkeys as dirty so a single rebuild covers any number of
		// inbound events between sweeper cycles.
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
