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
		h.ProjectAuthorAnalytics,
		h.SyncMeilisearch,
	}
	for _, step := range steps {
		if err := step(ctx, eventID); err != nil {
			return err
		}
	}
	return nil
}

// lockPubkeyForWriteTx acquires a transaction-scoped PostgreSQL advisory lock
// keyed on the pubkey. It must be called at the very start of any
// transaction that performs heavy multi-row writes against per-author
// projection tables (author_activity_daily, author_engagement_stats,
// author_topic_stats, author_media_mix_stats, author_activity_windows,
// author_posting_patterns, profile_public_stats, ...).
//
// Without this lock, two workers processing different events from the same
// hot pubkey can enter the same DELETE/INSERT path concurrently and end up
// fighting for row-level locks for many minutes — observed in production as
// transactionid waits stretching past 10 minutes that effectively serialize
// the entire derivation pipeline on a handful of high-volume authors.
//
// The lock is transaction-scoped (pg_advisory_xact_lock), so it auto-releases
// on commit or rollback. It uses hashtextextended for a 64-bit key space to
// minimize collisions; a hash collision merely causes brief unnecessary
// serialization of two unrelated pubkeys, never a correctness issue.
func lockPubkeyForWriteTx(ctx context.Context, tx pgx.Tx, pubkey string) error {
	if pubkey == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, pubkey); err != nil {
		return fmt.Errorf("acquire per-pubkey write lock for %s: %w", pubkey, err)
	}
	return nil
}
