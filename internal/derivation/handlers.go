package derivation

import (
	"context"

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
