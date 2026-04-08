package derivation

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

type Handlers struct {
	pool *pgxpool.Pool
}

type EventJobPayload = jobs.EventJobPayload

func NewHandlers(pool *pgxpool.Pool) *Handlers {
	return &Handlers{pool: pool}
}

// DeriveEventBundle runs low-cost per-event derivations as a single queued job.
func (h *Handlers) DeriveEventBundle(ctx context.Context, eventID string) error {
	steps := []func(context.Context, string) error{
		h.DeriveEventRelationships,
		h.UpdateReplaceableState,
		h.ProjectProfilesLatest,
		h.ProjectAuthorRecentEvent,
		h.ProjectEventHashtags,
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
		h.ProjectNoteDiscoveryStats,
		h.ProjectProfileDiscoveryStats,
		h.ProjectProfilePublicStats,
	}
	for _, step := range steps {
		if err := step(ctx, eventID); err != nil {
			return err
		}
	}
	return nil
}
