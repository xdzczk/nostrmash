package jobs

import (
	"context"
	"strings"
)

// Job type vocabulary for queue payload dispatch.
const (
	JobTypeDeriveEventBundle        = "derive_event_bundle"
	JobTypeDeriveEventRelationships = "derive_event_relationships"
	JobTypeUpdateReplaceableState   = "update_replaceable_state"
	JobTypeProjectProfilesLatest    = "project_profiles_latest"
	JobTypeProjectAuthorRecentEvent = "project_author_recent_event"
	JobTypeProjectReplyCounts       = "project_reply_counts"
	JobTypeProjectReactionCounts    = "project_reaction_counts"
	JobTypeProjectRepostCounts      = "project_repost_counts"
	JobTypeProjectReactionEvents    = "project_reaction_events"
	JobTypeProjectRepostEvents      = "project_repost_events"
	JobTypeProjectDeletionEvents    = "project_deletion_events"
	JobTypeProjectContactLists      = "project_contact_lists_latest"
	JobTypeProjectRelayLists        = "project_relay_lists_latest"
	JobTypeProjectDMUnreadCounts    = "project_dm_unread_counts"
	JobTypeProjectZapReceipts       = "project_zap_receipts"
	JobTypeUpdateThreadProjection   = "update_thread_projection"
	JobTypeRepairUnresolvedRefs     = "repair_unresolved_references"
	JobTypeRebuildProjectionScope   = "rebuild_projection_scope"

	JobTypeTrustSyncGraphRedis       = "trust_sync_graph_redis"
	JobTypeTrustComputeGlobalScore   = "trust_compute_global_scores"
	JobTypeTrustComputeNeighborhoods = "trust_compute_neighborhoods"
	JobTypeTrustPromoteRun           = "trust_promote_run"

	JobTypeHydrateAccount = "hydrate_account"
)

// HydrateAccountPayload is the payload for an on-demand account hydration job.
type HydrateAccountPayload struct {
	Pubkey      string `json:"pubkey"`
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requested_by,omitempty"`
}

const (
	WorkerPoolDefault  = "default"
	WorkerPoolLive     = "live"
	WorkerPoolBackfill = "backfill"
	WorkerPoolTrust    = "trust"
)

// IsKnownWorkerPool reports whether the supplied name matches one of the
// recognized worker pool constants.
func IsKnownWorkerPool(pool string) bool {
	switch strings.TrimSpace(pool) {
	case WorkerPoolDefault, WorkerPoolLive, WorkerPoolBackfill, WorkerPoolTrust:
		return true
	}
	return false
}

type workerPoolContextKey struct{}

// WithWorkerPool returns a derived context that pins the supplied worker pool
// for any downstream job enqueues that respect the override (notably
// PublishCanonicalEventJobsTx). Empty / unknown pool values are ignored so the
// publisher can fall back to the default routing rules.
func WithWorkerPool(ctx context.Context, pool string) context.Context {
	pool = strings.TrimSpace(pool)
	if pool == "" || !IsKnownWorkerPool(pool) {
		return ctx
	}
	return context.WithValue(ctx, workerPoolContextKey{}, pool)
}

// WorkerPoolFromContext returns the worker pool override stored in ctx (if
// any). Callers should fall back to WorkerPoolForJobType when ok is false.
func WorkerPoolFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	pool, ok := ctx.Value(workerPoolContextKey{}).(string)
	if !ok {
		return "", false
	}
	pool = strings.TrimSpace(pool)
	if pool == "" {
		return "", false
	}
	return pool, true
}

// EventJobPayload is the common event-scoped job payload shape.
type EventJobPayload struct {
	EventID string `json:"event_id"`
}

func WorkerPoolForJobType(jobType string) string {
	jobType = strings.TrimSpace(jobType)
	if strings.HasPrefix(jobType, "trust_") {
		return WorkerPoolTrust
	}
	return WorkerPoolDefault
}
