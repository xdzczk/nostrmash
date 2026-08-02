package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
)

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
}

type Queue interface {
	ClaimAvailableForPool(ctx context.Context, workerID, workerPool string, limit int) ([]jobs.Job, error)
	CompleteJob(ctx context.Context, jobID int64, workerID string) error
	FailJob(ctx context.Context, jobID int64, workerID string, errMsg string, retryDelay time.Duration) (jobs.FailureResult, error)
	RecoverStaleRunningJobs(ctx context.Context, workerPool string, olderThan time.Time, limit int) (jobs.RecoveryResult, error)
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

type InvalidEventRetentionStore interface {
	PurgeInvalidEventsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	TrimInvalidEventPayloadsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// EngagementRetentionStore purges expired raw engagement events. Satisfied by
// *store.PostgresStore.
type EngagementRetentionStore interface {
	PurgeExpiredEngagementEvents(ctx context.Context, createdBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// ReplaceableRetentionStore purges superseded raw replaceable events
// (kinds 0/3/10002). Satisfied by *store.PostgresStore.
type ReplaceableRetentionStore interface {
	PurgeSupersededReplaceableEvents(ctx context.Context, supersededBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// DeletionRetentionStore purges processed raw deletion events (kind 5).
// Satisfied by *store.PostgresStore.
type DeletionRetentionStore interface {
	PurgeProcessedDeletionEvents(ctx context.Context, createdBefore time.Time, deadGraceBefore time.Time, limit int) (int64, error)
}

// UntrustedRetentionStore purges author-gated raw events, plus their derived
// links/hashtags, from authors outside trust_graph_snapshot. Satisfied by
// *store.PostgresStore.
type UntrustedRetentionStore interface {
	PurgeUntrustedAuthorEvents(ctx context.Context, olderThan time.Time, deadGraceBefore time.Time, limit int) (int64, error)
	PurgeUntrustedAuthorEventURLs(ctx context.Context, limit int) (int64, error)
	PurgeUntrustedAuthorEventHashtags(ctx context.Context, limit int) (int64, error)
}

// AuthorRecentRetentionStore bounds the author_recent_events projection.
// Satisfied by *store.PostgresStore.
type AuthorRecentRetentionStore interface {
	PruneAuthorRecentEvents(ctx context.Context, olderThan time.Time, perAuthorCap, authorBatchLimit, deleteBatchLimit int) (int64, error)
}

// SearchDocsRetentionStore grooms the search_documents projection.
// Satisfied by *store.PostgresStore.
type SearchDocsRetentionStore interface {
	GroomSearchDocuments(ctx context.Context, freshnessBefore time.Time, maxBodyChars, batchLimit int) (trimmed int64, pruned int64, err error)
}

// EventRelaysRetentionStore prunes stale event_relays provenance rows.
// Satisfied by *store.PostgresStore.
type EventRelaysRetentionStore interface {
	PurgeStaleEventRelays(ctx context.Context, seenBefore time.Time, limit int) (int64, error)
}

// AppliedStatDeltasRetentionStore prunes orphaned applied_stat_deltas ledger
// rows (see docs/design/incremental-author-stats.md). Satisfied by
// *store.PostgresStore.
type AppliedStatDeltasRetentionStore interface {
	PruneOrphanedAppliedStatDeltas(ctx context.Context, appliedBefore time.Time, limit int) (int64, error)
}

// TrustRetentionStore performs the durable trust-retention hook deletes.
// Satisfied by *store.PostgresStore.
type TrustRetentionStore interface {
	PurgeStaleTrustedDiscoveryCandidates(ctx context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error)
	PurgeIdleAccountStates(ctx context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error)
}

type ProcessJobFn func(jobCtx context.Context, job jobs.Job) error

type ClaimLoopFn func(
	ctx context.Context,
	log Logger,
	queue Queue,
	workerID string,
	workerPool string,
	batchSize int,
	concurrency int,
	pollInterval time.Duration,
	retryDelay time.Duration,
	processJob ProcessJobFn,
)
