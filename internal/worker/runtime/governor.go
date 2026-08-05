package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// GovernorStore is the slice of *store.PostgresStore the storage governor
// needs: read the database size, persist the computed pressure level, and
// perform immediate bounded retention drains when pressure escalates.
type GovernorStore interface {
	GetDatabaseBytes(ctx context.Context) (int64, error)
	UpsertStoragePressureState(ctx context.Context, level int, ratio float64, databaseBytes, capacityBytes int64) error
	PurgeExpiredEngagementEvents(ctx context.Context, createdBefore, deadGraceBefore time.Time, limit int) (int64, error)
	PurgeSupersededReplaceableEvents(ctx context.Context, supersededBefore, deadGraceBefore time.Time, limit int) (int64, error)
	PurgeProcessedDeletionEvents(ctx context.Context, createdBefore, deadGraceBefore time.Time, limit int) (int64, error)
	PurgeUntrustedAuthorEvents(ctx context.Context, olderThan, deadGraceBefore time.Time, limit int) (int64, error)
	PruneAuthorRecentEvents(ctx context.Context, olderThan time.Time, perAuthorCap, authorBatchLimit, deleteBatchLimit int) (int64, error)
	PurgeStaleEventRelays(ctx context.Context, seenBefore time.Time, limit int) (int64, error)
	PruneFilteredEventTags(ctx context.Context, limit int) (int64, error)
}

// GovernorQueue is the jobs-queue slice the governor uses for terminal job
// reclamation under pressure.
type GovernorQueue interface {
	PurgeTerminalJobs(ctx context.Context, succeededBefore, deadBefore time.Time, limit int) (int64, error)
}

// RunStorageGovernorLoop periodically computes the storage-pressure level from
// pg_database_size / configured capacity, persists it for cross-process
// consumers (ingestor, hydration handler), and publishes metrics.
//
// At or above the "aggressive" level it performs an immediate bounded drain of
// the existing retention targets (engagement/replaceable/deletion raw events
// and terminal jobs) to accelerate reclamation ahead of the per-loop tick.
// Canonical trusted/tracked data is never touched: the governor only triggers
// the same retention purges that already run on their own intervals.
//
// When capacity is unset (cfg.StoragePressure.CapacityBytes == 0) the loop
// still observes and reports the ratio as 0 / level normal, but takes no
// defensive action — shipping this is behavior-neutral until an operator sets a
// capacity budget.
func RunStorageGovernorLoop(
	ctx context.Context,
	log Logger,
	store GovernorStore,
	queue GovernorQueue,
	cfg config.WorkerConfig,
) {
	pressure := cfg.Shared.StoragePressure
	if pressure.RunInterval <= 0 {
		log.Error("storage_governor_invalid_config", "run_interval", pressure.RunInterval.String())
		return
	}
	if store == nil {
		log.Error("storage_governor_no_store")
		return
	}
	if !pressure.Enabled() {
		// Deliberately logged at error level: an unprotected disk is the
		// single most common way a self-hosted deployment degrades.
		log.Error(
			"storage_governor_observe_only",
			"hint", "STORAGE_PRESSURE_CAPACITY_BYTES is unset (0); the governor will report the database size but take no defensive action. Set it to ~80% of the volume backing Postgres to enable automatic retention drains and ingest backpressure.",
		)
	}
	log.Info(
		"storage_governor_enabled",
		"capacity_bytes", pressure.CapacityBytes,
		"acts", pressure.Enabled(),
		"run_interval", pressure.RunInterval.String(),
		"warn_percent", pressure.WarnPercent,
		"aggressive_percent", pressure.AggressivePercent,
		"disable_hydration_percent", pressure.DisableHydrationPercent,
		"pause_candidate_percent", pressure.PauseCandidatePercent,
	)

	runOnce := func() {
		dbBytes, err := store.GetDatabaseBytes(ctx)
		if err != nil {
			log.Error("storage_governor_db_size_failed", "error", err)
			return
		}
		ratio := pressure.Ratio(dbBytes)
		level := pressure.Resolve(ratio)
		metrics.SetStoragePressure(ratio, int(level))
		if err := store.UpsertStoragePressureState(ctx, int(level), ratio, dbBytes, pressure.CapacityBytes); err != nil {
			log.Error("storage_governor_persist_failed", "error", err)
		}
		log.Info(
			"storage_pressure",
			"level", int(level),
			"ratio", ratio,
			"database_bytes", dbBytes,
			"capacity_bytes", pressure.CapacityBytes,
		)
		if level >= config.PressureAggressive {
			drainUnderPressure(ctx, log, store, queue, cfg, level)
		}
	}

	runOnce()
	ticker := time.NewTicker(pressure.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

// drainUnderPressure runs one immediate bounded purge of each existing
// retention target. This is the same set of safe, rebuildable/operational
// deletions the dedicated loops perform; the governor just triggers them now
// instead of waiting for the next per-loop tick. Pruning order follows the
// canonical-last policy: operational queue exhaust and low-value raw engagement
// first, superseded replaceables and processed deletions next, never canonical
// trusted/tracked notes.
func drainUnderPressure(
	ctx context.Context,
	log Logger,
	store GovernorStore,
	queue GovernorQueue,
	cfg config.WorkerConfig,
	level config.StoragePressureLevel,
) {
	now := time.Now().UTC()

	if queue != nil && cfg.JobRetention.Enabled {
		if deleted, err := queue.PurgeTerminalJobs(
			ctx,
			now.Add(-cfg.JobRetention.SucceededMaxAge),
			now.Add(-cfg.JobRetention.DeadMaxAge),
			cfg.JobRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "jobs_terminal", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("jobs_terminal", deleted)
			log.Info("storage_governor_drained", "target", "jobs_terminal", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.EngagementRetention.Enabled {
		if deleted, err := store.PurgeExpiredEngagementEvents(
			ctx,
			now.Add(-cfg.EngagementRetention.MaxAge),
			now.Add(-cfg.EngagementRetention.DeadGrace),
			cfg.EngagementRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "engagement_events", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("engagement_events", deleted)
			log.Info("storage_governor_drained", "target", "engagement_events", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.ReplaceableRetention.Enabled {
		if deleted, err := store.PurgeSupersededReplaceableEvents(
			ctx,
			now.Add(-cfg.ReplaceableRetention.MinAge),
			now.Add(-cfg.ReplaceableRetention.DeadGrace),
			cfg.ReplaceableRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "replaceable_events", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("replaceable_events", deleted)
			log.Info("storage_governor_drained", "target", "replaceable_events", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.DeletionRetention.Enabled {
		if deleted, err := store.PurgeProcessedDeletionEvents(
			ctx,
			now.Add(-cfg.DeletionRetention.MaxAge),
			now.Add(-cfg.DeletionRetention.DeadGrace),
			cfg.DeletionRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "deletion_events", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("deletion_events", deleted)
			log.Info("storage_governor_drained", "target", "deletion_events", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.UntrustedAuthorRetention.Enabled {
		if deleted, err := store.PurgeUntrustedAuthorEvents(
			ctx,
			now.Add(-cfg.UntrustedAuthorRetention.MaxAge),
			now.Add(-cfg.UntrustedAuthorRetention.DeadGrace),
			cfg.UntrustedAuthorRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "untrusted_author_events", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("untrusted_author_events", deleted)
			log.Info("storage_governor_drained", "target", "untrusted_author_events", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.AuthorRecentRetention.Enabled {
		if deleted, err := store.PruneAuthorRecentEvents(
			ctx,
			now.Add(-cfg.AuthorRecentRetention.MaxAge),
			cfg.AuthorRecentRetention.PerAuthorCap,
			cfg.AuthorRecentRetention.AuthorBatchLimit,
			cfg.AuthorRecentRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "author_recent_events", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("author_recent_events", deleted)
			log.Info("storage_governor_drained", "target", "author_recent_events", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.EventRelaysRetention.Enabled {
		if deleted, err := store.PurgeStaleEventRelays(
			ctx,
			now.Add(-cfg.EventRelaysRetention.MaxAge),
			cfg.EventRelaysRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "event_relays", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("event_relays", deleted)
			log.Info("storage_governor_drained", "target", "event_relays", "deleted", deleted, "level", int(level))
		}
	}

	if cfg.EventTagsRetention.Enabled {
		if deleted, err := store.PruneFilteredEventTags(
			ctx,
			cfg.EventTagsRetention.DeleteBatchLimit,
		); err != nil {
			log.Error("storage_governor_drain_failed", "target", "event_tags", "error", err)
		} else if deleted > 0 {
			metrics.AddRetentionPurgedRows("event_tags", deleted)
			log.Info("storage_governor_drained", "target", "event_tags", "deleted", deleted, "level", int(level))
		}
	}
}
