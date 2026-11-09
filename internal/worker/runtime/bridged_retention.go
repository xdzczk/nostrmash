package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/meili"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/retention"
)

// BridgedEventRetentionStore is the purge the loop below drives. Satisfied by
// *store.PostgresStore.
type BridgedEventRetentionStore interface {
	PurgeBridgedEvents(ctx context.Context, limit int) (retention.BridgedPurgeResult, error)
}

// ProfileSearchIndex removes profile documents for authors whose last event
// was purged. Satisfied by *meili.Client; nil skips the step.
type ProfileSearchIndex interface {
	Enabled() bool
	DeleteProfileDocuments(ctx context.Context, pubkeys []string) error
}

// profileSearchIndex adapts the optional Meilisearch client so a nil pointer
// becomes a nil interface instead of a typed nil.
func profileSearchIndex(client *meili.Client) ProfileSearchIndex {
	if client == nil {
		return nil
	}
	return client
}

// RunBridgedEventRetentionLoop deletes stored events that carry a NIP-48
// proxy tag. The ingestor no longer admits them, so this is a backlog drain:
// while batches come back full it keeps going after a short pause, and once a
// batch is short it waits RunInterval before probing again.
func RunBridgedEventRetentionLoop(ctx context.Context, log Logger, st BridgedEventRetentionStore, search ProfileSearchIndex, cfg config.WorkerBridgedEventRetentionConfig) {
	if st == nil || !cfg.Enabled {
		log.Info("bridged_event_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.BatchPause <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error("bridged_event_retention_invalid_config")
		return
	}
	log.Info(
		"bridged_event_retention_enabled",
		"run_interval", cfg.RunInterval.String(),
		"batch_pause", cfg.BatchPause.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)
	for {
		result, err := st.PurgeBridgedEvents(ctx, cfg.DeleteBatchLimit)
		wait := cfg.RunInterval
		switch {
		case err != nil:
			if ctx.Err() != nil {
				return
			}
			metrics.IncRetentionPurgeRun("bridged_events", "error")
			log.Error("bridged_event_retention_failed", "error", err)
			// A deadlock or lock timeout already retried inside the purge.
			// Pause briefly and take the next batch instead of waiting out
			// the idle interval while bridged rows are still queued.
			if retention.LockConflict(err) {
				wait = cfg.BatchPause
			}
		default:
			metrics.IncRetentionPurgeRun("bridged_events", "ok")
			metrics.AddRetentionPurgedRows("bridged_events", result.Deleted)
			if result.Deleted > 0 {
				log.Info("bridged_event_retention_purged", "deleted", result.Deleted, "orphaned_authors", len(result.OrphanedAuthors))
			}
			if len(result.OrphanedAuthors) > 0 && search != nil && search.Enabled() {
				if err := search.DeleteProfileDocuments(ctx, result.OrphanedAuthors); err != nil {
					if ctx.Err() != nil {
						return
					}
					// Postgres is already clean; the profiles index catches up
					// on the next full sync if this keeps failing.
					log.Error("bridged_event_retention_search_delete_failed", "error", err, "pubkeys", len(result.OrphanedAuthors))
				}
			}
			if result.Deleted >= int64(cfg.DeleteBatchLimit) {
				wait = cfg.BatchPause
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}
