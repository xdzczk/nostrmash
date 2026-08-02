package jobs

import (
	"context"
	"time"
)

// Bounded metric labels for untrusted-author purge runs/rows (shared
// retention metric vectors in internal/metrics).
const (
	untrustedRetentionTarget         = "untrusted_author_events"
	untrustedRetentionURLsTarget     = "untrusted_author_event_urls"
	untrustedRetentionHashtagsTarget = "untrusted_author_event_hashtags"
)

// UntrustedAuthorRetentionPurger deletes bounded batches of author-gated
// rows (raw events, plus their derived links/hashtags) whose author is
// outside trust_graph_snapshot. Satisfied by *store.PostgresStore.
type UntrustedAuthorRetentionPurger interface {
	PurgeUntrustedAuthorEvents(ctx context.Context, olderThan time.Time, deadGraceBefore time.Time, limit int) (int64, error)
	// PurgeUntrustedAuthorEventURLs and PurgeUntrustedAuthorEventHashtags
	// are the retroactive complement to the write-time trust gate in
	// internal/derivation (projection_urls.go / projection_hashtags.go):
	// that gate stops new rows for untrusted authors going forward, these
	// reclaim rows written before the gate existed, or from an author
	// later dropped from the trust graph.
	PurgeUntrustedAuthorEventURLs(ctx context.Context, limit int) (int64, error)
	PurgeUntrustedAuthorEventHashtags(ctx context.Context, limit int) (int64, error)
}

// UntrustedAuthorRetentionConfig is the narrow projection of
// config.WorkerUntrustedAuthorRetentionConfig the loop needs.
type UntrustedAuthorRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// RunUntrustedAuthorRetentionLoop periodically purges author-gated raw events
// (kinds 1/4/5/9802/10000/10003/30023) from authors outside the trust graph,
// once older than MaxAge. The store-side purge is fail-safe when the trust
// graph snapshot has never loaded (it deletes nothing), so running this loop
// on a fresh deployment is harmless. Uses the shared auto-pacing drain: a
// saturated batch immediately re-runs after retentionCatchupPause. Blocks
// until ctx is done.
func RunUntrustedAuthorRetentionLoop(ctx context.Context, log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) {
	if !cfg.Enabled {
		log.Info("untrusted_retention_disabled")
		return
	}
	if cfg.MaxAge <= 0 || cfg.DeadGrace <= 0 || cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"untrusted_retention_invalid_config",
			"max_age", cfg.MaxAge.String(),
			"dead_grace", cfg.DeadGrace.String(),
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"untrusted_retention_enabled",
		"max_age", cfg.MaxAge.String(),
		"dead_grace", cfg.DeadGrace.String(),
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	runRetentionTicker(
		ctx, cfg.RunInterval,
		untrustedAuthorRetentionDrain(log, purger, cfg),
		untrustedAuthorEventURLsRetentionDrain(log, purger, cfg),
		untrustedAuthorEventHashtagsRetentionDrain(log, purger, cfg),
	)
}

func untrustedAuthorRetentionDrain(log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: untrustedRetentionTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "untrusted_retention_purged",
		failedEvent:  "untrusted_retention_purge_failed",
		catchupEvent: "untrusted_retention_catchup",
		purge: func(ctx context.Context) (int64, []any, error) {
			now := time.Now().UTC()
			olderThan := now.Add(-cfg.MaxAge)
			deadGraceBefore := now.Add(-cfg.DeadGrace)
			deleted, err := purger.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, []any{
				"older_than", olderThan.Format(time.RFC3339),
				"dead_grace_before", deadGraceBefore.Format(time.RFC3339),
			}, nil
		},
	}
}

func runUntrustedAuthorRetentionDrain(ctx context.Context, log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) {
	untrustedAuthorRetentionDrain(log, purger, cfg).run(ctx)
}

// untrustedAuthorEventURLsRetentionDrain and
// untrustedAuthorEventHashtagsRetentionDrain reuse the same
// Enabled/RunInterval/DeleteBatchLimit knobs as the raw-events drain above
// (MaxAge/DeadGrace don't apply — see PurgeUntrustedAuthorEventURLs's doc
// comment for why these two purges have no age gating).
func untrustedAuthorEventURLsRetentionDrain(log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: untrustedRetentionURLsTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "untrusted_retention_purged",
		failedEvent:  "untrusted_retention_purge_failed",
		catchupEvent: "untrusted_retention_catchup",
		staticFields: []any{"target", untrustedRetentionURLsTarget},
		purge: func(ctx context.Context) (int64, []any, error) {
			deleted, err := purger.PurgeUntrustedAuthorEventURLs(ctx, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, nil, nil
		},
	}
}

func untrustedAuthorEventHashtagsRetentionDrain(log RetentionLogger, purger UntrustedAuthorRetentionPurger, cfg UntrustedAuthorRetentionConfig) retentionDrain {
	return retentionDrain{
		log:          log,
		metricTarget: untrustedRetentionHashtagsTarget,
		batchLimit:   cfg.DeleteBatchLimit,
		purgedEvent:  "untrusted_retention_purged",
		failedEvent:  "untrusted_retention_purge_failed",
		catchupEvent: "untrusted_retention_catchup",
		staticFields: []any{"target", untrustedRetentionHashtagsTarget},
		purge: func(ctx context.Context) (int64, []any, error) {
			deleted, err := purger.PurgeUntrustedAuthorEventHashtags(ctx, cfg.DeleteBatchLimit)
			if err != nil {
				return 0, nil, err
			}
			return deleted, nil, nil
		},
	}
}
