package runtime

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nip05"
	"github.com/xdzczk/nostrmash/internal/store"
)

// RunExpiredEventRetentionLoop deletes events past their NIP-40 expiration.
func RunExpiredEventRetentionLoop(ctx context.Context, log Logger, st *store.PostgresStore, cfg config.WorkerExpiredEventRetentionConfig) {
	if st == nil || !cfg.Enabled {
		log.Info("expired_event_retention_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 || cfg.Grace <= 0 {
		log.Error("expired_event_retention_invalid_config")
		return
	}
	log.Info("expired_event_retention_enabled", "grace", cfg.Grace.String(), "interval", cfg.RunInterval.String())
	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := st.PurgeExpiredNIP40Events(ctx, time.Now().UTC().Add(-cfg.Grace), cfg.DeleteBatchLimit)
			if err != nil {
				metrics.IncRetentionPurgeRun("expired_events", "error")
				log.Error("expired_event_retention_failed", "error", err)
				continue
			}
			metrics.IncRetentionPurgeRun("expired_events", "ok")
			metrics.AddRetentionPurgedRows("expired_events", deleted)
			if deleted > 0 {
				log.Info("expired_event_retention_purged", "deleted", deleted)
			}
		}
	}
}

// RunNIP05VerificationLoop checks due profile identifiers.
func RunNIP05VerificationLoop(ctx context.Context, log Logger, pool *pgxpool.Pool, cfg config.WorkerNIP05Config) {
	nip05.RunLoop(ctx, pool, nip05.Config{
		Enabled:         cfg.Enabled,
		Interval:        cfg.Interval,
		Batch:           cfg.Batch,
		Timeout:         cfg.Timeout,
		RecheckInterval: cfg.RecheckInterval,
	}, func(msg string, args ...any) {
		log.Info(msg, args...)
	})
}
