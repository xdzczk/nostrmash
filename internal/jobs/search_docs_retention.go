package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

const (
	searchDocsTrimTarget  = "search_documents_body_trim"
	searchDocsPruneTarget = "search_documents_orphans"
)

// SearchDocsGroomer trims stale note bodies and prunes orphaned note
// documents in search_documents. Satisfied by *store.PostgresStore.
type SearchDocsGroomer interface {
	GroomSearchDocuments(ctx context.Context, freshnessBefore time.Time, maxBodyChars, batchLimit int) (trimmed int64, pruned int64, err error)
}

// SearchDocsRetentionConfig is the narrow projection of
// config.WorkerSearchDocsRetentionConfig the loop needs.
type SearchDocsRetentionConfig struct {
	Enabled      bool
	BodyMaxAge   time.Duration
	BodyMaxChars int
	RunInterval  time.Duration
	BatchLimit   int
}

// RunSearchDocsRetentionLoop periodically grooms search_documents: stale note
// bodies are trimmed (shrinking the generated tsvector and its GIN index) and
// orphaned note documents are pruned. Uses the shared auto-pacing drain.
// Blocks until ctx is done.
func RunSearchDocsRetentionLoop(ctx context.Context, log RetentionLogger, groomer SearchDocsGroomer, cfg SearchDocsRetentionConfig) {
	if !cfg.Enabled {
		log.Info("search_docs_retention_disabled")
		return
	}
	if cfg.BodyMaxAge <= 0 || cfg.BodyMaxChars <= 0 || cfg.RunInterval <= 0 || cfg.BatchLimit <= 0 {
		log.Error(
			"search_docs_retention_invalid_config",
			"body_max_age", cfg.BodyMaxAge.String(),
			"body_max_chars", cfg.BodyMaxChars,
			"run_interval", cfg.RunInterval.String(),
			"batch_limit", cfg.BatchLimit,
		)
		return
	}
	log.Info(
		"search_docs_retention_enabled",
		"body_max_age", cfg.BodyMaxAge.String(),
		"body_max_chars", cfg.BodyMaxChars,
		"run_interval", cfg.RunInterval.String(),
		"batch_limit", cfg.BatchLimit,
	)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		runSearchDocsRetentionDrain(ctx, log, groomer, cfg)
	}
}

func runSearchDocsRetentionDrain(ctx context.Context, log RetentionLogger, groomer SearchDocsGroomer, cfg SearchDocsRetentionConfig) {
	consecutiveSaturated := 0
	for {
		freshnessBefore := time.Now().UTC().Add(-cfg.BodyMaxAge)
		trimmed, pruned, err := groomer.GroomSearchDocuments(ctx, freshnessBefore, cfg.BodyMaxChars, cfg.BatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun(searchDocsTrimTarget, "error")
			log.Error("search_docs_retention_groom_failed", "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(searchDocsTrimTarget, "ok")
		metrics.AddRetentionPurgedRows(searchDocsTrimTarget, trimmed)
		metrics.AddRetentionPurgedRows(searchDocsPruneTarget, pruned)
		if trimmed > 0 || pruned > 0 {
			log.Info(
				"search_docs_retention_groomed",
				"trimmed", trimmed,
				"pruned", pruned,
				"freshness_before", freshnessBefore.Format(time.RFC3339),
			)
		}
		if int(trimmed) < cfg.BatchLimit && int(pruned) < cfg.BatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"search_docs_retention_catchup",
				"consecutive_full_batches", consecutiveSaturated,
				"batch_limit", cfg.BatchLimit,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retentionCatchupPause):
		}
	}
}
