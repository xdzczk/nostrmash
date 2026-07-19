package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestSweeperLoops_NilHandlersLogAndReturn(t *testing.T) {
	ctx := context.Background()

	t.Run("author analytics", func(t *testing.T) {
		log := &recordingLogger{}
		RunAuthorAnalyticsSweeperLoop(ctx, log, nil, config.WorkerAuthorAnalyticsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 3)
		if !log.sawError("author_analytics_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})

	t.Run("meilisearch", func(t *testing.T) {
		log := &recordingLogger{}
		RunMeilisearchSweeperLoop(ctx, log, nil, config.WorkerMeilisearchSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 1)
		if !log.sawError("meilisearch_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})

	t.Run("profile stats", func(t *testing.T) {
		log := &recordingLogger{}
		RunProfileStatsSweeperLoop(ctx, log, nil, config.WorkerProfileStatsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 0)
		if !log.sawError("profile_stats_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})
}

func TestSweeperLoops_DisabledReturnWithoutError(t *testing.T) {
	ctx := context.Background()
	// A non-nil handlers value (nil pool) is fine: disabled config returns
	// before any DB access.
	handlers := derivation.NewHandlers(nil)

	t.Run("author analytics disabled", func(t *testing.T) {
		log := &recordingLogger{}
		RunAuthorAnalyticsSweeperLoop(ctx, log, handlers, config.WorkerAuthorAnalyticsSweeperConfig{Enabled: false}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("disabled sweeper must not error, got %v", log.errs)
		}
	})

	t.Run("meilisearch zero interval", func(t *testing.T) {
		log := &recordingLogger{}
		RunMeilisearchSweeperLoop(ctx, log, handlers, config.WorkerMeilisearchSweeperConfig{Enabled: true, Interval: 0, BatchSize: 10}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("zero-interval sweeper must not error, got %v", log.errs)
		}
	})

	t.Run("profile stats zero batch", func(t *testing.T) {
		log := &recordingLogger{}
		RunProfileStatsSweeperLoop(ctx, log, handlers, config.WorkerProfileStatsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 0}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("zero-batch sweeper must not error, got %v", log.errs)
		}
	})
}
