package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestRunIncrementalStatsReconciliationLoop_NilHandlersLogsAndReturns(t *testing.T) {
	log := &recordingLogger{}
	RunIncrementalStatsReconciliationLoop(context.Background(), log, nil, config.WorkerIncrementalStatsReconciliationConfig{
		Enabled:    true,
		Interval:   time.Minute,
		SampleSize: 10,
	})
	if !log.sawError("incremental_stats_reconciliation_no_handlers") {
		t.Fatal("expected no-handlers error log")
	}
}

func TestRunIncrementalStatsReconciliationLoop_DisabledReturnsWithoutError(t *testing.T) {
	// A non-nil handlers value (nil pool) is fine: disabled config returns
	// before any DB access.
	handlers := derivation.NewHandlers(nil)

	t.Run("disabled", func(t *testing.T) {
		log := &recordingLogger{}
		RunIncrementalStatsReconciliationLoop(context.Background(), log, handlers, config.WorkerIncrementalStatsReconciliationConfig{Enabled: false})
		if len(log.errs) != 0 {
			t.Fatalf("disabled loop must not error, got %v", log.errs)
		}
	})

	t.Run("zero interval", func(t *testing.T) {
		log := &recordingLogger{}
		RunIncrementalStatsReconciliationLoop(context.Background(), log, handlers, config.WorkerIncrementalStatsReconciliationConfig{
			Enabled:    true,
			Interval:   0,
			SampleSize: 10,
		})
		if len(log.errs) != 0 {
			t.Fatalf("zero-interval loop must not error, got %v", log.errs)
		}
	})

	t.Run("zero sample size", func(t *testing.T) {
		log := &recordingLogger{}
		RunIncrementalStatsReconciliationLoop(context.Background(), log, handlers, config.WorkerIncrementalStatsReconciliationConfig{
			Enabled:    true,
			Interval:   time.Minute,
			SampleSize: 0,
		})
		if len(log.errs) != 0 {
			t.Fatalf("zero-sample-size loop must not error, got %v", log.errs)
		}
	})
}
