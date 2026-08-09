package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
)

func recoveryConfig() config.WorkerJobRecoveryConfig {
	return config.WorkerJobRecoveryConfig{
		RunningTimeout:          15 * time.Minute,
		StaleRecoveryInterval:   time.Millisecond,
		StaleRecoveryBatchLimit: 100,
	}
}

func TestRunStaleRecoveryLoop_InvalidConfig(t *testing.T) {
	log := &recordingLogger{}
	cfg := recoveryConfig()
	cfg.StaleRecoveryBatchLimit = 0
	RunStaleRecoveryLoop(context.Background(), log, &fakeQueue{}, jobs.WorkerPoolDefault, cfg)
	if !log.sawError("stale_recovery_invalid_config") {
		t.Fatal("expected invalid-config error log")
	}
}

func TestRunStaleRecoveryLoop_RecoversThenExits(t *testing.T) {
	log := &recordingLogger{}
	queue := &fakeQueue{recoverResult: jobs.RecoveryResult{Recovered: 2, DeadLettered: 1}}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	RunStaleRecoveryLoop(ctx, log, queue, jobs.WorkerPoolDefault, recoveryConfig())

	if queue.recoverCalls == 0 {
		t.Fatal("expected at least one recovery tick")
	}
	if !log.sawInfo("stale_recovery_enabled") {
		t.Fatal("expected enabled info log")
	}
	if !log.sawInfo("stale_recovery_completed") {
		t.Fatal("expected completed info log when jobs are recovered")
	}
}

func TestRunRelayWindowSnapshotsLoop_NilHandlers(t *testing.T) {
	log := &recordingLogger{}
	RunRelayWindowSnapshotsLoop(context.Background(), log, nil)
	if !log.sawError("relay_window_snapshots_no_handlers") {
		t.Fatal("expected no-handlers error log")
	}
}

// TestRefreshRelayWindowSnapshotsOnce_UninitializedHandlersDoesNotPanic
// covers the failure path that left the homepage silently serving a 3-day-
// old snapshot: refreshRelayWindowSnapshotsOnce must always log a clear
// error and return — never panic and take the rest of the worker's
// background loops down with it — regardless of whether the failure comes
// from the refresh itself or from the staleness-metric query that runs
// afterward.
func TestRefreshRelayWindowSnapshotsOnce_UninitializedHandlersDoesNotPanic(t *testing.T) {
	log := &recordingLogger{}
	handlers := derivation.NewHandlers(nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("refreshRelayWindowSnapshotsOnce panicked: %v", r)
		}
	}()
	refreshRelayWindowSnapshotsOnce(context.Background(), log, handlers)

	if !log.sawError("relay_window_snapshots_refresh_failed") {
		t.Fatal("expected refresh-failed error log")
	}
	if !log.sawError("relay_window_snapshots_age_query_failed") {
		t.Fatal("expected age-query-failed error log")
	}
	// Permanent wiring errors must not burn the retry delay.
	failed := 0
	for _, msg := range log.errs {
		if msg == "relay_window_snapshots_refresh_failed" {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("expected a single refresh attempt for uninitialized handlers, got %d", failed)
	}
}

func TestRefreshRelayWindowSnapshotsWithRetry_RetriesTransientFailure(t *testing.T) {
	oldDelay := relayWindowSnapshotsRefreshRetryDelay
	relayWindowSnapshotsRefreshRetryDelay = time.Millisecond
	defer func() { relayWindowSnapshotsRefreshRetryDelay = oldDelay }()

	log := &recordingLogger{}
	attempts := 0
	err := refreshRelayWindowSnapshotsWithRetry(context.Background(), log, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected retry to succeed, got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
	if !log.sawError("relay_window_snapshots_refresh_failed") {
		t.Fatal("expected first-attempt failure log")
	}
	if !log.sawInfo("relay_window_snapshots_refreshed") {
		t.Fatal("expected success log on retry")
	}
}

func TestShouldRetryRelayWindowSnapshotRefresh(t *testing.T) {
	ctx := context.Background()
	if shouldRetryRelayWindowSnapshotRefresh(ctx, nil) {
		t.Fatal("nil error should not retry")
	}
	if !shouldRetryRelayWindowSnapshotRefresh(ctx, context.DeadlineExceeded) {
		t.Fatal("transient deadline errors should retry")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if shouldRetryRelayWindowSnapshotRefresh(canceled, context.DeadlineExceeded) {
		t.Fatal("canceled parent should not retry")
	}
	if shouldRetryRelayWindowSnapshotRefresh(ctx, errHandlersNotInitialized()) {
		t.Fatal("uninitialized handlers should not retry")
	}
}

func errHandlersNotInitialized() error {
	return derivation.NewHandlers(nil).RefreshRelayWindowSnapshots(context.Background())
}

func TestRunMeilisearchStartupSync_NilClientReturns(t *testing.T) {
	log := &recordingLogger{}
	// Nil client and nil pool: must return immediately without logging or panic.
	RunMeilisearchStartupSync(context.Background(), log, nil, nil)
	if len(log.info) != 0 || len(log.errs) != 0 {
		t.Fatalf("expected no logs for nil client/pool, got info=%v errs=%v", log.info, log.errs)
	}
}

func TestRunQueueAndRebuildMetricsReporter_NilPoolReturns(t *testing.T) {
	log := &recordingLogger{}
	RunQueueAndRebuildMetricsReporter(context.Background(), log, nil, []string{"default"}, time.Minute)
	// Nil pool returns immediately; nothing to assert beyond no panic.
	if len(log.errs) != 0 {
		t.Fatalf("expected no error logs for nil pool, got %v", log.errs)
	}
}
