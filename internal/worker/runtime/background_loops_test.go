package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
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
