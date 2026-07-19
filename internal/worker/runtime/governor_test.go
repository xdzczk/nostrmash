package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
)

func governorTestConfig() config.WorkerConfig {
	cfg := config.WorkerConfig{}
	cfg.Shared.StoragePressure = config.StoragePressureConfig{
		CapacityBytes:           1000,
		WarnPercent:             80,
		AggressivePercent:       90,
		DisableHydrationPercent: 95,
		PauseCandidatePercent:   98,
		RunInterval:             time.Minute,
	}
	// Enable the retention targets drainUnderPressure fans out to.
	cfg.JobRetention.Enabled = true
	cfg.JobRetention.SucceededMaxAge = time.Hour
	cfg.JobRetention.DeadMaxAge = time.Hour
	cfg.JobRetention.DeleteBatchLimit = 100
	cfg.EngagementRetention.Enabled = true
	cfg.EngagementRetention.DeleteBatchLimit = 100
	cfg.ReplaceableRetention.Enabled = true
	cfg.ReplaceableRetention.DeleteBatchLimit = 100
	cfg.DeletionRetention.Enabled = true
	cfg.DeletionRetention.DeleteBatchLimit = 100
	cfg.UntrustedAuthorRetention.Enabled = true
	cfg.UntrustedAuthorRetention.DeleteBatchLimit = 100
	cfg.AuthorRecentRetention.Enabled = true
	cfg.AuthorRecentRetention.DeleteBatchLimit = 100
	cfg.EventRelaysRetention.Enabled = true
	cfg.EventRelaysRetention.DeleteBatchLimit = 100
	return cfg
}

func TestRunStorageGovernorLoop_InvalidInterval(t *testing.T) {
	log := &recordingLogger{}
	cfg := governorTestConfig()
	cfg.Shared.StoragePressure.RunInterval = 0
	RunStorageGovernorLoop(context.Background(), log, newFakeGovernorStore(), &fakeGovernorQueue{}, cfg)
	if !log.sawError("storage_governor_invalid_config") {
		t.Fatal("expected invalid-config error log")
	}
}

func TestRunStorageGovernorLoop_NilStore(t *testing.T) {
	log := &recordingLogger{}
	RunStorageGovernorLoop(context.Background(), log, nil, &fakeGovernorQueue{}, governorTestConfig())
	if !log.sawError("storage_governor_no_store") {
		t.Fatal("expected no-store error log")
	}
}

func TestRunStorageGovernorLoop_NormalPressureNoDrain(t *testing.T) {
	log := &recordingLogger{}
	store := newFakeGovernorStore()
	store.dbBytes = 100 // 10% of 1000 => normal
	queue := &fakeGovernorQueue{}

	// Cancel immediately so the loop runs runOnce() exactly once then exits.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunStorageGovernorLoop(ctx, log, store, queue, governorTestConfig())

	if len(store.upserts) != 1 {
		t.Fatalf("expected exactly one pressure upsert, got %d", len(store.upserts))
	}
	if store.upserts[0].level != int(config.PressureNormal) {
		t.Fatalf("expected normal level, got %d", store.upserts[0].level)
	}
	if len(store.drainCalls) != 0 {
		t.Fatalf("expected no drains at normal pressure, got %v", store.drainCalls)
	}
	if queue.purgeCalls != 0 {
		t.Fatalf("expected no terminal-job purge at normal pressure, got %d", queue.purgeCalls)
	}
}

func TestRunStorageGovernorLoop_AggressivePressureDrains(t *testing.T) {
	log := &recordingLogger{}
	store := newFakeGovernorStore()
	store.dbBytes = 920 // 92% of 1000 => aggressive
	queue := &fakeGovernorQueue{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunStorageGovernorLoop(ctx, log, store, queue, governorTestConfig())

	if len(store.upserts) == 0 || store.upserts[0].level != int(config.PressureAggressive) {
		t.Fatalf("expected aggressive level upsert, got %+v", store.upserts)
	}
	for _, target := range []string{"engagement", "replaceable", "deletion", "untrusted", "author_recent", "event_relays"} {
		if store.drainCalls[target] == 0 {
			t.Fatalf("expected drain of %q under aggressive pressure; calls=%v", target, store.drainCalls)
		}
	}
	if queue.purgeCalls == 0 {
		t.Fatal("expected terminal-job purge under aggressive pressure")
	}
}

func TestDrainUnderPressure_RespectsDisabledTargets(t *testing.T) {
	log := &recordingLogger{}
	store := newFakeGovernorStore()
	queue := &fakeGovernorQueue{}
	cfg := governorTestConfig()
	// Disable everything except engagement.
	cfg.JobRetention.Enabled = false
	cfg.ReplaceableRetention.Enabled = false
	cfg.DeletionRetention.Enabled = false
	cfg.UntrustedAuthorRetention.Enabled = false
	cfg.AuthorRecentRetention.Enabled = false
	cfg.EventRelaysRetention.Enabled = false

	drainUnderPressure(context.Background(), log, store, queue, cfg, config.PressureAggressive)

	if store.drainCalls["engagement"] != 1 {
		t.Fatalf("expected engagement drain once, got %d", store.drainCalls["engagement"])
	}
	if len(store.drainCalls) != 1 {
		t.Fatalf("expected only engagement drained, got %v", store.drainCalls)
	}
	if queue.purgeCalls != 0 {
		t.Fatalf("expected no terminal purge when job retention disabled, got %d", queue.purgeCalls)
	}
}

func TestDrainUnderPressure_EngagementErrorDoesNotStopOthers(t *testing.T) {
	log := &recordingLogger{}
	store := newFakeGovernorStore()
	store.engagementErr = context.DeadlineExceeded
	queue := &fakeGovernorQueue{}

	drainUnderPressure(context.Background(), log, store, queue, governorTestConfig(), config.PressureAggressive)

	if !log.sawError("storage_governor_drain_failed") {
		t.Fatal("expected a drain-failed error log for engagement")
	}
	// Subsequent targets still run despite the engagement error.
	if store.drainCalls["replaceable"] == 0 || store.drainCalls["event_relays"] == 0 {
		t.Fatalf("expected later targets to still drain; calls=%v", store.drainCalls)
	}
}
