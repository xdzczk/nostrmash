package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeAppliedStatDeltasPurger struct {
	mu                sync.Mutex
	results           []int64
	calls             int
	err               error
	lastAppliedBefore time.Time
	lastLimit         int
}

func (f *fakeAppliedStatDeltasPurger) PruneOrphanedAppliedStatDeltas(_ context.Context, appliedBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastAppliedBefore = appliedBefore
	f.lastLimit = limit
	if f.err != nil {
		return 0, f.err
	}
	if len(f.results) == 0 {
		return 0, nil
	}
	n := f.results[0]
	f.results = f.results[1:]
	if n > int64(limit) {
		n = int64(limit)
	}
	return n, nil
}

func (f *fakeAppliedStatDeltasPurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func appliedStatDeltasCfg() AppliedStatDeltasRetentionConfig {
	return AppliedStatDeltasRetentionConfig{
		Enabled:          true,
		GracePeriod:      1 * time.Hour,
		RunInterval:      6 * time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunAppliedStatDeltasRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeAppliedStatDeltasPurger{}
	log := &recorderLogger{}
	cfg := appliedStatDeltasCfg()
	cfg.Enabled = false

	RunAppliedStatDeltasRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("applied_stat_deltas_retention_disabled") != 1 {
		t.Fatal("expected applied_stat_deltas_retention_disabled log")
	}
}

func TestRunAppliedStatDeltasRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeAppliedStatDeltasPurger{}
	log := &recorderLogger{}
	cfg := appliedStatDeltasCfg()
	cfg.GracePeriod = 0

	RunAppliedStatDeltasRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunAppliedStatDeltasRetentionDrain_CutoffAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeAppliedStatDeltasPurger{results: []int64{100, 100, 5}}
	log := &recorderLogger{}
	cfg := appliedStatDeltasCfg()

	before := time.Now().UTC()
	runAppliedStatDeltasRetentionDrain(context.Background(), log, purger, cfg)
	after := time.Now().UTC()

	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, purger.lastLimit)
	}
	wantAppliedBefore := before.Add(-cfg.GracePeriod)
	if purger.lastAppliedBefore.Before(wantAppliedBefore.Add(-2*time.Second)) || purger.lastAppliedBefore.After(after.Add(-cfg.GracePeriod).Add(2*time.Second)) {
		t.Fatalf("appliedBefore %v not within expected window around %v", purger.lastAppliedBefore, wantAppliedBefore)
	}
}

func TestRunAppliedStatDeltasRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeAppliedStatDeltasPurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runAppliedStatDeltasRetentionDrain(context.Background(), log, purger, appliedStatDeltasCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("applied_stat_deltas_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
}
