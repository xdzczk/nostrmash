package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeReplaceablePurger struct {
	mu                   sync.Mutex
	results              []int64
	calls                int
	err                  error
	lastSupersededBefore time.Time
	lastDeadGraceBefore  time.Time
	lastLimit            int
}

func (f *fakeReplaceablePurger) PurgeSupersededReplaceableEvents(_ context.Context, supersededBefore, deadGraceBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSupersededBefore = supersededBefore
	f.lastDeadGraceBefore = deadGraceBefore
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

func (f *fakeReplaceablePurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func replaceableCfg() ReplaceableRetentionConfig {
	return ReplaceableRetentionConfig{
		Enabled:          true,
		MinAge:           24 * time.Hour,
		DeadGrace:        7 * 24 * time.Hour,
		RunInterval:      time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunReplaceableRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeReplaceablePurger{}
	log := &recorderLogger{}
	cfg := replaceableCfg()
	cfg.Enabled = false

	RunReplaceableRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("replaceable_retention_disabled") != 1 {
		t.Fatal("expected replaceable_retention_disabled log")
	}
}

func TestRunReplaceableRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeReplaceablePurger{}
	log := &recorderLogger{}
	cfg := replaceableCfg()
	cfg.MinAge = 0

	RunReplaceableRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunReplaceableRetentionDrain_CutoffsAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeReplaceablePurger{results: []int64{100, 100, 30}}
	log := &recorderLogger{}
	cfg := replaceableCfg()

	before := time.Now().UTC()
	runReplaceableRetentionDrain(context.Background(), log, purger, cfg)
	after := time.Now().UTC()

	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, purger.lastLimit)
	}
	wantSuperseded := before.Add(-cfg.MinAge)
	if purger.lastSupersededBefore.Before(wantSuperseded.Add(-2*time.Second)) || purger.lastSupersededBefore.After(after.Add(-cfg.MinAge).Add(2*time.Second)) {
		t.Fatalf("supersededBefore %v not within expected window around %v", purger.lastSupersededBefore, wantSuperseded)
	}
	wantGrace := before.Add(-cfg.DeadGrace)
	if purger.lastDeadGraceBefore.Before(wantGrace.Add(-2*time.Second)) || purger.lastDeadGraceBefore.After(after.Add(-cfg.DeadGrace).Add(2*time.Second)) {
		t.Fatalf("deadGraceBefore %v not within expected window around %v", purger.lastDeadGraceBefore, wantGrace)
	}
}

func TestRunReplaceableRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeReplaceablePurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runReplaceableRetentionDrain(context.Background(), log, purger, replaceableCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("replaceable_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
}
