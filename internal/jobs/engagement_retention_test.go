package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeEngagementPurger struct {
	mu                  sync.Mutex
	results             []int64
	calls               int
	err                 error
	lastCreatedBefore   time.Time
	lastDeadGraceBefore time.Time
	lastLimit           int
}

func (f *fakeEngagementPurger) PurgeExpiredEngagementEvents(_ context.Context, createdBefore, deadGraceBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastCreatedBefore = createdBefore
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

func (f *fakeEngagementPurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func engagementCfg() EngagementRetentionConfig {
	return EngagementRetentionConfig{
		Enabled:          true,
		MaxAge:           14 * 24 * time.Hour,
		DeadGrace:        7 * 24 * time.Hour,
		RunInterval:      time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunEngagementRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeEngagementPurger{}
	log := &recorderLogger{}
	cfg := engagementCfg()
	cfg.Enabled = false

	RunEngagementRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("engagement_retention_disabled") != 1 {
		t.Fatal("expected engagement_retention_disabled log")
	}
}

func TestRunEngagementRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeEngagementPurger{}
	log := &recorderLogger{}
	cfg := engagementCfg()
	cfg.DeadGrace = 0

	RunEngagementRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunEngagementRetentionDrain_CutoffsAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeEngagementPurger{results: []int64{100, 100, 30}}
	log := &recorderLogger{}
	cfg := engagementCfg()

	before := time.Now().UTC()
	runEngagementRetentionDrain(context.Background(), log, purger, cfg)
	after := time.Now().UTC()

	// Drains 2 saturated batches + 1 below-limit batch in a single cycle.
	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, purger.lastLimit)
	}
	// createdBefore should be roughly now - MaxAge.
	wantCreated := before.Add(-cfg.MaxAge)
	if purger.lastCreatedBefore.Before(wantCreated.Add(-2*time.Second)) || purger.lastCreatedBefore.After(after.Add(-cfg.MaxAge).Add(2*time.Second)) {
		t.Fatalf("createdBefore %v not within expected window around %v", purger.lastCreatedBefore, wantCreated)
	}
	// deadGraceBefore should be roughly now - DeadGrace.
	wantGrace := before.Add(-cfg.DeadGrace)
	if purger.lastDeadGraceBefore.Before(wantGrace.Add(-2*time.Second)) || purger.lastDeadGraceBefore.After(after.Add(-cfg.DeadGrace).Add(2*time.Second)) {
		t.Fatalf("deadGraceBefore %v not within expected window around %v", purger.lastDeadGraceBefore, wantGrace)
	}
}

func TestRunEngagementRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeEngagementPurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runEngagementRetentionDrain(context.Background(), log, purger, engagementCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("engagement_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
}
