package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeUntrustedPurger struct {
	mu                  sync.Mutex
	results             []int64
	calls               int
	err                 error
	lastOlderThan       time.Time
	lastDeadGraceBefore time.Time
	lastLimit           int
}

func (f *fakeUntrustedPurger) PurgeUntrustedAuthorEvents(_ context.Context, olderThan, deadGraceBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastOlderThan = olderThan
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

func (f *fakeUntrustedPurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func untrustedCfg() UntrustedAuthorRetentionConfig {
	return UntrustedAuthorRetentionConfig{
		Enabled:          true,
		MaxAge:           14 * 24 * time.Hour,
		DeadGrace:        7 * 24 * time.Hour,
		RunInterval:      time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunUntrustedAuthorRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeUntrustedPurger{}
	log := &recorderLogger{}
	cfg := untrustedCfg()
	cfg.Enabled = false

	RunUntrustedAuthorRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("untrusted_retention_disabled") != 1 {
		t.Fatal("expected untrusted_retention_disabled log")
	}
}

func TestRunUntrustedAuthorRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeUntrustedPurger{}
	log := &recorderLogger{}
	cfg := untrustedCfg()
	cfg.MaxAge = 0

	RunUntrustedAuthorRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunUntrustedAuthorRetentionDrain_CutoffsAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeUntrustedPurger{results: []int64{100, 100, 30}}
	log := &recorderLogger{}
	cfg := untrustedCfg()

	before := time.Now().UTC()
	runUntrustedAuthorRetentionDrain(context.Background(), log, purger, cfg)
	after := time.Now().UTC()

	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, purger.lastLimit)
	}
	wantOlder := before.Add(-cfg.MaxAge)
	if purger.lastOlderThan.Before(wantOlder.Add(-2*time.Second)) || purger.lastOlderThan.After(after.Add(-cfg.MaxAge).Add(2*time.Second)) {
		t.Fatalf("olderThan %v not within expected window around %v", purger.lastOlderThan, wantOlder)
	}
	wantGrace := before.Add(-cfg.DeadGrace)
	if purger.lastDeadGraceBefore.Before(wantGrace.Add(-2*time.Second)) || purger.lastDeadGraceBefore.After(after.Add(-cfg.DeadGrace).Add(2*time.Second)) {
		t.Fatalf("deadGraceBefore %v not within expected window around %v", purger.lastDeadGraceBefore, wantGrace)
	}
}

func TestRunUntrustedAuthorRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeUntrustedPurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runUntrustedAuthorRetentionDrain(context.Background(), log, purger, untrustedCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("untrusted_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
}
