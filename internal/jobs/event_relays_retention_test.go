package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeEventRelaysPurger struct {
	mu             sync.Mutex
	results        []int64
	calls          int
	err            error
	lastSeenBefore time.Time
	lastLimit      int
}

func (f *fakeEventRelaysPurger) PurgeStaleEventRelays(_ context.Context, seenBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastSeenBefore = seenBefore
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

func (f *fakeEventRelaysPurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func eventRelaysCfg() EventRelaysRetentionConfig {
	return EventRelaysRetentionConfig{
		Enabled:          true,
		MaxAge:           180 * 24 * time.Hour,
		RunInterval:      6 * time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunEventRelaysRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeEventRelaysPurger{}
	log := &recorderLogger{}
	cfg := eventRelaysCfg()
	cfg.Enabled = false

	RunEventRelaysRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("event_relays_retention_disabled") != 1 {
		t.Fatal("expected event_relays_retention_disabled log")
	}
}

func TestRunEventRelaysRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeEventRelaysPurger{}
	log := &recorderLogger{}
	cfg := eventRelaysCfg()
	cfg.MaxAge = 0

	RunEventRelaysRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunEventRelaysRetentionDrain_CutoffAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeEventRelaysPurger{results: []int64{100, 100, 5}}
	log := &recorderLogger{}
	cfg := eventRelaysCfg()

	before := time.Now().UTC()
	runEventRelaysRetentionDrain(context.Background(), log, purger, cfg)
	after := time.Now().UTC()

	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, purger.lastLimit)
	}
	wantSeen := before.Add(-cfg.MaxAge)
	if purger.lastSeenBefore.Before(wantSeen.Add(-2*time.Second)) || purger.lastSeenBefore.After(after.Add(-cfg.MaxAge).Add(2*time.Second)) {
		t.Fatalf("seenBefore %v not within expected window around %v", purger.lastSeenBefore, wantSeen)
	}
}

func TestRunEventRelaysRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeEventRelaysPurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runEventRelaysRetentionDrain(context.Background(), log, purger, eventRelaysCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("event_relays_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
}
