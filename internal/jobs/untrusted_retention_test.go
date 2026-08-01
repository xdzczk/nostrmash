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

	// urlsCalls/hashtagsCalls track the two link-purge drains separately
	// from the raw-events drain above; both always return 0 rows deleted
	// unless a test overrides urlsResults/hashtagsResults, so they never
	// interfere with the events-drain pacing assertions.
	urlsCalls       int
	urlsResults     []int64
	urlsErr         error
	hashtagsCalls   int
	hashtagsResults []int64
	hashtagsErr     error
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

func (f *fakeUntrustedPurger) PurgeUntrustedAuthorEventURLs(_ context.Context, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.urlsCalls++
	if f.urlsErr != nil {
		return 0, f.urlsErr
	}
	if len(f.urlsResults) == 0 {
		return 0, nil
	}
	n := f.urlsResults[0]
	f.urlsResults = f.urlsResults[1:]
	if n > int64(limit) {
		n = int64(limit)
	}
	return n, nil
}

func (f *fakeUntrustedPurger) PurgeUntrustedAuthorEventHashtags(_ context.Context, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hashtagsCalls++
	if f.hashtagsErr != nil {
		return 0, f.hashtagsErr
	}
	if len(f.hashtagsResults) == 0 {
		return 0, nil
	}
	n := f.hashtagsResults[0]
	f.hashtagsResults = f.hashtagsResults[1:]
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

func (f *fakeUntrustedPurger) urlsCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.urlsCalls
}

func (f *fakeUntrustedPurger) hashtagsCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hashtagsCalls
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

// TestUntrustedAuthorEventURLsRetentionDrain_PacesAndStopsOnError verifies
// the event_urls link-purge drain has the same auto-pacing/error-stop
// behavior as the raw-events drain, just without cutoff timestamps.
func TestUntrustedAuthorEventURLsRetentionDrain_PacesAndStopsOnError(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeUntrustedPurger{urlsResults: []int64{100, 100, 30}}
	log := &recorderLogger{}
	cfg := untrustedCfg()

	untrustedAuthorEventURLsRetentionDrain(log, purger, cfg).run(context.Background())

	if got := purger.urlsCallCount(); got != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", got)
	}

	purger = &fakeUntrustedPurger{urlsErr: errors.New("db down")}
	untrustedAuthorEventURLsRetentionDrain(log, purger, cfg).run(context.Background())
	if got := purger.urlsCallCount(); got != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", got)
	}
}

// TestUntrustedAuthorEventHashtagsRetentionDrain_PacesAndStopsOnError is the
// event_hashtags counterpart to the URLs drain test above.
func TestUntrustedAuthorEventHashtagsRetentionDrain_PacesAndStopsOnError(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeUntrustedPurger{hashtagsResults: []int64{100, 100, 30}}
	log := &recorderLogger{}
	cfg := untrustedCfg()

	untrustedAuthorEventHashtagsRetentionDrain(log, purger, cfg).run(context.Background())

	if got := purger.hashtagsCallCount(); got != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", got)
	}

	purger = &fakeUntrustedPurger{hashtagsErr: errors.New("db down")}
	untrustedAuthorEventHashtagsRetentionDrain(log, purger, cfg).run(context.Background())
	if got := purger.hashtagsCallCount(); got != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", got)
	}
}

// TestRunUntrustedAuthorRetentionLoop_RunsAllThreeDrains verifies a single
// enabled loop drives the raw-events drain and both link-purge drains on
// the shared ticker, not just the original events drain.
func TestRunUntrustedAuthorRetentionLoop_RunsAllThreeDrains(t *testing.T) {
	purger := &fakeUntrustedPurger{}
	log := &recorderLogger{}
	cfg := untrustedCfg()
	cfg.RunInterval = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		RunUntrustedAuthorRetentionLoop(ctx, log, purger, cfg)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if purger.callCount() > 0 && purger.urlsCallCount() > 0 && purger.hashtagsCallCount() > 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if purger.callCount() == 0 {
		t.Fatal("expected the raw-events drain to run")
	}
	if purger.urlsCallCount() == 0 {
		t.Fatal("expected the event_urls drain to run")
	}
	if purger.hashtagsCallCount() == 0 {
		t.Fatal("expected the event_hashtags drain to run")
	}
}
