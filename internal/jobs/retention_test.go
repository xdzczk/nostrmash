package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// fakePurger lets retention_test drive the loop without touching Postgres.
// Each call returns the next value from results (clamped to limit) and
// records the call so tests can assert the pacing pattern.
type fakePurger struct {
	mu      sync.Mutex
	results []int64
	calls   int
	err     error
}

func (f *fakePurger) PurgeTerminalJobs(_ context.Context, _, _ time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
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

func (f *fakePurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// recorderLogger captures structured log calls so tests can assert that
// catchup reporting fires at the expected cadence.
type recorderLogger struct {
	mu       sync.Mutex
	infoMsgs []string
	errMsgs  []string
}

func (r *recorderLogger) Info(msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infoMsgs = append(r.infoMsgs, msg)
}

func (r *recorderLogger) Error(msg string, _ ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errMsgs = append(r.errMsgs, msg)
}

func (r *recorderLogger) countInfo(msg string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, m := range r.infoMsgs {
		if m == msg {
			n++
		}
	}
	return n
}

// withShortCatchupPause shrinks the inter-batch courtesy pause so tests
// don't have to wait the production 100ms between batches. Restored on
// cleanup so other tests in the package keep the production value.
func withShortCatchupPause(t *testing.T, d time.Duration) {
	t.Helper()
	orig := retentionCatchupPause
	retentionCatchupPause = d
	t.Cleanup(func() { retentionCatchupPause = orig })
}

// TestRunRetentionLoop_DrainsSaturatedBatchesWithoutWaitingForTicker is the
// core auto-pacing assertion: when several consecutive ticks come back at
// the batch limit, the loop must keep firing without waiting RunInterval
// between them, and stop only when a batch returns below the limit. Without
// the auto-pacing change the loop would only call PurgeTerminalJobs once per
// ticker fire, so the four queued saturated results would take 4 *
// RunInterval to drain.
func TestRunRetentionLoop_DrainsSaturatedBatchesWithoutWaitingForTicker(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)

	purger := &fakePurger{
		results: []int64{100, 100, 100, 100, 50},
	}
	log := &recorderLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// RunInterval is intentionally long enough relative to the catchup
	// pause that we know the 5 expected calls all happen inside a single
	// ticker fire, not via separate fires. With pause=1ms and 5 batches,
	// the inner drain takes ~5ms; a 200ms interval means the next ticker
	// fire is far enough out that we can cancel cleanly first.
	cfg := RetentionConfig{
		Enabled:          true,
		SucceededMaxAge:  time.Hour,
		DeadMaxAge:       24 * time.Hour,
		RunInterval:      200 * time.Millisecond,
		DeleteBatchLimit: 100,
	}

	done := make(chan struct{})
	go func() {
		RunRetentionLoop(ctx, log, purger, cfg)
		close(done)
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if purger.callCount() >= 5 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	<-done

	if got := purger.callCount(); got < 5 {
		t.Fatalf("expected >=5 purge calls (4 saturated catchup + 1 below-limit) inside one ticker fire, got %d", got)
	}
	// Allow up to 7 in case a second ticker fire raced cancellation.
	if got := purger.callCount(); got > 7 {
		t.Fatalf("expected <=7 purge calls (single ticker fire + at most one extra), got %d", got)
	}
}

// TestRunRetentionLoop_StopsCatchupOnContextCancel guards against the
// catchup loop spinning forever when ctx is cancelled mid-burst.
func TestRunRetentionLoop_StopsCatchupOnContextCancel(t *testing.T) {
	withShortCatchupPause(t, 10*time.Millisecond)

	purger := &alwaysFullPurger{}
	log := &recorderLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	cfg := RetentionConfig{
		Enabled:          true,
		SucceededMaxAge:  time.Hour,
		DeadMaxAge:       24 * time.Hour,
		RunInterval:      10 * time.Second,
		DeleteBatchLimit: 100,
	}

	done := make(chan struct{})
	go func() {
		RunRetentionLoop(ctx, log, purger, cfg)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunRetentionLoop did not return promptly after context cancellation during catchup burst")
	}
}

// TestRunRetentionLoop_DisabledReturnsImmediately is a sanity guard: when
// Enabled=false the loop must not block its goroutine.
func TestRunRetentionLoop_DisabledReturnsImmediately(t *testing.T) {
	purger := &fakePurger{}
	log := &recorderLogger{}
	cfg := RetentionConfig{Enabled: false}

	done := make(chan struct{})
	go func() {
		RunRetentionLoop(context.Background(), log, purger, cfg)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunRetentionLoop with Enabled=false should return immediately")
	}
	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not call purger, got %d calls", purger.callCount())
	}
	if got := log.countInfo("job_retention_disabled"); got != 1 {
		t.Fatalf("expected one job_retention_disabled log, got %d", got)
	}
}

// TestRunRetentionLoop_ErrorReturnsToOuterTicker pins that a purge error
// breaks out of the catchup drain instead of looping tightly on the error.
// With RunInterval=100ms and a tiny catchup pause (1ms), a buggy
// retry-on-error implementation would produce ~250 calls in 250ms, while a
// correct implementation produces ~2-3 (one per ticker fire).
func TestRunRetentionLoop_ErrorReturnsToOuterTicker(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)

	purger := &fakePurger{err: errors.New("boom")}
	log := &recorderLogger{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := RetentionConfig{
		Enabled:          true,
		SucceededMaxAge:  time.Hour,
		DeadMaxAge:       24 * time.Hour,
		RunInterval:      100 * time.Millisecond,
		DeleteBatchLimit: 100,
	}

	done := make(chan struct{})
	go func() {
		RunRetentionLoop(ctx, log, purger, cfg)
		close(done)
	}()

	time.Sleep(250 * time.Millisecond)
	cancel()
	<-done

	// Expect roughly one error per ticker fire (~2-3 in 250ms). A
	// pathological tight-retry would be in the hundreds.
	got := purger.callCount()
	if got < 1 {
		t.Fatalf("expected at least one purge call within 250ms (RunInterval=100ms), got %d", got)
	}
	if got > 10 {
		t.Fatalf("expected ~2-3 purge calls (one per ticker fire), got %d -- error path is tight-retrying", got)
	}
	if errs := log.errMsgs; len(errs) == 0 {
		t.Fatalf("expected at least one job_retention_purge_failed error log, got none")
	}
}

// alwaysFullPurger always returns the full requested limit; used to
// simulate an unbounded backlog so the cancel-during-catchup test has
// something to catch.
type alwaysFullPurger struct{}

func (a *alwaysFullPurger) PurgeTerminalJobs(_ context.Context, _, _ time.Time, limit int) (int64, error) {
	return int64(limit), nil
}
