package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestSweeperLoops_NilHandlersLogAndReturn(t *testing.T) {
	ctx := context.Background()

	t.Run("author analytics", func(t *testing.T) {
		log := &recordingLogger{}
		RunAuthorAnalyticsSweeperLoop(ctx, log, nil, config.WorkerAuthorAnalyticsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 3)
		if !log.sawError("author_analytics_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})

	t.Run("meilisearch", func(t *testing.T) {
		log := &recordingLogger{}
		RunMeilisearchSweeperLoop(ctx, log, nil, config.WorkerMeilisearchSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 1)
		if !log.sawError("meilisearch_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})

	t.Run("profile stats", func(t *testing.T) {
		log := &recordingLogger{}
		RunProfileStatsSweeperLoop(ctx, log, nil, config.WorkerProfileStatsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 10}, 0)
		if !log.sawError("profile_stats_sweeper_no_handlers") {
			t.Fatal("expected no-handlers error log")
		}
	})
}

func TestSweeperLoops_DisabledReturnWithoutError(t *testing.T) {
	ctx := context.Background()
	// A non-nil handlers value (nil pool) is fine: disabled config returns
	// before any DB access.
	handlers := derivation.NewHandlers(nil)

	t.Run("author analytics disabled", func(t *testing.T) {
		log := &recordingLogger{}
		RunAuthorAnalyticsSweeperLoop(ctx, log, handlers, config.WorkerAuthorAnalyticsSweeperConfig{Enabled: false}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("disabled sweeper must not error, got %v", log.errs)
		}
	})

	t.Run("meilisearch zero interval", func(t *testing.T) {
		log := &recordingLogger{}
		RunMeilisearchSweeperLoop(ctx, log, handlers, config.WorkerMeilisearchSweeperConfig{Enabled: true, Interval: 0, BatchSize: 10}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("zero-interval sweeper must not error, got %v", log.errs)
		}
	})

	t.Run("profile stats zero batch", func(t *testing.T) {
		log := &recordingLogger{}
		RunProfileStatsSweeperLoop(ctx, log, handlers, config.WorkerProfileStatsSweeperConfig{Enabled: true, Interval: time.Minute, BatchSize: 0}, 0)
		if len(log.errs) != 0 {
			t.Fatalf("zero-batch sweeper must not error, got %v", log.errs)
		}
	})
}

// TestRunWithBatchTimeout_HardCapsHungCall verifies the safety net that
// motivated WorkerMeilisearchSweeperConfig.BatchTimeout: a fn that never
// respects ctx.Done() (simulating the production hang) still returns once
// the configured timeout elapses, with an error naming that timeout.
func TestRunWithBatchTimeout_HardCapsHungCall(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	start := time.Now()
	_, err := runWithBatchTimeout(context.Background(), 20*time.Millisecond, func(callCtx context.Context) (int, error) {
		// Deliberately ignore callCtx, like a call stuck inside a
		// blocking dependency that doesn't propagate cancellation.
		<-block
		return 0, nil
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected timeout error from a call that never returns on its own")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected wrapped context.DeadlineExceeded, got %v", err)
	}
	if !strings.Contains(err.Error(), "20ms") {
		t.Fatalf("expected error to name the configured timeout, got %q", err.Error())
	}
	if elapsed > 2*time.Second {
		t.Fatalf("runWithBatchTimeout did not return promptly after deadline: took %s", elapsed)
	}
}

func TestRunWithBatchTimeout_ZeroDisablesWrapper(t *testing.T) {
	called := false
	processed, err := runWithBatchTimeout(context.Background(), 0, func(callCtx context.Context) (int, error) {
		called = true
		if callCtx.Err() != nil {
			t.Fatalf("expected unmodified context, got Err()=%v", callCtx.Err())
		}
		if _, ok := callCtx.Deadline(); ok {
			t.Fatal("expected no deadline when BatchTimeout <= 0")
		}
		return 5, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called || processed != 5 {
		t.Fatalf("expected fn to run and return its own result, called=%v processed=%d", called, processed)
	}
}

func TestRunWithBatchTimeout_PropagatesNonTimeoutError(t *testing.T) {
	wantErr := errors.New("boom")
	_, err := runWithBatchTimeout(context.Background(), time.Minute, func(callCtx context.Context) (int, error) {
		return 0, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected underlying error to pass through unwrapped, got %v", err)
	}
}
