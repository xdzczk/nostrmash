package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestWaitForWorkWakesOnEnqueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	queue := jobs.NewQueue(pool)

	// Start the LISTEN connection (first Wait times out quickly once listening).
	queue.WaitForWork(ctx, 50*time.Millisecond)

	waited := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		queue.WaitForWork(ctx, 2*time.Second)
		waited <- time.Since(start)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for i := 0; ; i++ {
		select {
		case d := <-waited:
			if d > 750*time.Millisecond {
				t.Fatalf("WaitForWork took %v; expected a notify wake well under the 2s poll cap", d)
			}
			return
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("WaitForWork did not return after enqueue")
		}
		if _, err := queue.Enqueue(ctx, jobs.EnqueueParams{
			JobType:        "derive_profile",
			Payload:        []byte(`{"event_id":"notify-test"}`),
			IdempotencyKey: "notify-test",
			MaxAttempts:    1,
			RunAfter:       time.Now().UTC().Add(-time.Second),
		}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
		_ = i
	}
}
