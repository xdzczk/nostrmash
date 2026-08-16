package meili

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestShouldPacingCheckpoint(t *testing.T) {
	cases := []struct {
		batches int
		want    bool
	}{
		{0, false},
		{1, false},
		{4, false},
		{5, true},
		{6, false},
		{9, false},
		{10, true},
		{15, true},
	}
	for _, tc := range cases {
		got := shouldPacingCheckpoint(tc.batches)
		if got != tc.want {
			t.Errorf("shouldPacingCheckpoint(%d) = %v, want %v", tc.batches, got, tc.want)
		}
	}
}

// TestFullSyncPacerCheckpointNoOpWhenNoTask verifies a pacer that has never
// recorded a task (lastTaskUID == 0, e.g. every batch in the stream enqueued
// nothing) returns immediately without sleeping.
func TestFullSyncPacerCheckpointNoOpWhenNoTask(t *testing.T) {
	p := &fullSyncPacer{client: &Client{enabled: false}}
	start := time.Now()
	if err := p.checkpoint(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("expected checkpoint with no task to return immediately, took %s", elapsed)
	}
}

// TestFullSyncPacerCheckpointRespectsContextCancellation verifies that an
// already-canceled context short-circuits the pacing delay instead of
// blocking for fullSyncPacingDelay.
func TestFullSyncPacerCheckpointRespectsContextCancellation(t *testing.T) {
	p := &fullSyncPacer{client: &Client{enabled: false}, lastTaskUID: 42}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := p.checkpoint(ctx)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("expected checkpoint to return promptly on canceled context, took %s", elapsed)
	}
}

// TestFullSyncPacerRecordTaskCheckpointsAtInterval drives recordTask through
// several batches and confirms it only pauses (observable via elapsed time)
// once fullSyncPacingBatchInterval batches have been recorded.
func TestFullSyncPacerRecordTaskCheckpointsAtInterval(t *testing.T) {
	p := &fullSyncPacer{client: &Client{enabled: false}}
	ctx := context.Background()

	for i := 1; i < fullSyncPacingBatchInterval; i++ {
		start := time.Now()
		if err := p.recordTask(ctx, int64(i)); err != nil {
			t.Fatalf("unexpected error on batch %d: %v", i, err)
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("batch %d should not have paced-checkpointed, took %s", i, elapsed)
		}
	}

	start := time.Now()
	if err := p.recordTask(ctx, int64(fullSyncPacingBatchInterval)); err != nil {
		t.Fatalf("unexpected error at interval boundary: %v", err)
	}
	if elapsed := time.Since(start); elapsed < fullSyncPacingDelay {
		t.Fatalf("expected pacing delay of at least %s at interval boundary, took %s", fullSyncPacingDelay, elapsed)
	}
}
