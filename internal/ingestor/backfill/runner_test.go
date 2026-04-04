package backfill

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestRunnerResumeFromPersistedCheckpointAfterFailure(t *testing.T) {
	t.Parallel()

	store := newMemoryCheckpointStore()
	fetcherA := &scriptedPageFetcher{
		steps: []pageStep{
			{
				result: PageResult{
					Events: [][]byte{
						[]byte(`{"created_at":100}`),
						[]byte(`{"created_at":90}`),
					},
				},
			},
			{err: errors.New("relay timeout")},
		},
	}

	runnerA := mustRunner(t, store, fetcherA)
	err := runnerA.Run(context.Background())
	if err == nil {
		t.Fatal("expected run to fail")
	}

	cp, _ := store.GetIngestCheckpoint(context.Background(), "wss://relay.one", model.ModeBackfill, "default_v1")
	if cp == nil {
		t.Fatal("expected checkpoint to be persisted")
	}
	if cp.Status != model.CheckpointFailed {
		t.Fatalf("status mismatch: got %q want %q", cp.Status, model.CheckpointFailed)
	}
	if cp.Cursor == nil || *cp.Cursor != "100" {
		t.Fatalf("cursor mismatch after failed run: got %v want 100", cp.Cursor)
	}

	fetcherB := &scriptedPageFetcher{
		steps: []pageStep{
			{result: PageResult{EOSESeen: true}},
		},
	}
	runnerB := mustRunner(t, store, fetcherB)
	if err := runnerB.Run(context.Background()); err != nil {
		t.Fatalf("resume run failed: %v", err)
	}

	if len(fetcherB.requests) == 0 || fetcherB.requests[0].Since == nil || *fetcherB.requests[0].Since != 101 {
		t.Fatalf("resume should continue from cursor+1, got %+v", fetcherB.requests)
	}
	cp, _ = store.GetIngestCheckpoint(context.Background(), "wss://relay.one", model.ModeBackfill, "default_v1")
	if cp.Status != model.CheckpointCompleted {
		t.Fatalf("status mismatch: got %q want %q", cp.Status, model.CheckpointCompleted)
	}
}

func TestRunnerCompletesWithEmptyPageFallbackWhenNoEOSE(t *testing.T) {
	t.Parallel()

	store := newMemoryCheckpointStore()
	fetcher := &scriptedPageFetcher{
		steps: []pageStep{
			{result: PageResult{}},
			{result: PageResult{}},
		},
	}
	runner := mustRunner(t, store, fetcher)
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	cp, _ := store.GetIngestCheckpoint(context.Background(), "wss://relay.one", model.ModeBackfill, "default_v1")
	if cp.Status != model.CheckpointCompleted {
		t.Fatalf("status mismatch: got %q want %q", cp.Status, model.CheckpointCompleted)
	}
}

func TestRunnerCursorMonotonicWithinRun(t *testing.T) {
	t.Parallel()

	store := newMemoryCheckpointStore()
	fetcher := &scriptedPageFetcher{
		steps: []pageStep{
			{
				result: PageResult{
					Events: [][]byte{
						[]byte(`{"created_at":50}`),
						[]byte(`{"created_at":40}`),
						[]byte(`{"created_at":55}`),
					},
				},
			},
			{result: PageResult{EOSESeen: true}},
		},
	}
	runner := mustRunner(t, store, fetcher)
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	cp, _ := store.GetIngestCheckpoint(context.Background(), "wss://relay.one", model.ModeBackfill, "default_v1")
	if cp.Cursor == nil || *cp.Cursor != "55" {
		t.Fatalf("cursor mismatch: got %v want 55", cp.Cursor)
	}
	if len(fetcher.requests) < 2 || fetcher.requests[1].Since == nil || *fetcher.requests[1].Since != 56 {
		t.Fatalf("expected follow-up since=56, got %+v", fetcher.requests)
	}
}

func mustRunner(t *testing.T, store *memoryCheckpointStore, fetcher *scriptedPageFetcher) *Runner {
	t.Helper()
	runner, err := NewRunner(
		slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Config{
			Relays:       []string{"wss://relay.one"},
			FilterGroup:  "default_v1",
			Kinds:        []int{1},
			Mode:         model.ModeBackfill,
			Since:        int64Ptr(0),
			PageLimit:    100,
			EmptyPageMax: 2,
		},
		store,
		fetcher,
		func(ctx context.Context, relayURL string, payload []byte) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	runner.nowFn = func() time.Time {
		return time.Date(2026, 4, 4, 15, 0, 0, 0, time.UTC)
	}
	return runner
}

type pageStep struct {
	result PageResult
	err    error
}

type scriptedPageFetcher struct {
	steps    []pageStep
	requests []PageRequest
}

func (f *scriptedPageFetcher) FetchPage(ctx context.Context, relayURL string, request PageRequest) (PageResult, error) {
	f.requests = append(f.requests, request)
	if len(f.steps) == 0 {
		return PageResult{}, fmt.Errorf("no scripted steps left")
	}
	step := f.steps[0]
	f.steps = f.steps[1:]
	return step.result, step.err
}

type memoryCheckpointStore struct {
	byKey map[string]model.IngestCheckpoint
}

func newMemoryCheckpointStore() *memoryCheckpointStore {
	return &memoryCheckpointStore{byKey: map[string]model.IngestCheckpoint{}}
}

func (s *memoryCheckpointStore) GetIngestCheckpoint(
	ctx context.Context,
	relayURL string,
	mode string,
	filterGroup string,
) (*model.IngestCheckpoint, error) {
	k := checkpointKey(relayURL, mode, filterGroup)
	cp, ok := s.byKey[k]
	if !ok {
		return nil, nil
	}
	cloned := cp
	return &cloned, nil
}

func (s *memoryCheckpointStore) UpsertIngestCheckpoint(ctx context.Context, checkpoint model.IngestCheckpoint) error {
	k := checkpointKey(checkpoint.RelayURL, checkpoint.Mode, checkpoint.FilterGroup)
	s.byKey[k] = checkpoint
	return nil
}

func checkpointKey(relayURL string, mode string, filterGroup string) string {
	return relayURL + "|" + mode + "|" + filterGroup
}

func int64Ptr(v int64) *int64 { return &v }
