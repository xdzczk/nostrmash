package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/retention"
)

type fakeBridgedPurge struct {
	results []retention.BridgedPurgeResult
	calls   int
	cancel  context.CancelFunc
}

func (f *fakeBridgedPurge) PurgeBridgedEvents(_ context.Context, _ int) (retention.BridgedPurgeResult, error) {
	f.calls++
	if f.calls > len(f.results) {
		f.cancel()
		return retention.BridgedPurgeResult{}, nil
	}
	return f.results[f.calls-1], nil
}

type fakeProfileIndex struct {
	deleted [][]string
}

func (f *fakeProfileIndex) Enabled() bool { return true }
func (f *fakeProfileIndex) DeleteProfileDocuments(_ context.Context, pubkeys []string) error {
	f.deleted = append(f.deleted, pubkeys)
	return nil
}

func TestBridgedRetentionLoopExitsOnDisabledOrInvalidConfig(t *testing.T) {
	log := &recordingLogger{}
	RunBridgedEventRetentionLoop(context.Background(), log, nil, nil, config.WorkerBridgedEventRetentionConfig{})
	RunBridgedEventRetentionLoop(context.Background(), log, &store.PostgresStore{}, nil, config.WorkerBridgedEventRetentionConfig{
		Enabled: true,
	})
	if !log.sawInfo("bridged_event_retention_disabled") || !log.sawError("bridged_event_retention_invalid_config") {
		t.Fatalf("logs info=%v errs=%v", log.info, log.errs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunBridgedEventRetentionLoop(ctx, log, &store.PostgresStore{}, profileSearchIndex(nil), config.WorkerBridgedEventRetentionConfig{
		Enabled:          true,
		RunInterval:      time.Hour,
		BatchPause:       time.Second,
		DeleteBatchLimit: 10,
	})
	if !log.sawInfo("bridged_event_retention_enabled") {
		t.Fatal("expected enabled log before the cancelled loop returned")
	}
}

func TestBridgedRetentionLoopDrainsBacklogThenIdles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Two full batches, then a short one: the loop should continue after the
	// short pause twice and only then fall back to the (long) run interval,
	// where the fake cancels the context on the next call.
	fake := &fakeBridgedPurge{
		results: []retention.BridgedPurgeResult{
			{Deleted: 10},
			{Deleted: 10, OrphanedAuthors: []string{"a", "b"}},
			{Deleted: 3, OrphanedAuthors: []string{"c"}},
		},
		cancel: cancel,
	}
	index := &fakeProfileIndex{}
	log := &recordingLogger{}

	started := time.Now()
	done := make(chan struct{})
	go func() {
		RunBridgedEventRetentionLoop(ctx, log, fake, index, config.WorkerBridgedEventRetentionConfig{
			Enabled:          true,
			RunInterval:      50 * time.Millisecond,
			BatchPause:       time.Millisecond,
			DeleteBatchLimit: 10,
		})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not exit")
	}
	if fake.calls != 4 {
		t.Fatalf("calls = %d, want 4", fake.calls)
	}
	// Three purges separated by two short pauses plus one run interval.
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("loop did not wait the run interval after a short batch: %s", elapsed)
	}
	if !log.sawInfo("bridged_event_retention_purged") {
		t.Fatalf("expected purge log, got %v", log.info)
	}
	if len(index.deleted) != 2 || len(index.deleted[0]) != 2 || index.deleted[1][0] != "c" {
		t.Fatalf("profile document deletes = %v", index.deleted)
	}
}
