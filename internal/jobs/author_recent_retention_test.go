package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeAuthorRecentPruner struct {
	mu               sync.Mutex
	results          []int64
	calls            int
	err              error
	lastOlderThan    time.Time
	lastPerAuthorCap int
	lastAuthorBatch  int
	lastDeleteLimit  int
}

func (f *fakeAuthorRecentPruner) PruneAuthorRecentEvents(_ context.Context, olderThan time.Time, perAuthorCap, authorBatchLimit, deleteBatchLimit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastOlderThan = olderThan
	f.lastPerAuthorCap = perAuthorCap
	f.lastAuthorBatch = authorBatchLimit
	f.lastDeleteLimit = deleteBatchLimit
	if f.err != nil {
		return 0, f.err
	}
	if len(f.results) == 0 {
		return 0, nil
	}
	n := f.results[0]
	f.results = f.results[1:]
	return n, nil
}

func (f *fakeAuthorRecentPruner) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func authorRecentCfg() AuthorRecentRetentionConfig {
	return AuthorRecentRetentionConfig{
		Enabled:          true,
		MaxAge:           90 * 24 * time.Hour,
		PerAuthorCap:     200,
		AuthorBatchLimit: 500,
		RunInterval:      6 * time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunAuthorRecentRetentionLoop_DisabledNoOp(t *testing.T) {
	pruner := &fakeAuthorRecentPruner{}
	log := &recorderLogger{}
	cfg := authorRecentCfg()
	cfg.Enabled = false

	RunAuthorRecentRetentionLoop(context.Background(), log, pruner, cfg)

	if pruner.callCount() != 0 {
		t.Fatalf("disabled loop must not prune, got %d calls", pruner.callCount())
	}
	if log.countInfo("author_recent_retention_disabled") != 1 {
		t.Fatal("expected author_recent_retention_disabled log")
	}
}

func TestRunAuthorRecentRetentionLoop_InvalidConfig(t *testing.T) {
	pruner := &fakeAuthorRecentPruner{}
	log := &recorderLogger{}
	cfg := authorRecentCfg()
	cfg.PerAuthorCap = 0

	RunAuthorRecentRetentionLoop(context.Background(), log, pruner, cfg)

	if pruner.callCount() != 0 {
		t.Fatalf("invalid config must not prune, got %d calls", pruner.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunAuthorRecentRetentionDrain_ArgumentsAndPacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	pruner := &fakeAuthorRecentPruner{results: []int64{100, 100, 10}}
	log := &recorderLogger{}
	cfg := authorRecentCfg()

	before := time.Now().UTC()
	runAuthorRecentRetentionDrain(context.Background(), log, pruner, cfg)
	after := time.Now().UTC()

	if pruner.callCount() != 3 {
		t.Fatalf("expected 3 prune calls (2 saturated + 1 below limit), got %d", pruner.callCount())
	}
	if pruner.lastPerAuthorCap != cfg.PerAuthorCap || pruner.lastAuthorBatch != cfg.AuthorBatchLimit || pruner.lastDeleteLimit != cfg.DeleteBatchLimit {
		t.Fatalf("unexpected limits: cap=%d authorBatch=%d deleteLimit=%d", pruner.lastPerAuthorCap, pruner.lastAuthorBatch, pruner.lastDeleteLimit)
	}
	wantOlder := before.Add(-cfg.MaxAge)
	if pruner.lastOlderThan.Before(wantOlder.Add(-2*time.Second)) || pruner.lastOlderThan.After(after.Add(-cfg.MaxAge).Add(2*time.Second)) {
		t.Fatalf("olderThan %v not within expected window around %v", pruner.lastOlderThan, wantOlder)
	}
}

func TestRunAuthorRecentRetentionDrain_StopsOnError(t *testing.T) {
	pruner := &fakeAuthorRecentPruner{err: errors.New("db down")}
	log := &recorderLogger{}

	runAuthorRecentRetentionDrain(context.Background(), log, pruner, authorRecentCfg())

	if pruner.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", pruner.callCount())
	}
	if log.countInfo("author_recent_retention_pruned") != 0 {
		t.Fatal("must not log a prune on error")
	}
}
