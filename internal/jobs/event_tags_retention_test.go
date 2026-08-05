package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeEventTagsPurger struct {
	mu      sync.Mutex
	results []int64
	calls   int
	err     error
	lastLim int
}

func (f *fakeEventTagsPurger) PruneFilteredEventTags(_ context.Context, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastLim = limit
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

func (f *fakeEventTagsPurger) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func eventTagsCfg() EventTagsRetentionConfig {
	return EventTagsRetentionConfig{
		Enabled:          true,
		RunInterval:      5 * time.Minute,
		DeleteBatchLimit: 100,
	}
}

func TestRunEventTagsRetentionLoop_DisabledNoOp(t *testing.T) {
	purger := &fakeEventTagsPurger{}
	log := &recorderLogger{}
	cfg := eventTagsCfg()
	cfg.Enabled = false

	RunEventTagsRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("disabled loop must not purge, got %d calls", purger.callCount())
	}
	if log.countInfo("event_tags_retention_disabled") != 1 {
		t.Fatal("expected event_tags_retention_disabled log")
	}
}

func TestRunEventTagsRetentionLoop_InvalidConfig(t *testing.T) {
	purger := &fakeEventTagsPurger{}
	log := &recorderLogger{}
	cfg := eventTagsCfg()
	cfg.DeleteBatchLimit = 0

	RunEventTagsRetentionLoop(context.Background(), log, purger, cfg)

	if purger.callCount() != 0 {
		t.Fatalf("invalid config must not purge, got %d calls", purger.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunEventTagsRetentionDrain_Pacing(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	purger := &fakeEventTagsPurger{results: []int64{100, 100, 5}}
	log := &recorderLogger{}
	cfg := eventTagsCfg()

	runEventTagsRetentionDrain(context.Background(), log, purger, cfg)

	if purger.callCount() != 3 {
		t.Fatalf("expected 3 purge calls (2 saturated + 1 below limit), got %d", purger.callCount())
	}
	if purger.lastLim != cfg.DeleteBatchLimit {
		t.Fatalf("last limit = %d, want %d", purger.lastLim, cfg.DeleteBatchLimit)
	}
	if log.countInfo("event_tags_retention_purged") != 3 {
		t.Fatal("expected three purged log lines")
	}
}

func TestRunEventTagsRetentionDrain_StopsOnError(t *testing.T) {
	purger := &fakeEventTagsPurger{err: errors.New("db down")}
	log := &recorderLogger{}

	runEventTagsRetentionDrain(context.Background(), log, purger, eventTagsCfg())

	if purger.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", purger.callCount())
	}
	if log.countInfo("event_tags_retention_purged") != 0 {
		t.Fatal("must not log a purge on error")
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected purge_failed error log")
	}
}
