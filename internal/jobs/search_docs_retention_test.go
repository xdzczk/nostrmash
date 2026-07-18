package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeSearchDocsGroomer struct {
	mu                  sync.Mutex
	results             [][2]int64
	calls               int
	err                 error
	lastFreshnessBefore time.Time
	lastMaxBodyChars    int
	lastBatchLimit      int
}

func (f *fakeSearchDocsGroomer) GroomSearchDocuments(_ context.Context, freshnessBefore time.Time, maxBodyChars, batchLimit int) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastFreshnessBefore = freshnessBefore
	f.lastMaxBodyChars = maxBodyChars
	f.lastBatchLimit = batchLimit
	if f.err != nil {
		return 0, 0, f.err
	}
	if len(f.results) == 0 {
		return 0, 0, nil
	}
	r := f.results[0]
	f.results = f.results[1:]
	return r[0], r[1], nil
}

func (f *fakeSearchDocsGroomer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func searchDocsCfg() SearchDocsRetentionConfig {
	return SearchDocsRetentionConfig{
		Enabled:      true,
		BodyMaxAge:   30 * 24 * time.Hour,
		BodyMaxChars: 280,
		RunInterval:  6 * time.Hour,
		BatchLimit:   100,
	}
}

func TestRunSearchDocsRetentionLoop_DisabledNoOp(t *testing.T) {
	groomer := &fakeSearchDocsGroomer{}
	log := &recorderLogger{}
	cfg := searchDocsCfg()
	cfg.Enabled = false

	RunSearchDocsRetentionLoop(context.Background(), log, groomer, cfg)

	if groomer.callCount() != 0 {
		t.Fatalf("disabled loop must not groom, got %d calls", groomer.callCount())
	}
	if log.countInfo("search_docs_retention_disabled") != 1 {
		t.Fatal("expected search_docs_retention_disabled log")
	}
}

func TestRunSearchDocsRetentionLoop_InvalidConfig(t *testing.T) {
	groomer := &fakeSearchDocsGroomer{}
	log := &recorderLogger{}
	cfg := searchDocsCfg()
	cfg.BodyMaxChars = 0

	RunSearchDocsRetentionLoop(context.Background(), log, groomer, cfg)

	if groomer.callCount() != 0 {
		t.Fatalf("invalid config must not groom, got %d calls", groomer.callCount())
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunSearchDocsRetentionDrain_PacingOnEitherSaturatedPass(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	// First batch saturates trim, second saturates prune, third is below limit.
	groomer := &fakeSearchDocsGroomer{results: [][2]int64{{100, 0}, {0, 100}, {10, 10}}}
	log := &recorderLogger{}
	cfg := searchDocsCfg()

	before := time.Now().UTC()
	runSearchDocsRetentionDrain(context.Background(), log, groomer, cfg)
	after := time.Now().UTC()

	if groomer.callCount() != 3 {
		t.Fatalf("expected 3 groom calls, got %d", groomer.callCount())
	}
	if groomer.lastMaxBodyChars != cfg.BodyMaxChars || groomer.lastBatchLimit != cfg.BatchLimit {
		t.Fatalf("unexpected args: maxBodyChars=%d batchLimit=%d", groomer.lastMaxBodyChars, groomer.lastBatchLimit)
	}
	wantFreshness := before.Add(-cfg.BodyMaxAge)
	if groomer.lastFreshnessBefore.Before(wantFreshness.Add(-2*time.Second)) || groomer.lastFreshnessBefore.After(after.Add(-cfg.BodyMaxAge).Add(2*time.Second)) {
		t.Fatalf("freshnessBefore %v not within expected window around %v", groomer.lastFreshnessBefore, wantFreshness)
	}
}

func TestRunSearchDocsRetentionDrain_StopsOnError(t *testing.T) {
	groomer := &fakeSearchDocsGroomer{err: errors.New("db down")}
	log := &recorderLogger{}

	runSearchDocsRetentionDrain(context.Background(), log, groomer, searchDocsCfg())

	if groomer.callCount() != 1 {
		t.Fatalf("expected drain to stop after first error, got %d calls", groomer.callCount())
	}
	if log.countInfo("search_docs_retention_groomed") != 0 {
		t.Fatal("must not log a groom on error")
	}
}
