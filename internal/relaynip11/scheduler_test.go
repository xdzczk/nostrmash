package relaynip11

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

type memStore struct {
	mu     sync.Mutex
	relays []relayregistry.RelayRecord
	saved  map[string]StoredSummary
	err    error
}

func (m *memStore) ListRelaysForNIP11(ctx context.Context, staleBefore time.Time, limit int) ([]relayregistry.RelayRecord, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.relays, nil
}

func (m *memStore) UpdateCapabilitySummary(ctx context.Context, urlKey string, summary json.RawMessage) error {
	var decoded StoredSummary
	if err := json.Unmarshal(summary, &decoded); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saved == nil {
		m.saved = map[string]StoredSummary{}
	}
	m.saved[urlKey] = decoded
	return nil
}

type stubFetcher struct {
	info Info
	err  error
}

func (s stubFetcher) Fetch(ctx context.Context, relayURL string) (Info, error) {
	return s.info, s.err
}

func testScheduler(store capabilityStore, fetcher documentFetcher) *Scheduler {
	return &Scheduler{
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		store:   store,
		fetcher: fetcher,
		cfg: config.RelayRegistryNIP11Config{
			Enabled:     true,
			Interval:    time.Minute,
			Freshness:   time.Hour,
			Timeout:     time.Second,
			MaxParallel: 2,
			BatchSize:   10,
		},
	}
}

func TestRunOnce_PersistsSuccessWithoutProbe(t *testing.T) {
	store := &memStore{relays: []relayregistry.RelayRecord{{
		URLKey:        "relay-a",
		NormalizedURL: "wss://relay.example",
	}}}
	sched := testScheduler(store, stubFetcher{info: Info{Name: "Demo"}})
	if err := sched.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := store.saved["relay-a"]
	if !got.OK || got.Info == nil || got.Info.Name != "Demo" || got.FetchedAt.IsZero() {
		t.Fatalf("%#v", got)
	}
}

func TestRunOnce_PersistsFetchFailure(t *testing.T) {
	store := &memStore{relays: []relayregistry.RelayRecord{{
		URLKey:        "relay-a",
		NormalizedURL: "wss://relay.example",
	}}}
	sched := testScheduler(store, stubFetcher{err: &FetchError{Code: "http_404"}})
	if err := sched.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := store.saved["relay-a"]
	if got.OK || got.Error != "http_404" || got.Info != nil || got.FetchedAt.IsZero() {
		t.Fatalf("%#v", got)
	}
}

func TestMetricOutcome(t *testing.T) {
	if metricOutcome("http_404") != "http_error" || metricOutcome("not_json") != "not_json" || metricOutcome("nope") != "other" {
		t.Fatal("outcome mapping drifted")
	}
}
