package hydration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/account"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

const testPubkey = "abababababababababababababababababababababababababababababababab"

// --- pure helpers ---

func TestIsHexPubkey(t *testing.T) {
	t.Parallel()
	cases := map[string]bool{
		testPubkey:                  true,
		strings.Repeat("0", 64):     true,
		strings.Repeat("f", 64):     true,
		strings.Repeat("g", 64):     false, // non-hex char
		strings.ToUpper(testPubkey): false, // uppercase not accepted
		"abc":                       false, // too short
		strings.Repeat("a", 63):     false, // wrong length
		strings.Repeat("a", 65):     false,
	}
	for in, want := range cases {
		if got := isHexPubkey(in); got != want {
			t.Fatalf("isHexPubkey(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestCapStrings(t *testing.T) {
	t.Parallel()
	in := []string{"a", "b", "c", "d"}
	if got := capStrings(in, 2); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("capStrings cap 2 = %v", got)
	}
	if got := capStrings(in, 0); len(got) != 4 {
		t.Fatalf("capStrings cap 0 should be a no-op, got %v", got)
	}
	if got := capStrings(in, 10); len(got) != 4 {
		t.Fatalf("capStrings cap > len should be a no-op, got %v", got)
	}
}

func TestBuildFilter(t *testing.T) {
	t.Parallel()
	since := int64(100)
	until := int64(200)
	f := buildFilter(FetchFilter{
		Kinds:   []int{1, 7},
		Authors: []string{"a"},
		IDs:     []string{"id1"},
		ETags:   []string{"e1"},
		Since:   &since,
		Until:   &until,
		Limit:   25,
	})
	if f["limit"] != 25 {
		t.Fatalf("limit = %v", f["limit"])
	}
	if f["since"] != since || f["until"] != until {
		t.Fatalf("since/until = %v/%v", f["since"], f["until"])
	}
	if _, ok := f["#e"]; !ok {
		t.Fatal("expected #e key for ETags")
	}
	if _, ok := f["authors"]; !ok {
		t.Fatal("expected authors key")
	}
	// Empty slices must be omitted entirely.
	empty := buildFilter(FetchFilter{Limit: 5})
	for _, k := range []string{"kinds", "authors", "ids", "#e", "since", "until"} {
		if _, ok := empty[k]; ok {
			t.Fatalf("empty filter should omit %q", k)
		}
	}
}

func TestParseEnvelope(t *testing.T) {
	t.Parallel()
	t.Run("event", func(t *testing.T) {
		frame := []byte(`["EVENT","sub-1",{"id":"x","kind":1}]`)
		typ, sub, payload, ok := parseEnvelope(frame)
		if !ok || typ != "EVENT" || sub != "sub-1" {
			t.Fatalf("unexpected: %q %q ok=%v", typ, sub, ok)
		}
		var ev map[string]any
		if err := json.Unmarshal(payload, &ev); err != nil || ev["id"] != "x" {
			t.Fatalf("payload parse failed: %v %v", ev, err)
		}
	})
	t.Run("eose", func(t *testing.T) {
		typ, sub, payload, ok := parseEnvelope([]byte(`["EOSE","sub-1"]`))
		if !ok || typ != "EOSE" || sub != "sub-1" || payload != nil {
			t.Fatalf("unexpected eose parse: %q %q %v ok=%v", typ, sub, payload, ok)
		}
	})
	t.Run("malformed and unknown", func(t *testing.T) {
		if _, _, _, ok := parseEnvelope([]byte(`not json`)); ok {
			t.Fatal("malformed frame should not parse")
		}
		if _, _, _, ok := parseEnvelope([]byte(`["NOTICE","hello"]`)); ok {
			t.Fatal("unknown envelope type should not parse")
		}
		if _, _, _, ok := parseEnvelope([]byte(`["EVENT"]`)); ok {
			t.Fatal("too-short envelope should not parse")
		}
	})
}

// --- fakes ---

type fakeAccountStore struct {
	mu sync.Mutex

	row      account.AccountStateRow
	pressure store.StoragePressureState

	promoteCalls  int
	promotePub    string
	promoteReason string

	coverageCalls  int
	lastHydratedAt *time.Time
	lastSuccessful *time.Time
	oldest         *time.Time
	newest         *time.Time
	coverageWindow *int
}

func (f *fakeAccountStore) PromoteAccountToTracked(_ context.Context, pubkey, reason string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoteCalls++
	f.promotePub = pubkey
	f.promoteReason = reason
	return "tracked", nil
}

func (f *fakeAccountStore) GetAccountState(_ context.Context, _ string) (account.AccountStateRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.row, nil
}

func (f *fakeAccountStore) GetStoragePressureState(_ context.Context) (store.StoragePressureState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pressure, nil
}

func (f *fakeAccountStore) UpdateAccountCoverage(
	_ context.Context,
	_ string,
	lastHydratedAt *time.Time,
	lastSuccessfulHydrationAt *time.Time,
	oldestKnownNoteAt *time.Time,
	newestKnownNoteAt *time.Time,
	_ *time.Time,
	coverageWindowDays *int,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.coverageCalls++
	f.lastHydratedAt = lastHydratedAt
	f.lastSuccessful = lastSuccessfulHydrationAt
	f.oldest = oldestKnownNoteAt
	f.newest = newestKnownNoteAt
	f.coverageWindow = coverageWindowDays
	return nil
}

type fakePersister struct {
	mu       sync.Mutex
	handled  int
	payloads [][]byte
}

func (p *fakePersister) Handle(_ context.Context, _ string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handled++
	p.payloads = append(p.payloads, payload)
	return nil
}

type fakeRelays struct{ urls []string }

func (r fakeRelays) Relays(context.Context) ([]string, error) {
	return append([]string(nil), r.urls...), nil
}

// fakeFetcher returns canned events based on the shape of the filter, mirroring
// the four passes the service makes (profile, notes, engagement, parents).
type fakeFetcher struct {
	profile    [][]byte
	notes      [][]byte
	engagement [][]byte
	parents    [][]byte
}

func containsInt(xs []int, want int) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func (f fakeFetcher) Fetch(_ context.Context, _ string, filter FetchFilter) ([][]byte, error) {
	switch {
	case len(filter.IDs) > 0:
		return f.parents, nil
	case len(filter.ETags) > 0:
		return f.engagement, nil
	case containsInt(filter.Kinds, 1):
		return f.notes, nil
	case containsInt(filter.Kinds, 0):
		return f.profile, nil
	}
	return nil, nil
}

func note(id string, createdAt int64, parentID string) []byte {
	ev := map[string]any{
		"id":         id,
		"kind":       1,
		"created_at": createdAt,
		"tags":       [][]string{{"e", parentID}},
	}
	b, _ := json.Marshal(ev)
	return b
}

func event(id string, kind int, createdAt int64) []byte {
	ev := map[string]any{"id": id, "kind": kind, "created_at": createdAt}
	b, _ := json.Marshal(ev)
	return b
}

func successConfig() config.HydrationConfig {
	return config.HydrationConfig{
		Enabled:             true,
		MaxRelays:           4,
		MaxEventsPerAccount: 100,
		MaxLookbackDays:     90,
		MaxRuntime:          5 * time.Second,
		Cooldown:            time.Hour,
		MaxConcurrency:      2,
	}
}

func hexID(c byte) string { return strings.Repeat(string(c), 64) }

func TestHydrate_SuccessFullPath(t *testing.T) {
	t.Parallel()
	const oldestTS, newestTS = int64(1700000000), int64(1700000500)
	fetcher := fakeFetcher{
		profile: [][]byte{event(hexID('1'), 0, 1699000000)},
		notes: [][]byte{
			note(hexID('2'), oldestTS, hexID('a')),
			note(hexID('3'), newestTS, hexID('b')),
		},
		engagement: [][]byte{event(hexID('4'), 7, 1700000600)},
		parents:    [][]byte{event(hexID('a'), 1, 1700000250)},
	}
	st := &fakeAccountStore{}
	persister := &fakePersister{}
	svc := NewService(discardLogger(), successConfig(), st, persister, fetcher, fakeRelays{urls: []string{"wss://relay.example"}})

	res, err := svc.Hydrate(context.Background(), testPubkey, "test-reason")
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if res.Status != "success" {
		t.Fatalf("status = %q, want success", res.Status)
	}
	if !res.Promoted {
		t.Fatal("expected Promoted true")
	}
	// 1 profile + 2 notes + 1 engagement + 1 parent.
	if res.EventsFound != 5 {
		t.Fatalf("EventsFound = %d, want 5", res.EventsFound)
	}
	if persister.handled != 5 {
		t.Fatalf("persister handled = %d, want 5", persister.handled)
	}
	if st.promoteCalls != 1 || st.promotePub != testPubkey || st.promoteReason != "test-reason" {
		t.Fatalf("promote calls=%d pub=%q reason=%q", st.promoteCalls, st.promotePub, st.promoteReason)
	}
	if st.coverageCalls != 1 {
		t.Fatalf("coverage calls = %d, want 1", st.coverageCalls)
	}
	if st.lastHydratedAt == nil || st.lastSuccessful == nil {
		t.Fatal("expected last_hydrated_at and last_successful_hydration_at set on success")
	}
	if st.oldest == nil || st.oldest.Unix() != oldestTS {
		t.Fatalf("oldest = %v, want unix %d", st.oldest, oldestTS)
	}
	if st.newest == nil || st.newest.Unix() != newestTS {
		t.Fatalf("newest = %v, want unix %d", st.newest, newestTS)
	}
	if st.coverageWindow == nil || *st.coverageWindow != 90 {
		t.Fatalf("coverage window = %v, want 90", st.coverageWindow)
	}
}

func TestHydrate_PartialWhenNoEventsFound(t *testing.T) {
	t.Parallel()
	st := &fakeAccountStore{}
	persister := &fakePersister{}
	svc := NewService(discardLogger(), successConfig(), st, persister, fakeFetcher{}, fakeRelays{urls: []string{"wss://relay.example"}})

	res, err := svc.Hydrate(context.Background(), testPubkey, "r")
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if res.Status != "partial" {
		t.Fatalf("status = %q, want partial", res.Status)
	}
	if !res.Promoted || res.EventsFound != 0 {
		t.Fatalf("expected promoted with 0 events, got %+v", res)
	}
	// Coverage still updated, but last_successful must be nil when nothing found.
	if st.coverageCalls != 1 || st.lastSuccessful != nil {
		t.Fatalf("coverage=%d lastSuccessful=%v", st.coverageCalls, st.lastSuccessful)
	}
}

func TestHydrate_InvalidPubkeyFails(t *testing.T) {
	t.Parallel()
	st := &fakeAccountStore{}
	svc := NewService(discardLogger(), successConfig(), st, &fakePersister{}, fakeFetcher{}, fakeRelays{})
	res, err := svc.Hydrate(context.Background(), "not-a-pubkey", "r")
	if err == nil {
		t.Fatal("expected error for invalid pubkey")
	}
	if res.Status != "failed" {
		t.Fatalf("status = %q, want failed", res.Status)
	}
	if st.promoteCalls != 0 {
		t.Fatal("invalid pubkey must not promote")
	}
}

func TestHydrate_SkippedWhenDisabled(t *testing.T) {
	t.Parallel()
	cfg := successConfig()
	cfg.Enabled = false
	st := &fakeAccountStore{}
	svc := NewService(discardLogger(), cfg, st, &fakePersister{}, fakeFetcher{}, fakeRelays{})
	res, err := svc.Hydrate(context.Background(), testPubkey, "r")
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if res.Status != "skipped" || res.Promoted {
		t.Fatalf("expected skipped/not-promoted, got %+v", res)
	}
	if st.promoteCalls != 0 {
		t.Fatal("disabled hydration must not promote")
	}
}

func TestHydrate_SkippedUnderStoragePressure(t *testing.T) {
	t.Parallel()
	st := &fakeAccountStore{
		pressure: store.StoragePressureState{Level: int(config.PressureDisableHydration)},
	}
	svc := NewService(discardLogger(), successConfig(), st, &fakePersister{}, fakeFetcher{}, fakeRelays{})
	res, err := svc.Hydrate(context.Background(), testPubkey, "r")
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if res.Status != "skipped" {
		t.Fatalf("status = %q, want skipped", res.Status)
	}
	if st.promoteCalls != 0 {
		t.Fatal("hydration under disable-pressure must not promote")
	}
}

func TestHydrate_SkippedDuringCooldown(t *testing.T) {
	t.Parallel()
	now := time.Now()
	st := &fakeAccountStore{
		row: account.AccountStateRow{Exists: true, LastHydratedAt: &now},
	}
	svc := NewService(discardLogger(), successConfig(), st, &fakePersister{}, fakeFetcher{}, fakeRelays{})
	res, err := svc.Hydrate(context.Background(), testPubkey, "r")
	if err != nil {
		t.Fatalf("Hydrate: %v", err)
	}
	if res.Status != "skipped" {
		t.Fatalf("status = %q, want skipped", res.Status)
	}
	if st.promoteCalls != 0 {
		t.Fatal("hydration during cooldown must not promote")
	}
}
