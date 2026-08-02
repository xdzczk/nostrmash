package derivation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestRefreshRelayWindowSnapshots_NilHandlers(t *testing.T) {
	var h *derivation.Handlers
	if err := h.RefreshRelayWindowSnapshots(context.Background()); err == nil {
		t.Fatal("expected error when handlers are not initialized")
	}
}

func TestRelayWindowSnapshotAge_NilHandlers(t *testing.T) {
	var h *derivation.Handlers
	if _, _, err := h.RelayWindowSnapshotAge(context.Background()); err == nil {
		t.Fatal("expected error when handlers are not initialized")
	}
}

func TestRelayWindowSnapshotAge_NilPool(t *testing.T) {
	h := derivation.NewHandlers(nil)
	if _, _, err := h.RelayWindowSnapshotAge(context.Background()); err == nil {
		t.Fatal("expected error when pool is nil")
	}
}

func TestRelayWindowSnapshotAge_ReflectsRefresh(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)

	// Migrations 000047/000048 seed a placeholder home_window_24h row at
	// apply time so a brand-new environment has a reasonable default
	// before the first refresh — so the row already exists right after
	// MustMigrate. Delete it here to exercise the genuinely-never-computed
	// path this test is actually about: ok must be false rather than
	// reporting a misleading "age zero" or an error when the row is
	// absent (e.g. a from-scratch environment before this migration ever
	// ran, or a row explicitly cleared).
	if _, err := pool.Exec(ctx, `DELETE FROM relay_window_snapshots WHERE snapshot_label = 'home_window_24h'`); err != nil {
		t.Fatalf("delete seeded home_window_24h row: %v", err)
	}
	if age, ok, err := handlers.RelayWindowSnapshotAge(ctx); err != nil {
		t.Fatalf("RelayWindowSnapshotAge before refresh: %v", err)
	} else if ok {
		t.Fatalf("expected ok=false before any refresh, got age=%s", age)
	}

	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("refresh relay window snapshots: %v", err)
	}

	age, ok, err := handlers.RelayWindowSnapshotAge(ctx)
	if err != nil {
		t.Fatalf("RelayWindowSnapshotAge after refresh: %v", err)
	}
	if !ok {
		t.Fatal("expected ok=true after a successful refresh")
	}
	if age < 0 || age > 30*time.Second {
		t.Fatalf("expected a near-zero age right after refresh, got %s", age)
	}
}

func TestRefreshRelayWindowSnapshots_PopulatesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	now := time.Now().UTC()
	notes := []struct {
		id      string
		pubkey  string
		relay   string
		hashtag string
		content string
	}{
		{"note_a", "author_1", "wss://relay.one", "nostr", "the quick brown fox jumps over the lazy dog and you https://alpha.example/1"},
		{"note_b", "author_2", "wss://relay.one", "nostr", "have that from your window when there was this thing https://alpha.example/2"},
		{"note_c", "author_3", "wss://relay.two", "bitcoin", "what will you do about this and that for the people https://beta.example/1"},
	}
	for i, n := range notes {
		tags := [][]string{{"t", n.hashtag}}
		event := newEventForTest(n.id, n.pubkey, now.Unix()-int64(i*10), 1, tags, n.content, now)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, n.relay, now); err != nil {
			t.Fatalf("insert canonical event %s: %v", n.id, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", n.id, err)
		}
		// The trending hashtags/domains snapshots are Web-of-Trust scoped
		// (see trustedAuthorJoinClause); seed every author as trusted so
		// this test still exercises those snapshots being populated.
		if _, err := pool.Exec(ctx, `
			INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
			VALUES ($1, 0, false)
			ON CONFLICT (pubkey) DO NOTHING
		`, n.pubkey); err != nil {
			t.Fatalf("seed trust_graph_snapshot for %s: %v", n.pubkey, err)
		}
	}

	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("refresh relay window snapshots: %v", err)
	}

	labels := []string{
		"summary", "top_relays_7d", "home_window_24h", "home_window_7d",
		"top_languages_24h", "top_languages_7d", "top_hashtags_24h", "top_hashtags_7d",
		"top_domains_24h", "top_domains_7d",
	}
	for _, label := range labels {
		if !snapshotExists(t, ctx, pool, label) {
			t.Fatalf("expected snapshot row for label %q", label)
		}
	}

	summary := readSummarySnapshot(t, ctx, pool)
	if summary.Total < 2 {
		t.Fatalf("expected >=2 distinct relays, got total=%d", summary.Total)
	}
	if summary.Events24h < 3 {
		t.Fatalf("expected >=3 relay events in 24h window, got %d", summary.Events24h)
	}
	if summary.Authors24h < 3 {
		t.Fatalf("expected >=3 distinct authors in 24h window, got %d", summary.Authors24h)
	}

	home := readHomeWindowSnapshot(t, ctx, pool, "home_window_24h")
	if home.NoteVolume < 3 {
		t.Fatalf("expected note_volume >=3, got %d", home.NoteVolume)
	}
	if home.ActiveAuthors < 3 {
		t.Fatalf("expected active_authors >=3, got %d", home.ActiveAuthors)
	}
	var history struct {
		NoteVolume    int64
		ActiveAuthors int64
		RelayEvents   int64
	}
	if err := pool.QueryRow(ctx, `
		SELECT note_volume, active_authors, relay_events
		FROM stats_snapshot_history
		ORDER BY bucket_start DESC
		LIMIT 1
	`).Scan(&history.NoteVolume, &history.ActiveAuthors, &history.RelayEvents); err != nil {
		t.Fatalf("read stats snapshot history: %v", err)
	}
	if history.NoteVolume < 3 || history.ActiveAuthors < 3 || history.RelayEvents < 3 {
		t.Fatalf("unexpected stats snapshot history: %+v", history)
	}

	firstComputedAt := snapshotComputedAt(t, ctx, pool, "summary")

	// Idempotency: a second refresh must not error, must keep the same
	// aggregate values, and must advance computed_at.
	time.Sleep(5 * time.Millisecond)
	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	summary2 := readSummarySnapshot(t, ctx, pool)
	if summary2 != summary {
		t.Fatalf("summary changed across idempotent refresh: %+v vs %+v", summary, summary2)
	}
	secondComputedAt := snapshotComputedAt(t, ctx, pool, "summary")
	if !secondComputedAt.After(firstComputedAt) {
		t.Fatalf("expected computed_at to advance: first=%s second=%s", firstComputedAt, secondComputedAt)
	}
	var historyCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM stats_snapshot_history`).Scan(&historyCount); err != nil {
		t.Fatalf("count stats snapshot history: %v", err)
	}
	if historyCount != 1 {
		t.Fatalf("expected one current-hour history row, got %d", historyCount)
	}
}

// TestRefreshTrendingLinksSnapshots_ExcludesUntrustedAuthors verifies the
// product/perf fix from 2026-08-01: the homepage's trending hashtags and
// trending domains snapshots only include links/tags authored by pubkeys
// inside the Web of Trust (trust_graph_snapshot). An untrusted author's
// hashtag or link must never surface in these snapshots, even though the
// live, non-homepage /discovery/hashtags and /discovery/domains endpoints
// remain network-wide.
func TestRefreshTrendingLinksSnapshots_ExcludesUntrustedAuthors(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	trustedEvent := newEventForTest(
		"trust_note_trusted", "trusted_author", now.Unix(), 1,
		[][]string{{"t", "trustedtag"}},
		"sharing a link https://trusted.example/post",
		now,
	)
	untrustedEvent := newEventForTest(
		"trust_note_untrusted", "untrusted_author", now.Unix()-5, 1,
		[][]string{{"t", "untrustedtag"}},
		"sharing a link https://untrusted.example/post",
		now,
	)
	events := []struct {
		event model.Event
		tags  [][]string
	}{
		{trustedEvent, [][]string{{"t", "trustedtag"}}},
		{untrustedEvent, [][]string{{"t", "untrustedtag"}}},
	}
	for _, e := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, e.event, e.tags, "wss://relay.one", now); err != nil {
			t.Fatalf("insert canonical event %s: %v", e.event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, e.event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", e.event.ID, err)
		}
	}

	// Only the trusted author is in the Web of Trust.
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ($1, 0, true)
	`, trustedEvent.Pubkey); err != nil {
		t.Fatalf("seed trust_graph_snapshot: %v", err)
	}

	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("refresh relay window snapshots: %v", err)
	}

	hashtags := readHashtagLabels(t, ctx, pool, "top_hashtags_24h")
	if !containsString(hashtags, "trustedtag") {
		t.Fatalf("expected trusted author's hashtag in snapshot, got %#v", hashtags)
	}
	if containsString(hashtags, "untrustedtag") {
		t.Fatalf("expected untrusted author's hashtag to be excluded, got %#v", hashtags)
	}

	domains := readDomainLabels(t, ctx, pool, "top_domains_24h")
	if !containsString(domains, "trusted.example") {
		t.Fatalf("expected trusted author's domain in snapshot, got %#v", domains)
	}
	if containsString(domains, "untrusted.example") {
		t.Fatalf("expected untrusted author's domain to be excluded, got %#v", domains)
	}
}

// TestRefreshRelayWindowSnapshots_ExcludesFallbackRelay ensures the synthetic
// API-fallback provenance label never ranks as a real peer in Network stats.
func TestRefreshRelayWindowSnapshots_ExcludesFallbackRelay(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	real := newEventForTest("real_note", "author_real", now.Unix(), 1, nil, "hello from a real relay", now)
	if err := pgStore.InsertCanonicalEvent(ctx, real, nil, "wss://relay.one", now); err != nil {
		t.Fatalf("insert real event: %v", err)
	}
	// Flood the synthetic fallback provenance so it would otherwise dominate
	// top_relays_7d if the snapshot failed to exclude it.
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("fallback_note_%d", i)
		evt := newEventForTest(id, fmt.Sprintf("author_fb_%d", i), now.Unix()-int64(i), 1, nil, "fallback only", now)
		if err := pgStore.InsertCanonicalEvent(ctx, evt, nil, model.FallbackRelayURL, now); err != nil {
			t.Fatalf("insert fallback event %s: %v", id, err)
		}
	}

	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("refresh relay window snapshots: %v", err)
	}

	summary := readSummarySnapshot(t, ctx, pool)
	if summary.Total != 1 {
		t.Fatalf("expected total=1 real relay, got %d", summary.Total)
	}
	if summary.Events24h != 1 {
		t.Fatalf("expected events_24h=1 (fallback rows excluded), got %d", summary.Events24h)
	}

	top := readTopRelaysSnapshot(t, ctx, pool)
	if len(top) != 1 || top[0].RelayURL != "wss://relay.one" {
		t.Fatalf("expected only wss://relay.one in top relays, got %#v", top)
	}
	for _, row := range top {
		if row.RelayURL == model.FallbackRelayURL {
			t.Fatalf("fallback sentinel leaked into top_relays_7d: %#v", top)
		}
	}
}

func containsString(haystack []string, needle string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

func readTopRelaysSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []struct {
	RelayURL      string `json:"relay_url"`
	EventCount    int64  `json:"event_count"`
	UniqueAuthors int64  `json:"unique_authors"`
} {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = 'top_relays_7d'`).Scan(&raw); err != nil {
		t.Fatalf("read top_relays_7d payload: %v", err)
	}
	var out []struct {
		RelayURL      string `json:"relay_url"`
		EventCount    int64  `json:"event_count"`
		UniqueAuthors int64  `json:"unique_authors"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode top_relays_7d payload: %v", err)
	}
	return out
}

func readHashtagLabels(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) []string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = $1`, label).Scan(&raw); err != nil {
		t.Fatalf("read hashtag payload %q: %v", label, err)
	}
	var rows []struct {
		Hashtag string `json:"hashtag"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("decode hashtag payload %q: %v", label, err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Hashtag)
	}
	return out
}

func readDomainLabels(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) []string {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = $1`, label).Scan(&raw); err != nil {
		t.Fatalf("read domain payload %q: %v", label, err)
	}
	var rows []struct {
		Domain string `json:"domain"`
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("decode domain payload %q: %v", label, err)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.Domain)
	}
	return out
}

func snapshotExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM relay_window_snapshots WHERE snapshot_label = $1)`, label).Scan(&exists); err != nil {
		t.Fatalf("check snapshot %q: %v", label, err)
	}
	return exists
}

func snapshotComputedAt(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) time.Time {
	t.Helper()
	var at time.Time
	if err := pool.QueryRow(ctx, `SELECT computed_at FROM relay_window_snapshots WHERE snapshot_label = $1`, label).Scan(&at); err != nil {
		t.Fatalf("read computed_at %q: %v", label, err)
	}
	return at
}

type summarySnapshot struct {
	Total      int64 `json:"total"`
	Active24h  int64 `json:"active_24h"`
	Events24h  int64 `json:"events_24h"`
	Authors24h int64 `json:"authors_24h"`
}

func readSummarySnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool) summarySnapshot {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = 'summary'`).Scan(&raw); err != nil {
		t.Fatalf("read summary payload: %v", err)
	}
	var out summarySnapshot
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode summary payload: %v", err)
	}
	return out
}

type homeWindowSnapshot struct {
	NoteVolume    int64 `json:"note_volume"`
	ActiveAuthors int64 `json:"active_authors"`
}

func readHomeWindowSnapshot(t *testing.T, ctx context.Context, pool *pgxpool.Pool, label string) homeWindowSnapshot {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM relay_window_snapshots WHERE snapshot_label = $1`, label).Scan(&raw); err != nil {
		t.Fatalf("read home window payload %q: %v", label, err)
	}
	var out homeWindowSnapshot
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode home window payload %q: %v", label, err)
	}
	return out
}
