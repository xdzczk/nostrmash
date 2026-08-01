package derivation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
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

	// Before the first refresh the row doesn't exist yet: ok must be
	// false rather than reporting a misleading "age zero" or an error.
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
		{"note_a", "author_1", "wss://relay.one", "nostr", "the quick brown fox jumps over the lazy dog and you"},
		{"note_b", "author_2", "wss://relay.one", "nostr", "have that from your window when there was this thing"},
		{"note_c", "author_3", "wss://relay.two", "bitcoin", "what will you do about this and that for the people"},
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
	}

	if err := handlers.RefreshRelayWindowSnapshots(ctx); err != nil {
		t.Fatalf("refresh relay window snapshots: %v", err)
	}

	labels := []string{
		"summary", "top_relays_7d", "home_window_24h", "home_window_7d",
		"top_languages_24h", "top_languages_7d", "top_hashtags_24h", "top_hashtags_7d",
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
