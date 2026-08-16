package meili

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

// TestSyncEventsBatchAndSearch_Smoke drives a real Meilisearch index+search
// round-trip against a real Postgres: it seeds a note event, syncs it into
// Meilisearch via SyncEventsBatch, then searches for it through the Searcher
// and asserts the note is returned. Gated on TEST_DATABASE_URL and
// TEST_MEILI_URL; skips when either is unset.
func TestSyncEventsBatchAndSearch_Smoke(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meili")
	meiliURL := dbtest.MeiliURL(t, "meili")

	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meili")
	if err := store.Migrate(ctx, pool, "meili-test"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	client, err := NewClient(Config{
		Enabled:   true,
		URL:       meiliURL,
		MasterKey: dbtest.MeiliMasterKey(),
	})
	if err != nil {
		t.Fatalf("new meili client: %v", err)
	}
	if !client.Enabled() {
		t.Fatal("expected meili client to be enabled")
	}
	if err := client.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}

	// Unique token so the search cannot match unrelated documents that may
	// already live in a shared test Meilisearch instance.
	token := fmt.Sprintf("nmsmoke%d", time.Now().UnixNano())
	eventID := "evt_" + token
	content := "hello " + token + " world"
	raw := fmt.Sprintf(`{"id":%q,"kind":1,"content":%q}`, eventID, content)
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
		VALUES ($1, 'author_pk', $2, 1, 'sig', $3, $4::jsonb, now(), now())
	`, eventID, time.Now().Unix(), content, raw); err != nil {
		t.Fatalf("seed note event: %v", err)
	}

	if err := client.SyncEventsBatch(ctx, pool, []string{eventID}); err != nil {
		t.Fatalf("sync events batch: %v", err)
	}

	searcher := NewSearcher(client, store.NewPostgresStore(pool))
	if !searcher.Enabled() {
		t.Fatal("expected searcher to be enabled")
	}

	results, err := searcher.SearchNotes(ctx, token, "", nil, "", 10, 0)
	if err != nil {
		t.Fatalf("search notes: %v", err)
	}
	if len(results) == 0 {
		t.Fatal("expected at least one search result for the seeded note")
	}

	found := false
	for _, raw := range results {
		var evt struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &evt); err != nil {
			continue
		}
		if evt.ID == eventID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("seeded note %q not present in search results", eventID)
	}
}

// TestNeedsSync_NotesComparesOnlyWithinIndexedWindow guards against a
// regression where NeedsSync compared Meili's notes index (which only ever
// holds indexedNotesMaxAge worth of content) against the all-time Postgres
// events count. That mismatch meant the ratio could never reach the sync
// threshold once the corpus outgrew the window, so NeedsSync returned true
// on every restart forever and triggered a full nuclear resync every time
// regardless of whether the indexed window was actually caught up.
func TestNeedsSync_NotesComparesOnlyWithinIndexedWindow(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meilineedssync")
	meiliURL := dbtest.MeiliURL(t, "meilineedssync")

	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meilineedssync")
	if err := store.Migrate(ctx, pool, "meilineedssync-test"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	client, err := NewClient(Config{
		Enabled:   true,
		URL:       meiliURL,
		MasterKey: dbtest.MeiliMasterKey(),
	})
	if err != nil {
		t.Fatalf("new meili client: %v", err)
	}
	if err := client.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}

	// The notes index is a single shared resource across every test in this
	// binary (only Postgres gets a fresh schema per test). Clear it first so
	// documents left behind by other tests can't pollute the ratio this test
	// computes.
	notesTask, err := client.service.Index(IndexNotes).DeleteAllDocumentsWithContext(ctx, nil)
	if err != nil {
		t.Fatalf("clear notes index: %v", err)
	}
	if err := client.waitForTask(ctx, notesTask.TaskUID); err != nil {
		t.Fatalf("wait for notes index clear: %v", err)
	}

	token := fmt.Sprintf("nmneedssync%d", time.Now().UnixNano())
	now := time.Now()
	insertNote := func(idSuffix string, createdAt time.Time) string {
		eventID := fmt.Sprintf("evt_%s_%s", token, idSuffix)
		content := "hello " + token
		raw := fmt.Sprintf(`{"id":%q,"kind":1,"content":%q}`, eventID, content)
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
			VALUES ($1, 'author_pk_'||$1, $2, 1, 'sig', $3, $4::jsonb, now(), now())
		`, eventID, createdAt.Unix(), content, raw); err != nil {
			t.Fatalf("seed note event %s: %v", eventID, err)
		}
		return eventID
	}

	// A large number of ancient (outside the 14-day indexed window) rows so
	// the all-time events count dwarfs anything the notes index could ever
	// hold. If NeedsSync regresses to comparing against all-time count,
	// this alone forces the ratio below threshold.
	const ancientRows = 20
	for i := 0; i < ancientRows; i++ {
		insertNote(fmt.Sprintf("ancient%d", i), now.Add(-60*24*time.Hour))
	}

	// One recent row, fully synced into Meili, so the *windowed* ratio is
	// 100%.
	recentID := insertNote("recent0", now.Add(-1*time.Hour))
	if err := client.SyncEventsBatch(ctx, pool, []string{recentID}); err != nil {
		t.Fatalf("sync recent note: %v", err)
	}

	needsSync, err := client.NeedsSync(ctx, pool)
	if err != nil {
		t.Fatalf("needs sync: %v", err)
	}
	if needsSync {
		t.Fatalf("expected NeedsSync=false: windowed notes ratio is fully caught up despite %d unrelated ancient rows", ancientRows)
	}

	// Now add more recent (in-window) rows without syncing them, so the
	// windowed ratio drops below threshold — NeedsSync should flip to true.
	const unsyncedRecentRows = 10
	for i := 0; i < unsyncedRecentRows; i++ {
		insertNote(fmt.Sprintf("recentunsynced%d", i), now.Add(-2*time.Hour))
	}
	needsSync, err = client.NeedsSync(ctx, pool)
	if err != nil {
		t.Fatalf("needs sync after adding unsynced recent rows: %v", err)
	}
	if !needsSync {
		t.Fatal("expected NeedsSync=true once the indexed window itself falls behind")
	}
}

// TestFullSync_Smoke drives FullSync end-to-end (notes + profiles +
// documents streams, each paced by fullSyncPacer) against real Postgres and
// Meilisearch instances, and asserts every seeded note lands in the notes
// index. A small batch size relative to the row count forces multiple
// pacing checkpoints per stream, exercising the new fullSyncPacer wiring
// rather than just its isolated unit tests.
func TestFullSync_Smoke(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meilifullsync")
	meiliURL := dbtest.MeiliURL(t, "meilifullsync")

	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meilifullsync")
	if err := store.Migrate(ctx, pool, "meilifullsync-test"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	client, err := NewClient(Config{
		Enabled:   true,
		URL:       meiliURL,
		MasterKey: dbtest.MeiliMasterKey(),
	})
	if err != nil {
		t.Fatalf("new meili client: %v", err)
	}

	token := fmt.Sprintf("nmfullsync%d", time.Now().UnixNano())
	now := time.Now()
	const noteCount = 23 // > 2*fullSyncPacingBatchInterval batches at batchSize=1
	wantIDs := make(map[string]struct{}, noteCount)
	for i := 0; i < noteCount; i++ {
		eventID := fmt.Sprintf("evt_%s_%d", token, i)
		content := fmt.Sprintf("hello %s world %d", token, i)
		raw := fmt.Sprintf(`{"id":%q,"kind":1,"content":%q}`, eventID, content)
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
			VALUES ($1, 'author_pk_'||$1, $2, 1, 'sig', $3, $4::jsonb, now(), now())
		`, eventID, now.Add(-time.Duration(i)*time.Minute).Unix(), content, raw); err != nil {
			t.Fatalf("seed note event %s: %v", eventID, err)
		}
		wantIDs[eventID] = struct{}{}
	}

	// batchSize=1 forces noteCount enqueue calls through fullSyncPacer,
	// crossing several fullSyncPacingBatchInterval checkpoints.
	stats, err := client.FullSync(ctx, pool, 1)
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}
	if stats.Notes != int64(noteCount) {
		t.Fatalf("stats.Notes = %d, want %d", stats.Notes, noteCount)
	}

	idx := client.service.Index(IndexNotes)
	var gotTotal int64
	for _, eventID := range keys(wantIDs) {
		var doc map[string]any
		if err := idx.GetDocument(eventID, nil, &doc); err != nil {
			t.Fatalf("get document %s from notes index: %v", eventID, err)
		}
		gotTotal++
	}
	if gotTotal != int64(noteCount) {
		t.Fatalf("found %d/%d seeded notes in the notes index after FullSync", gotTotal, noteCount)
	}
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
