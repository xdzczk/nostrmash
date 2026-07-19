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
