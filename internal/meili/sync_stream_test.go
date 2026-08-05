package meili

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestStreamNotes_KeysetCoversAllRowsAcrossBatches(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meili_stream_notes")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meili_stream_notes")
	if err := store.Migrate(ctx, pool, "meili-stream-notes"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Same created_at for a few rows exercises the id tie-break in the keyset.
	// Timestamps must fall inside indexedNotesMaxAge or streamNotes omits them.
	now := time.Now().UTC().Unix()
	const n = 7
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("note_%02d", i)
		createdAt := now - int64(i/2) // pairs share created_at
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			VALUES ($1, 'pub', $2, 1, 'sig', $3, '{}'::jsonb)
		`, id, createdAt, "content "+id); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	// Noise: kind that must not appear.
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ('meta_0', 'pub', $1, 0, 'sig', '', '{}'::jsonb)
	`, now); err != nil {
		t.Fatalf("insert kind0 noise: %v", err)
	}

	var seen []string
	batchCount := 0
	if err := streamNotes(ctx, pool, 3, func(batch []NoteDocument) error {
		batchCount++
		if len(batch) == 0 || len(batch) > 3 {
			t.Fatalf("unexpected batch size %d", len(batch))
		}
		for i := 1; i < len(batch); i++ {
			prev, cur := batch[i-1], batch[i]
			if prev.CreatedAt < cur.CreatedAt || (prev.CreatedAt == cur.CreatedAt && prev.ID <= cur.ID) {
				t.Fatalf("batch not ordered DESC: %+v then %+v", prev, cur)
			}
		}
		for _, row := range batch {
			seen = append(seen, row.ID)
		}
		return nil
	}); err != nil {
		t.Fatalf("streamNotes: %v", err)
	}
	if batchCount < 3 {
		t.Fatalf("expected multiple batches, got %d", batchCount)
	}
	if len(seen) != n {
		t.Fatalf("seen=%d want %d (%v)", len(seen), n, seen)
	}
	uniq := map[string]struct{}{}
	for _, id := range seen {
		if _, ok := uniq[id]; ok {
			t.Fatalf("duplicate id %q", id)
		}
		uniq[id] = struct{}{}
	}
}

func TestStreamProfiles_KeysetCoversAllRowsAcrossBatches(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meili_stream_profiles")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meili_stream_profiles")
	if err := store.Migrate(ctx, pool, "meili-stream-profiles"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Same metadata_created_at for a few rows exercises pubkey ASC tie-break.
	seeds := []struct {
		pubkey string
		at     int64
	}{
		{"pk_c", 300},
		{"pk_a", 300},
		{"pk_b", 200},
		{"pk_d", 200},
		{"pk_e", 100},
	}
	for i, s := range seeds {
		eventID := fmt.Sprintf("evt_%d", i)
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			VALUES ($1, $2, $3, 0, 'sig', '', '{}'::jsonb)
		`, eventID, s.pubkey, s.at); err != nil {
			t.Fatalf("insert event %s: %v", eventID, err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO profiles_latest (
				pubkey, metadata_event_id, metadata_created_at, name, profile_json, derivation_version
			) VALUES ($1, $2, $3, $4, '{}'::jsonb, 1)
		`, s.pubkey, eventID, s.at, s.pubkey); err != nil {
			t.Fatalf("insert profile %s: %v", s.pubkey, err)
		}
	}

	var seen []string
	if err := streamProfiles(ctx, pool, 2, func(batch []ProfileDocument) error {
		for i := 1; i < len(batch); i++ {
			prev, cur := batch[i-1], batch[i]
			if prev.MetadataCreatedAt < cur.MetadataCreatedAt {
				t.Fatalf("created_at not DESC: %+v then %+v", prev, cur)
			}
			if prev.MetadataCreatedAt == cur.MetadataCreatedAt && prev.Pubkey >= cur.Pubkey {
				t.Fatalf("pubkey not ASC within created_at: %+v then %+v", prev, cur)
			}
		}
		for _, row := range batch {
			seen = append(seen, row.Pubkey)
		}
		return nil
	}); err != nil {
		t.Fatalf("streamProfiles: %v", err)
	}
	if len(seen) != len(seeds) {
		t.Fatalf("seen=%d want %d (%v)", len(seen), len(seeds), seen)
	}
	wantOrder := []string{"pk_a", "pk_c", "pk_b", "pk_d", "pk_e"}
	for i := range wantOrder {
		if seen[i] != wantOrder[i] {
			t.Fatalf("order[%d]=%q want %q (full=%v)", i, seen[i], wantOrder[i], seen)
		}
	}
}

func TestStreamSearchDocuments_KeysetCoversAllRowsAcrossBatches(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meili_stream_docs")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meili_stream_docs")
	if err := store.Migrate(ctx, pool, "meili-stream-docs"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	seeds := [][2]string{
		{"hashtag", "zzz"},
		{"hashtag", "aaa"},
		{"note", "n2"},
		{"note", "n1"},
		{"profile", "p1"},
		{"identity", "alice"},
		{"relay", "wss://x"},
	}
	for _, s := range seeds {
		if _, err := pool.Exec(ctx, `
			INSERT INTO search_documents (entity_type, entity_id, title, body)
			VALUES ($1, $2, $3, '')
		`, s[0], s[1], s[0]+":"+s[1]); err != nil {
			t.Fatalf("insert %s/%s: %v", s[0], s[1], err)
		}
	}

	var seen []string
	if err := streamSearchDocuments(ctx, pool, 2, func(batch []SearchDocument) error {
		for i := 1; i < len(batch); i++ {
			prev, cur := batch[i-1], batch[i]
			if prev.EntityType > cur.EntityType || (prev.EntityType == cur.EntityType && prev.EntityID >= cur.EntityID) {
				t.Fatalf("not ASC: %+v then %+v", prev, cur)
			}
		}
		for _, row := range batch {
			seen = append(seen, row.EntityType+"/"+row.EntityID)
		}
		return nil
	}); err != nil {
		t.Fatalf("streamSearchDocuments: %v", err)
	}
	// note/profile rows are intentionally excluded from the Meili documents index.
	want := []string{"hashtag/aaa", "hashtag/zzz", "identity/alice", "relay/wss://x"}
	if len(seen) != len(want) {
		t.Fatalf("seen=%v want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("order[%d]=%q want %q", i, seen[i], want[i])
		}
	}
}
