package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// insertSearchDoc upserts because the trg_search_document_note_sync trigger on
// events auto-creates note documents; the test overrides body and freshness.
func insertSearchDoc(t *testing.T, ctx context.Context, pool *pgxpool.Pool, entityType, entityID, body string, freshness time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO search_documents (entity_type, entity_id, body, freshness)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (entity_type, entity_id) DO UPDATE
		SET body = EXCLUDED.body, freshness = EXCLUDED.freshness
	`, entityType, entityID, body, freshness.UTC()); err != nil {
		t.Fatalf("insert search doc %s/%s: %v", entityType, entityID, err)
	}
}

func searchDocBody(t *testing.T, ctx context.Context, pool *pgxpool.Pool, entityType, entityID string) (string, bool) {
	t.Helper()
	var body string
	err := pool.QueryRow(ctx, `
		SELECT body FROM search_documents WHERE entity_type = $1 AND entity_id = $2
	`, entityType, entityID).Scan(&body)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return "", false
		}
		t.Fatalf("read search doc %s/%s: %v", entityType, entityID, err)
	}
	return body, true
}

func TestGroomSearchDocuments(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	freshnessBefore := ref.Add(-30 * 24 * time.Hour)
	oldFresh := freshnessBefore.Add(-24 * time.Hour)
	recentFresh := ref

	longBody := strings.Repeat("x", 500)

	// Source events for non-orphaned notes.
	insertEventRow(t, ctx, pool, "note_trim", 1, ref.Unix())
	insertEventRow(t, ctx, pool, "note_recent", 1, ref.Unix())
	insertEventRow(t, ctx, pool, "note_short", 1, ref.Unix())

	// Trimmed: stale note with a long body.
	insertSearchDoc(t, ctx, pool, "note", "note_trim", longBody, oldFresh)
	// Untouched: recent long body, stale short body, stale long profile body.
	insertSearchDoc(t, ctx, pool, "note", "note_recent", longBody, recentFresh)
	insertSearchDoc(t, ctx, pool, "note", "note_short", "short", oldFresh)
	insertSearchDoc(t, ctx, pool, "profile", "profile_pub", longBody, oldFresh)
	// Pruned: note document with no backing event.
	insertSearchDoc(t, ctx, pool, "note", "note_orphan", "orphan body", recentFresh)

	trimmed, pruned, err := NewPostgresStore(pool).GroomSearchDocuments(ctx, freshnessBefore, 280, 100)
	if err != nil {
		t.Fatalf("groom: %v", err)
	}
	if trimmed != 1 {
		t.Fatalf("expected 1 trimmed row, got %d", trimmed)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned row, got %d", pruned)
	}

	if body, ok := searchDocBody(t, ctx, pool, "note", "note_trim"); !ok || len(body) != 280 {
		t.Fatalf("expected note_trim body trimmed to 280 chars, got ok=%v len=%d", ok, len(body))
	}
	if body, ok := searchDocBody(t, ctx, pool, "note", "note_recent"); !ok || len(body) != 500 {
		t.Fatalf("expected note_recent untouched, got ok=%v len=%d", ok, len(body))
	}
	if body, ok := searchDocBody(t, ctx, pool, "note", "note_short"); !ok || body != "short" {
		t.Fatalf("expected note_short untouched, got ok=%v body=%q", ok, body)
	}
	if body, ok := searchDocBody(t, ctx, pool, "profile", "profile_pub"); !ok || len(body) != 500 {
		t.Fatalf("expected profile body untouched, got ok=%v len=%d", ok, len(body))
	}
	if _, ok := searchDocBody(t, ctx, pool, "note", "note_orphan"); ok {
		t.Fatal("expected note_orphan pruned")
	}
}
