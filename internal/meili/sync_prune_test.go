package meili

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

func TestPrunePendingMeilisearchSyncs_DeletesOnlyRowsAtOrBeforeCutoff(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "meili_prune")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "meili_prune")
	if err := store.Migrate(ctx, pool, "meili-prune-test"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO pending_meilisearch_syncs (event_id, marked_at) VALUES
			('old_a', now() - interval '2 hours'),
			('old_b', now() - interval '1 hour'),
			('boundary', now()),
			('new_a', now() + interval '1 hour')
	`); err != nil {
		t.Fatalf("seed pending rows: %v", err)
	}

	var syncStartedAt time.Time
	if err := pool.QueryRow(ctx, `
		SELECT marked_at FROM pending_meilisearch_syncs WHERE event_id = 'boundary'
	`).Scan(&syncStartedAt); err != nil {
		t.Fatalf("load boundary marked_at: %v", err)
	}

	deleted, err := prunePendingMeilisearchSyncs(ctx, pool, syncStartedAt)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("deleted=%d, want 3 (old_a, old_b, boundary)", deleted)
	}

	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pending_meilisearch_syncs`).Scan(&remaining); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("remaining=%d, want 1", remaining)
	}
	var survivor string
	if err := pool.QueryRow(ctx, `SELECT event_id FROM pending_meilisearch_syncs`).Scan(&survivor); err != nil {
		t.Fatalf("load survivor: %v", err)
	}
	if survivor != "new_a" {
		t.Fatalf("survivor=%q, want new_a", survivor)
	}
}
