package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestThreadProjection_RepairsMissingParentWhenReferenceArrives(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	baseTime := time.Date(2026, 4, 4, 19, 0, 0, 0, time.UTC)

	child := newEventForTest(
		"thread_child_missing_parent",
		"author_child",
		1001,
		1,
		[][]string{{"e", "thread_parent_late", "", "reply"}},
		`{"content":"child"}`,
		baseTime,
	)
	if err := pgStore.InsertCanonicalEvent(ctx, child, extractTagsFromRaw(t, child.RawJSON), "wss://relay.one", child.FirstSeenAt); err != nil {
		t.Fatalf("insert child event: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("project child thread edge with missing parent: %v", err)
	}

	var parentMissing bool
	if err := pool.QueryRow(ctx, `
		SELECT parent_missing
		FROM thread_edges
		WHERE child_event_id = $1
	`, child.ID).Scan(&parentMissing); err != nil {
		t.Fatalf("query projected thread edge: %v", err)
	}
	if !parentMissing {
		t.Fatalf("expected parent_missing=true before repair")
	}

	var unresolvedCount int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM unresolved_thread_references
		WHERE source_event_id = $1 AND missing_event_id = $2
	`, child.ID, "thread_parent_late").Scan(&unresolvedCount); err != nil {
		t.Fatalf("count unresolved references before repair: %v", err)
	}
	if unresolvedCount != 1 {
		t.Fatalf("expected one unresolved reference, got %d", unresolvedCount)
	}

	parent := newEventForTest(
		"thread_parent_late",
		"author_parent",
		1000,
		1,
		nil,
		`{"content":"parent"}`,
		baseTime.Add(1*time.Second),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, parent, nil, "wss://relay.one", parent.FirstSeenAt); err != nil {
		t.Fatalf("insert late parent: %v", err)
	}
	if err := handlers.RepairUnresolvedReferences(ctx, parent.ID); err != nil {
		t.Fatalf("repair unresolved references: %v", err)
	}
	if err := handlers.UpdateThreadProjection(ctx, child.ID); err != nil {
		t.Fatalf("reproject child after repair: %v", err)
	}

	if err := pool.QueryRow(ctx, `
		SELECT parent_missing
		FROM thread_edges
		WHERE child_event_id = $1
	`, child.ID).Scan(&parentMissing); err != nil {
		t.Fatalf("query thread edge after repair: %v", err)
	}
	if parentMissing {
		t.Fatalf("expected parent_missing=false after repair")
	}
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM unresolved_thread_references
		WHERE source_event_id = $1
	`, child.ID).Scan(&unresolvedCount); err != nil {
		t.Fatalf("count unresolved references after repair: %v", err)
	}
	if unresolvedCount != 0 {
		t.Fatalf("expected unresolved references to be cleared, got %d", unresolvedCount)
	}
}
