package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestProjectEventURLs_ExtractsNormalizesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	event := newEventForTest(
		"evt_url_1",
		"author_one",
		2200,
		1,
		nil,
		"Read https://Example.com/path, then https://example.com/path! "+
			"Also HTTP://Example.com:80/A?x=1#frag and https://sub.EXAMPLE.com/post. "+
			"Ignore ftp://example.com and broken https://-bad.example.",
		time.Unix(2200, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.ProjectEventURLs(ctx, event.ID); err != nil {
		t.Fatalf("project urls: %v", err)
	}

	rows := readEventURLRows(t, ctx, pool, event.ID)
	if len(rows) != 3 {
		t.Fatalf("unexpected URL row count: got=%d want=3 rows=%#v", len(rows), rows)
	}
	if rows[0].Domain != "example.com" || rows[0].URL != "http://example.com/A?x=1" {
		t.Fatalf("unexpected first row: %#v", rows[0])
	}
	if rows[1].Domain != "example.com" || rows[1].URL != "https://example.com/path" {
		t.Fatalf("unexpected second row: %#v", rows[1])
	}
	if rows[2].Domain != "sub.example.com" || rows[2].URL != "https://sub.example.com/post" {
		t.Fatalf("unexpected third row: %#v", rows[2])
	}
}

func TestDeriveEventBundle_ProjectsEventURLs(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	event := newEventForTest(
		"evt_bundle_url",
		"author_bundle",
		2201,
		1,
		nil,
		"bundle projection https://nostr.example/post",
		time.Unix(2201, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
		t.Fatalf("derive event bundle: %v", err)
	}

	rows := readEventURLRows(t, ctx, pool, event.ID)
	if len(rows) != 1 || rows[0].Domain != "nostr.example" {
		t.Fatalf("unexpected bundle url rows: %#v", rows)
	}
}

func TestProjectionRebuildScopes_EventURLsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	events := []model.Event{
		newEventForTest("evt_url_rebuild_1", "author_a", 2300, 1, nil, "https://alpha.example/a", time.Unix(2300, 0).UTC()),
		newEventForTest("evt_url_rebuild_2", "author_b", 2301, 1, nil, "https://beta.example/b", time.Unix(2301, 0).UTC()),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventURLs(ctx, event.ID); err != nil {
			t.Fatalf("project urls %s: %v", event.ID, err)
		}
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationEventURLs,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger URL rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute URL rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationEventURLs, 2, 2)

	var minVersion int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MIN(derivation_version), 0) FROM event_urls`).Scan(&minVersion); err != nil {
		t.Fatalf("query min URL derivation version: %v", err)
	}
	if minVersion != 2 {
		t.Fatalf("unexpected URL derivation version after rebuild: got=%d want=2", minVersion)
	}
}

type eventURLRow struct {
	URL    string
	Domain string
}

func readEventURLRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) []eventURLRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT url, domain
		FROM event_urls
		WHERE event_id = $1
		ORDER BY domain ASC, url ASC
	`, eventID)
	if err != nil {
		t.Fatalf("query event urls for %s: %v", eventID, err)
	}
	defer rows.Close()

	out := make([]eventURLRow, 0)
	for rows.Next() {
		var row eventURLRow
		if err := rows.Scan(&row.URL, &row.Domain); err != nil {
			t.Fatalf("scan event url row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event url rows: %v", err)
	}
	return out
}
