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

func TestProjectEventHashtags_ExtractsNormalizesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	event := newEventForTest(
		"evt_hashtag_1",
		"author_one",
		1200,
		1,
		[][]string{
			{"t", "Nostr"},
			{"t", " nostr "},
			{"T", "#Bitcoin"},
			{"t", ""},
			{"t", "###bad"},
			{"t", "bad tag"},
			{"p", "ignored_pubkey"},
		},
		"hello hashtags",
		time.Unix(1200, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
		t.Fatalf("project hashtags: %v", err)
	}

	rows := readEventHashtagRows(t, ctx, pool, event.ID)
	if len(rows) != 2 {
		t.Fatalf("unexpected hashtag row count: got=%d want=2 rows=%#v", len(rows), rows)
	}
	if rows[0].Hashtag != "bitcoin" || rows[1].Hashtag != "nostr" {
		t.Fatalf("unexpected normalized hashtags: %#v", rows)
	}
}

func TestDeriveEventBundle_ProjectsEventHashtags(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	event := newEventForTest(
		"evt_bundle_hashtag",
		"author_bundle",
		1201,
		1,
		[][]string{{"t", "nostr"}},
		"bundle projection",
		time.Unix(1201, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
		t.Fatalf("derive event bundle: %v", err)
	}

	rows := readEventHashtagRows(t, ctx, pool, event.ID)
	if len(rows) != 1 || rows[0].Hashtag != "nostr" {
		t.Fatalf("unexpected bundle hashtag rows: %#v", rows)
	}
}

func TestProjectionRebuildScopes_EventHashtagsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	events := []model.Event{
		newEventForTest("evt_hash_rebuild_1", "author_a", 1300, 1, [][]string{{"t", "nostr"}}, "a", time.Unix(1300, 0).UTC()),
		newEventForTest("evt_hash_rebuild_2", "author_b", 1301, 1, [][]string{{"t", "bitcoin"}}, "b", time.Unix(1301, 0).UTC()),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
			t.Fatalf("project hashtags %s: %v", event.ID, err)
		}
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationEventHashtags,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger hashtag rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute hashtag rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationEventHashtags, 2, 2)

	var minVersion int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MIN(derivation_version), 0) FROM event_hashtags`).Scan(&minVersion); err != nil {
		t.Fatalf("query min hashtag derivation version: %v", err)
	}
	if minVersion != 2 {
		t.Fatalf("unexpected hashtag derivation version after rebuild: got=%d want=2", minVersion)
	}
}

type hashtagRow struct {
	Hashtag string
}

func readEventHashtagRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) []hashtagRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT hashtag
		FROM event_hashtags
		WHERE event_id = $1
		ORDER BY hashtag ASC
	`, eventID)
	if err != nil {
		t.Fatalf("query event hashtags for %s: %v", eventID, err)
	}
	defer rows.Close()

	out := make([]hashtagRow, 0)
	for rows.Next() {
		var row hashtagRow
		if err := rows.Scan(&row.Hashtag); err != nil {
			t.Fatalf("scan event hashtag row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event hashtag rows: %v", err)
	}
	return out
}
