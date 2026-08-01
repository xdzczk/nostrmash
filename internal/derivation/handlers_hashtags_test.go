package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectEventHashtags_ExtractsNormalizesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

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
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

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

// TestProjectEventHashtags_ExcludesAuthorsOutsideTrustGraph verifies the
// 2026-08-01 fix: once trust_graph_snapshot is populated, hashtags are only
// recorded for authors inside it. Re-deriving an event whose author has
// since dropped out of the trust graph must also delete any hashtags
// recorded while they were still trusted.
func TestProjectEventHashtags_ExcludesAuthorsOutsideTrustGraph(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	trusted := newEventForTest("evt_hashtag_trusted", "trusted_author", 1400, 1, [][]string{{"t", "trustedtag"}}, "a", time.Unix(1400, 0).UTC())
	untrusted := newEventForTest("evt_hashtag_untrusted", "untrusted_author", 1401, 1, [][]string{{"t", "untrustedtag"}}, "b", time.Unix(1401, 0).UTC())
	for _, event := range []model.Event{trusted, untrusted} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
	}

	// A non-empty trust graph containing trusted_author (plus an unrelated
	// pubkey so the graph stays non-empty — and the fail-safe stays
	// inactive — once trusted_author is removed below).
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ('trusted_author', 0, true), ('someone_else', 1, false)
	`); err != nil {
		t.Fatalf("seed trust_graph_snapshot: %v", err)
	}

	if err := handlers.ProjectEventHashtags(ctx, trusted.ID); err != nil {
		t.Fatalf("project hashtags (trusted): %v", err)
	}
	if err := handlers.ProjectEventHashtags(ctx, untrusted.ID); err != nil {
		t.Fatalf("project hashtags (untrusted): %v", err)
	}

	if rows := readEventHashtagRows(t, ctx, pool, trusted.ID); len(rows) != 1 {
		t.Fatalf("expected trusted author's hashtag to be recorded, got %#v", rows)
	}
	if rows := readEventHashtagRows(t, ctx, pool, untrusted.ID); len(rows) != 0 {
		t.Fatalf("expected untrusted author's hashtag to be excluded, got %#v", rows)
	}

	// Re-deriving after the previously-trusted author drops out of the
	// graph must clean up their existing rows.
	if _, err := pool.Exec(ctx, `DELETE FROM trust_graph_snapshot WHERE pubkey = 'trusted_author'`); err != nil {
		t.Fatalf("remove trusted_author from trust graph: %v", err)
	}
	if err := handlers.ProjectEventHashtags(ctx, trusted.ID); err != nil {
		t.Fatalf("re-project hashtags after trust removal: %v", err)
	}
	if rows := readEventHashtagRows(t, ctx, pool, trusted.ID); len(rows) != 0 {
		t.Fatalf("expected hashtags to be purged once author left the trust graph, got %#v", rows)
	}
}

// TestProjectEventHashtags_EmptyTrustGraphFailsSafeOpen verifies that when
// trust_graph_snapshot has never been loaded (e.g. the trust worker isn't
// running on this deployment), hashtag projection is not silently disabled
// for every author.
func TestProjectEventHashtags_EmptyTrustGraphFailsSafeOpen(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	event := newEventForTest("evt_hashtag_no_trust_graph", "any_author", 1410, 1, [][]string{{"t", "nostr"}}, "a", time.Unix(1410, 0).UTC())
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.ProjectEventHashtags(ctx, event.ID); err != nil {
		t.Fatalf("project hashtags: %v", err)
	}
	if rows := readEventHashtagRows(t, ctx, pool, event.ID); len(rows) != 1 {
		t.Fatalf("expected hashtag to be recorded when trust graph is empty (fail-safe open), got %#v", rows)
	}
}

func TestProjectionRebuildScopes_EventHashtagsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

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
