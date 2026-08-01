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

func TestProjectEventURLs_ExtractsNormalizesAndDeduplicates(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

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
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

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

func TestProjectEventURLs_PreservesObservedAndWritesCanonicalDomain(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	event := newEventForTest(
		"evt_url_canonical",
		"author_canonical",
		2202,
		1,
		nil,
		"https://www.youtube.com/watch?v=one https://youtu.be/two",
		time.Unix(2202, 0).UTC(),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.ProjectEventURLs(ctx, event.ID); err != nil {
		t.Fatalf("project urls: %v", err)
	}

	rows := readEventURLRows(t, ctx, pool, event.ID)
	if len(rows) != 2 {
		t.Fatalf("unexpected URL row count: got=%d want=2 rows=%#v", len(rows), rows)
	}
	if rows[0].Domain != "www.youtube.com" || rows[0].CanonicalDomain != "youtube.com" {
		t.Fatalf("unexpected www.youtube.com row: %#v", rows[0])
	}
	if rows[1].Domain != "youtu.be" || rows[1].CanonicalDomain != "youtube.com" {
		t.Fatalf("unexpected youtu.be row: %#v", rows[1])
	}
}

// TestProjectEventURLs_ExcludesAuthorsOutsideTrustGraph verifies the
// 2026-08-01 fix: once trust_graph_snapshot is populated, URLs are only
// recorded for authors inside it. Re-deriving an event whose author has
// since dropped out of the trust graph must also delete any URLs recorded
// while they were still trusted.
func TestProjectEventURLs_ExcludesAuthorsOutsideTrustGraph(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	trusted := newEventForTest("evt_url_trusted", "trusted_author", 2400, 1, nil, "https://trusted.example/a", time.Unix(2400, 0).UTC())
	untrusted := newEventForTest("evt_url_untrusted", "untrusted_author", 2401, 1, nil, "https://untrusted.example/a", time.Unix(2401, 0).UTC())
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

	if err := handlers.ProjectEventURLs(ctx, trusted.ID); err != nil {
		t.Fatalf("project urls (trusted): %v", err)
	}
	if err := handlers.ProjectEventURLs(ctx, untrusted.ID); err != nil {
		t.Fatalf("project urls (untrusted): %v", err)
	}

	if rows := readEventURLRows(t, ctx, pool, trusted.ID); len(rows) != 1 {
		t.Fatalf("expected trusted author's URL to be recorded, got %#v", rows)
	}
	if rows := readEventURLRows(t, ctx, pool, untrusted.ID); len(rows) != 0 {
		t.Fatalf("expected untrusted author's URL to be excluded, got %#v", rows)
	}

	// Re-deriving after the previously-trusted author drops out of the
	// graph must clean up their existing rows.
	if _, err := pool.Exec(ctx, `DELETE FROM trust_graph_snapshot WHERE pubkey = 'trusted_author'`); err != nil {
		t.Fatalf("remove trusted_author from trust graph: %v", err)
	}
	if err := handlers.ProjectEventURLs(ctx, trusted.ID); err != nil {
		t.Fatalf("re-project urls after trust removal: %v", err)
	}
	if rows := readEventURLRows(t, ctx, pool, trusted.ID); len(rows) != 0 {
		t.Fatalf("expected URLs to be purged once author left the trust graph, got %#v", rows)
	}
}

// TestProjectEventURLs_EmptyTrustGraphFailsSafeOpen verifies that when
// trust_graph_snapshot has never been loaded (e.g. the trust worker isn't
// running on this deployment), URL projection is not silently disabled for
// every author.
func TestProjectEventURLs_EmptyTrustGraphFailsSafeOpen(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	event := newEventForTest("evt_url_no_trust_graph", "any_author", 2410, 1, nil, "https://example.com/a", time.Unix(2410, 0).UTC())
	if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := handlers.ProjectEventURLs(ctx, event.ID); err != nil {
		t.Fatalf("project urls: %v", err)
	}
	if rows := readEventURLRows(t, ctx, pool, event.ID); len(rows) != 1 {
		t.Fatalf("expected URL to be recorded when trust graph is empty (fail-safe open), got %#v", rows)
	}
}

func TestProjectionRebuildScopes_EventURLsFullRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	events := []model.Event{
		newEventForTest("evt_url_rebuild_1", "author_a", 2300, 1, nil, "https://alpha.example/a", time.Unix(2300, 0).UTC()),
		newEventForTest("evt_url_rebuild_2", "author_b", 2301, 1, nil, "https://youtu.be/b", time.Unix(2301, 0).UTC()),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.ProjectEventURLs(ctx, event.ID); err != nil {
			t.Fatalf("project urls %s: %v", event.ID, err)
		}
	}

	if _, err := pool.Exec(ctx, `
		UPDATE event_urls
		SET canonical_domain = domain
		WHERE event_id = 'evt_url_rebuild_2'
	`); err != nil {
		t.Fatalf("simulate pre-canonical projection: %v", err)
	}

	targetVersion := derivation.EventURLsVersion + 1
	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationEventURLs,
		TargetVersion:  targetVersion,
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
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationEventURLs, targetVersion, targetVersion)

	var minVersion int
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MIN(derivation_version), 0) FROM event_urls`).Scan(&minVersion); err != nil {
		t.Fatalf("query min URL derivation version: %v", err)
	}
	if minVersion != targetVersion {
		t.Fatalf("unexpected URL derivation version after rebuild: got=%d want=%d", minVersion, targetVersion)
	}
	var canonicalDomain string
	if err := pool.QueryRow(ctx, `
		SELECT canonical_domain
		FROM event_urls
		WHERE event_id = 'evt_url_rebuild_2'
	`).Scan(&canonicalDomain); err != nil {
		t.Fatalf("query rebuilt canonical domain: %v", err)
	}
	if canonicalDomain != "youtube.com" {
		t.Fatalf("unexpected canonical domain after rebuild: got=%q want=youtube.com", canonicalDomain)
	}
}

type eventURLRow struct {
	URL             string
	Domain          string
	CanonicalDomain string
}

func readEventURLRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID string) []eventURLRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT url, domain, canonical_domain
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
		if err := rows.Scan(&row.URL, &row.Domain, &row.CanonicalDomain); err != nil {
			t.Fatalf("scan event url row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read event url rows: %v", err)
	}
	return out
}
