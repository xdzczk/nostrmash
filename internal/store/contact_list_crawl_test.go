package store

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSeedContactListCrawl_TrustSeedsBeforeAuthors(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_seeds (pubkey, is_active) VALUES
			('active-seed', true),
			('inactive-seed', false)
	`); err != nil {
		t.Fatalf("insert trust seeds: %v", err)
	}
	insertProfileStats(t, ctx, pool, "author-1", 1, 100)

	inserted, err := s.SeedContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("inserted = %d, want 1", inserted)
	}
	if got := crawlHop(t, ctx, pool, "active-seed"); got != 0 {
		t.Fatalf("active seed hop = %d, want 0", got)
	}
	if crawlExists(t, ctx, pool, "inactive-seed") {
		t.Fatal("inactive seed was queued")
	}
	if crawlExists(t, ctx, pool, "author-1") {
		t.Fatal("author was queued before trust seeds were exhausted")
	}

	inserted, err = s.SeedContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("seed authors: %v", err)
	}
	if inserted != 1 || !crawlExists(t, ctx, pool, "author-1") {
		t.Fatalf("author inserted = %d queued = %v", inserted, crawlExists(t, ctx, pool, "author-1"))
	}
}

func TestSeedContactListCrawl_CursorAndRescan(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	insertProfileStats(t, ctx, pool, "author-a", 1, 300)
	insertProfileStats(t, ctx, pool, "author-b", 1, 200)
	insertProfileStats(t, ctx, pool, "author-c", 1, 100)
	insertProfileStats(t, ctx, pool, "quiet", 0, 800)
	insertCrawlEvent(t, ctx, pool, "evt-listed", "listed", 3, `{"id":"evt-listed"}`)
	if _, err := pool.Exec(ctx, `
		INSERT INTO contact_lists_latest (pubkey, event_id, created_at, contacts_json, derivation_version)
		VALUES ('listed', 'evt-listed', 1, '[]', 1)
	`); err != nil {
		t.Fatalf("insert contact list: %v", err)
	}
	insertProfileStats(t, ctx, pool, "listed", 1, 900)

	for i := 0; i < 3; i++ {
		inserted, err := s.SeedContactListCrawl(ctx, 1, time.Hour)
		if err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
		if inserted != 1 {
			t.Fatalf("seed %d inserted = %d, want 1", i, inserted)
		}
	}
	for _, pubkey := range []string{"author-a", "author-b", "author-c"} {
		if !crawlExists(t, ctx, pool, pubkey) {
			t.Fatalf("expected %s to be queued", pubkey)
		}
	}
	if crawlExists(t, ctx, pool, "quiet") || crawlExists(t, ctx, pool, "listed") {
		t.Fatal("queued an author with no notes or an author who already has a contact list")
	}

	inserted, err := s.SeedContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("exhaust seed: %v", err)
	}
	if inserted != 0 {
		t.Fatalf("exhaust inserted = %d, want 0", inserted)
	}
	var nextSeed time.Time
	if err := pool.QueryRow(ctx, `SELECT next_seed_at FROM contact_list_crawl_cursor WHERE id = 1`).Scan(&nextSeed); err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if !nextSeed.After(time.Now().Add(30 * time.Minute)) {
		t.Fatalf("next_seed_at = %s, want a rescan pause", nextSeed)
	}

	insertProfileStats(t, ctx, pool, "author-new", 1, 600)
	inserted, err = s.SeedContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("seed during pause: %v", err)
	}
	if inserted != 0 || crawlExists(t, ctx, pool, "author-new") {
		t.Fatal("seeded a new author during the rescan pause")
	}

	if _, err := pool.Exec(ctx, `
		UPDATE contact_list_crawl_cursor
		SET next_seed_at = now() - interval '1 minute',
		    recent_activity_at = NULL,
		    pubkey = '',
		    in_null_activity = false
	`); err != nil {
		t.Fatalf("reset cursor: %v", err)
	}
	inserted, err = s.SeedContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("seed after rescan: %v", err)
	}
	if inserted != 1 || !crawlExists(t, ctx, pool, "author-new") {
		t.Fatalf("after rescan inserted=%d author-new queued=%v", inserted, crawlExists(t, ctx, pool, "author-new"))
	}
}

func TestClaimContactListCrawl_LowestHopFirstAndLease(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO contact_list_crawl (pubkey, hop, next_eligible_at) VALUES
			('hop-two', 2, now() - interval '1 minute'),
			('hop-zero', 0, now() - interval '1 minute'),
			('hop-later', 0, now() + interval '1 hour'),
			('hop-one', 1, now() - interval '1 minute')
	`); err != nil {
		t.Fatalf("insert queue: %v", err)
	}

	first, err := s.ClaimContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(first) != 1 || first[0].Pubkey != "hop-zero" || first[0].Hop != 0 {
		t.Fatalf("first claim = %+v, want hop-zero", first)
	}
	second, err := s.ClaimContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 1 || second[0].Pubkey != "hop-one" {
		t.Fatalf("second claim = %+v, want hop-one", second)
	}
	third, err := s.ClaimContactListCrawl(ctx, 1, time.Hour)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if len(third) != 1 || third[0].Pubkey != "hop-two" {
		t.Fatalf("third claim = %+v, want hop-two", third)
	}
	fourth, err := s.ClaimContactListCrawl(ctx, 5, time.Hour)
	if err != nil {
		t.Fatalf("fourth claim: %v", err)
	}
	if len(fourth) != 0 {
		t.Fatalf("leased and future rows were claimed: %+v", fourth)
	}

	if err := s.MarkContactListCrawlFetched(ctx, "hop-zero", true, time.Hour); err != nil {
		t.Fatalf("mark fetched: %v", err)
	}
	var found bool
	var eligible time.Time
	if err := pool.QueryRow(ctx, `
		SELECT found_list, next_eligible_at FROM contact_list_crawl WHERE pubkey = 'hop-zero'
	`).Scan(&found, &eligible); err != nil {
		t.Fatalf("read mark: %v", err)
	}
	if !found || !eligible.After(time.Now().Add(30*time.Minute)) {
		t.Fatalf("found=%v eligible=%s", found, eligible)
	}

	if err := s.MarkContactListCrawlFailed(ctx, "hop-one", time.Minute, errors.New("relay down")); err != nil {
		t.Fatalf("mark failed: %v", err)
	}
	var lastError *string
	if err := pool.QueryRow(ctx, `SELECT last_error FROM contact_list_crawl WHERE pubkey = 'hop-one'`).Scan(&lastError); err != nil {
		t.Fatalf("read error: %v", err)
	}
	if lastError == nil || *lastError != "relay down" {
		t.Fatalf("last_error = %v", lastError)
	}
}

func TestEnqueueContactListCrawl_PendingCapAndConflict(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO contact_list_crawl (pubkey, hop, next_eligible_at) VALUES
			('due-1', 0, now() - interval '1 minute'),
			('due-2', 0, now() - interval '1 minute'),
			('kept', 0, now() - interval '1 minute')
	`); err != nil {
		t.Fatalf("insert queue: %v", err)
	}

	enqueued, err := s.EnqueueContactListCrawl(ctx, []string{"extra"}, 1, 2)
	if err != nil {
		t.Fatalf("enqueue at cap: %v", err)
	}
	if enqueued != 0 || crawlExists(t, ctx, pool, "extra") {
		t.Fatalf("enqueued %d at cap", enqueued)
	}

	enqueued, err = s.EnqueueContactListCrawl(ctx, []string{"extra", "kept"}, 5, 5)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if enqueued != 1 {
		t.Fatalf("enqueued = %d, want 1", enqueued)
	}
	if got := crawlHop(t, ctx, pool, "kept"); got != 0 {
		t.Fatalf("existing hop = %d, want 0", got)
	}
	if got := crawlHop(t, ctx, pool, "extra"); got != 5 {
		t.Fatalf("new hop = %d, want 5", got)
	}

	// One slot left, and the first candidate is already queued. The new pubkey still fits.
	enqueued, err = s.EnqueueContactListCrawl(ctx, []string{"kept", "tail"}, 4, 5)
	if err != nil {
		t.Fatalf("enqueue around duplicate: %v", err)
	}
	if enqueued != 1 || crawlHop(t, ctx, pool, "tail") != 4 {
		t.Fatalf("enqueued = %d, tail hop = %d", enqueued, crawlHop(t, ctx, pool, "tail"))
	}
	if got := crawlHop(t, ctx, pool, "kept"); got != 0 {
		t.Fatalf("existing hop = %d, want 0", got)
	}
}

func TestLatestKind10002Raw_UsesNewest(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	insertCrawlEvent(t, ctx, pool, "old-list", "author", 10002, `{"kind":10002,"tags":[["r","wss://old.example"]]}`)
	if _, err := pool.Exec(ctx, `UPDATE events SET created_at = 10 WHERE id = 'old-list'`); err != nil {
		t.Fatalf("age old list: %v", err)
	}
	insertCrawlEvent(t, ctx, pool, "new-list", "author", 10002, `{"kind":10002,"tags":[["r","wss://new.example","write"]]}`)
	if _, err := pool.Exec(ctx, `UPDATE events SET created_at = 20 WHERE id = 'new-list'`); err != nil {
		t.Fatalf("age new list: %v", err)
	}

	raw, err := s.LatestKind10002Raw(ctx, "author")
	if err != nil {
		t.Fatalf("load raw: %v", err)
	}
	if !strings.Contains(string(raw), "wss://new.example") || strings.Contains(string(raw), "wss://old.example") {
		t.Fatalf("raw = %s", raw)
	}
	missing, err := s.LatestKind10002Raw(ctx, "nobody")
	if err != nil || missing != nil {
		t.Fatalf("missing = %q, err = %v", missing, err)
	}
}

func insertProfileStats(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string, noteCount int, activity int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO profile_public_stats (
			pubkey, follower_count, following_count, note_count, reply_count, recent_activity_at, derivation_version
		) VALUES ($1, 0, 0, $2, 0, $3, 1)
	`, pubkey, noteCount, activity); err != nil {
		t.Fatalf("insert profile stats %s: %v", pubkey, err)
	}
}

func insertCrawlEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, raw string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ($1, $2, 1, $3, 'sig', '', $4::jsonb)
	`, id, pubkey, kind, raw); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func crawlExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) bool {
	t.Helper()
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM contact_list_crawl WHERE pubkey = $1)`, pubkey).Scan(&exists); err != nil {
		t.Fatalf("crawl exists %s: %v", pubkey, err)
	}
	return exists
}

func crawlHop(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) int {
	t.Helper()
	var hop int
	if err := pool.QueryRow(ctx, `SELECT hop FROM contact_list_crawl WHERE pubkey = $1`, pubkey).Scan(&hop); err != nil {
		t.Fatalf("crawl hop %s: %v", pubkey, err)
	}
	return hop
}
