package retention_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/store/retention"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func setupRetention(t *testing.T) (context.Context, *pgxpool.Pool, *retention.Retention) {
	t.Helper()
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "retention"), "retention")
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	return ctx, pool, retention.New(pool)
}

func insertEvent(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id, pubkey string, kind int, createdAt int64, firstSeenAt time.Time) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at)
		VALUES ($1, $2, $3, $4, 'sig', '', '{}'::jsonb, $5)
	`, id, pubkey, createdAt, kind, firstSeenAt.UTC()); err != nil {
		t.Fatalf("insert event %s: %v", id, err)
	}
}

func remainingEventIDs(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id FROM events ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("query events: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		out = append(out, id)
	}
	return out
}

func TestPurgeUntrustedAuthorEvents(t *testing.T) {
	ctx, pool, s := setupRetention(t)

	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	olderThan := ref
	deadGraceBefore := ref.Add(-7 * 24 * time.Hour)
	oldUnix := ref.Add(-30 * 24 * time.Hour).Unix()
	oldSeen := ref.Add(-30 * 24 * time.Hour)

	insertEvent(t, ctx, pool, "u_failsafe", "untrusted_pub", 1, oldUnix, oldSeen)
	deleted, err := s.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge with empty snapshot: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("empty trust graph must delete nothing, got %d", deleted)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ('trusted_pub', 1, false)
	`); err != nil {
		t.Fatalf("insert trust snapshot: %v", err)
	}

	insertEvent(t, ctx, pool, "u_old_note", "untrusted_pub", 1, oldUnix, oldSeen)
	insertEvent(t, ctx, pool, "u_old_deletion", "untrusted_pub", 5, oldUnix, oldSeen)
	insertEvent(t, ctx, pool, "k_trusted_note", "trusted_pub", 1, oldUnix, oldSeen)
	insertEvent(t, ctx, pool, "k_open_kind", "untrusted_pub", 0, oldUnix, oldSeen)
	// Untrusted reply to a local note must be retained (target-exists ingest).
	insertEvent(t, ctx, pool, "u_old_reply", "untrusted_pub", 1, oldUnix, oldSeen)
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id,
			parent_missing, root_missing, derivation_version
		)
		VALUES ('u_old_reply', $1, 'k_trusted_note', 'k_trusted_note', false, false, 1)
	`, oldUnix); err != nil {
		t.Fatalf("insert thread edge for untrusted reply: %v", err)
	}

	deleted, err = s.PurgeUntrustedAuthorEvents(ctx, olderThan, deadGraceBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 3 {
		t.Fatalf("expected 3 deletions, got %d", deleted)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"k_open_kind", "k_trusted_note", "u_old_reply"}
	if !sameStrings(got, want) {
		t.Fatalf("remaining mismatch: got %v want %v", got, want)
	}
}

func insertEventURL(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, pubkey, url string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_urls (event_id, author_pubkey, created_at, url, domain, canonical_domain, derivation_version)
		VALUES ($1, $2, $3, $4, $4, $4, 1)
	`, eventID, pubkey, createdAt, url); err != nil {
		t.Fatalf("insert event url %s/%s: %v", eventID, url, err)
	}
}

func insertEventHashtag(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, pubkey, hashtag string, createdAt int64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES ($1, $2, $3, $4, 1)
	`, eventID, pubkey, createdAt, hashtag); err != nil {
		t.Fatalf("insert event hashtag %s/%s: %v", eventID, hashtag, err)
	}
}

func remainingEventURLDomains(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT domain FROM event_urls ORDER BY domain ASC`)
	if err != nil {
		t.Fatalf("query event_urls: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			t.Fatalf("scan domain: %v", err)
		}
		out = append(out, domain)
	}
	return out
}

func remainingEventHashtagValues(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT hashtag FROM event_hashtags ORDER BY hashtag ASC`)
	if err != nil {
		t.Fatalf("query event_hashtags: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var hashtag string
		if err := rows.Scan(&hashtag); err != nil {
			t.Fatalf("scan hashtag: %v", err)
		}
		out = append(out, hashtag)
	}
	return out
}

// TestPurgeUntrustedAuthorEventURLs verifies the retroactive complement to
// the write-time trust gate in internal/derivation/projection_urls.go:
// event_urls rows from authors outside trust_graph_snapshot get purged, with
// the same empty-graph fail-safe as PurgeUntrustedAuthorEvents.
func TestPurgeUntrustedAuthorEventURLs(t *testing.T) {
	ctx, pool, s := setupRetention(t)
	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC).Unix()

	insertEvent(t, ctx, pool, "evt_url_failsafe", "untrusted_pub", 1, ref, time.Unix(ref, 0))
	insertEventURL(t, ctx, pool, "evt_url_failsafe", "untrusted_pub", "untrusted.example", ref)

	deleted, err := s.PurgeUntrustedAuthorEventURLs(ctx, 100)
	if err != nil {
		t.Fatalf("purge with empty snapshot: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("empty trust graph must delete nothing, got %d", deleted)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ('trusted_pub', 1, false)
	`); err != nil {
		t.Fatalf("insert trust snapshot: %v", err)
	}

	insertEvent(t, ctx, pool, "evt_url_trusted", "trusted_pub", 1, ref, time.Unix(ref, 0))
	insertEventURL(t, ctx, pool, "evt_url_trusted", "trusted_pub", "trusted.example", ref)

	deleted, err = s.PurgeUntrustedAuthorEventURLs(ctx, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	// evt_url_failsafe's row is now eligible too, since the graph is
	// non-empty and untrusted_pub was never added to it.
	if deleted != 1 {
		t.Fatalf("expected 1 deletion, got %d", deleted)
	}

	if got, want := remainingEventURLDomains(t, ctx, pool), []string{"trusted.example"}; !sameStrings(got, want) {
		t.Fatalf("remaining URLs mismatch: got %v want %v", got, want)
	}
}

// TestPurgeUntrustedAuthorEventHashtags is the event_hashtags counterpart to
// TestPurgeUntrustedAuthorEventURLs.
func TestPurgeUntrustedAuthorEventHashtags(t *testing.T) {
	ctx, pool, s := setupRetention(t)
	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC).Unix()

	insertEvent(t, ctx, pool, "evt_hashtag_failsafe", "untrusted_pub", 1, ref, time.Unix(ref, 0))
	insertEventHashtag(t, ctx, pool, "evt_hashtag_failsafe", "untrusted_pub", "untrustedtag", ref)

	deleted, err := s.PurgeUntrustedAuthorEventHashtags(ctx, 100)
	if err != nil {
		t.Fatalf("purge with empty snapshot: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("empty trust graph must delete nothing, got %d", deleted)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed)
		VALUES ('trusted_pub', 1, false)
	`); err != nil {
		t.Fatalf("insert trust snapshot: %v", err)
	}

	insertEvent(t, ctx, pool, "evt_hashtag_trusted", "trusted_pub", 1, ref, time.Unix(ref, 0))
	insertEventHashtag(t, ctx, pool, "evt_hashtag_trusted", "trusted_pub", "trustedtag", ref)

	deleted, err = s.PurgeUntrustedAuthorEventHashtags(ctx, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deletion, got %d", deleted)
	}

	if got, want := remainingEventHashtagValues(t, ctx, pool), []string{"trustedtag"}; !sameStrings(got, want) {
		t.Fatalf("remaining hashtags mismatch: got %v want %v", got, want)
	}
}

func TestPruneAuthorRecentEvents(t *testing.T) {
	ctx, pool, s := setupRetention(t)
	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	olderThan := ref.Add(-90 * 24 * time.Hour)

	for i := 0; i < 5; i++ {
		createdAt := ref.Add(-time.Duration(i) * time.Hour).Unix()
		id := fmt.Sprintf("a_%d", i)
		insertEvent(t, ctx, pool, id, "author_a", 1, createdAt, ref)
		if _, err := pool.Exec(ctx, `
			INSERT INTO author_recent_events (author_pubkey, event_id, created_at, derivation_version)
			VALUES ('author_a', $1, $2, 1)
		`, id, createdAt); err != nil {
			t.Fatalf("insert author recent %s: %v", id, err)
		}
	}

	deleted, err := s.PruneAuthorRecentEvents(ctx, olderThan, 3, 10, 100)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 cap deletions, got %d", deleted)
	}
}

func TestPurgeStaleEventRelays(t *testing.T) {
	ctx, pool, s := setupRetention(t)
	ref := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	seenBefore := ref.Add(-180 * 24 * time.Hour)
	ancient := seenBefore.Add(-24 * time.Hour)
	recent := ref

	insertEvent(t, ctx, pool, "e_multi", "pub", 1, ref.Unix(), ref)
	for _, row := range []struct {
		relay string
		seen  time.Time
	}{
		{"wss://first", ancient.Add(-time.Hour)},
		{"wss://dup_old", ancient},
		{"wss://dup_recent", recent},
	} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO event_relays (event_id, relay_url, seen_at, pubkey)
			VALUES ('e_multi', $1, $2, 'pub')
		`, row.relay, row.seen.UTC()); err != nil {
			t.Fatalf("insert event relay: %v", err)
		}
	}

	deleted, err := s.PurgeStaleEventRelays(ctx, seenBefore, 100)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deletion, got %d", deleted)
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	sort.Strings(got)
	sort.Strings(want)
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
