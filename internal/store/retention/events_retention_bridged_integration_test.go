package retention_test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/store/retention"
)

func insertEventAttributes(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, proxyProtocol, proxyID string) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_attributes (event_id, proxy_protocol, proxy_id, derivation_version)
		VALUES ($1, $2, $3, 1)
	`, eventID, proxyProtocol, proxyID); err != nil {
		t.Fatalf("insert event_attributes %s: %v", eventID, err)
	}
}

func insertAuthorProjections(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) {
	t.Helper()
	for _, q := range []string{
		`INSERT INTO profile_discovery_stats (pubkey, derivation_version) VALUES ($1, 1)`,
		`INSERT INTO profile_public_stats (pubkey, derivation_version) VALUES ($1, 1)`,
		`INSERT INTO profile_fields (pubkey, derivation_version) VALUES ($1, 1)`,
		`INSERT INTO nip05_verifications (pubkey, nip05, domain) VALUES ($1, 'x@example.com', 'example.com')`,
	} {
		if _, err := pool.Exec(ctx, q, pubkey); err != nil {
			t.Fatalf("insert author projection for %s: %v", pubkey, err)
		}
	}
}

func countAuthorProjections(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubkey string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT (SELECT count(*) FROM profile_discovery_stats WHERE pubkey = $1)
		     + (SELECT count(*) FROM profile_public_stats WHERE pubkey = $1)
		     + (SELECT count(*) FROM profile_fields WHERE pubkey = $1)
		     + (SELECT count(*) FROM nip05_verifications WHERE pubkey = $1)
	`, pubkey).Scan(&n); err != nil {
		t.Fatalf("count author projections for %s: %v", pubkey, err)
	}
	return n
}

func TestLockConflict(t *testing.T) {
	deadlock := fmt.Errorf("delete events by id: %w", &pgconn.PgError{Code: "40P01"})
	if !retention.LockConflict(deadlock) {
		t.Fatal("deadlock was not a lock conflict")
	}
	timeout := &pgconn.PgError{Code: "55P03"}
	if !retention.LockConflict(timeout) {
		t.Fatal("lock_timeout was not a lock conflict")
	}
	if retention.LockConflict(&pgconn.PgError{Code: "57014"}) {
		t.Fatal("statement timeout was treated as a lock conflict")
	}
}

func TestPurgeBridgedEvents(t *testing.T) {
	ctx, pool, s := setupRetention(t)
	seen := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	// Author A is fully bridged: a kind-0 and two notes, all with proxy tags.
	insertEvent(t, ctx, pool, "a_meta", "A", 0, 100, seen)
	insertEventAttributes(t, ctx, pool, "a_meta", "activitypub", "https://mastodon.example/users/a")
	insertEvent(t, ctx, pool, "a_note1", "A", 1, 101, seen)
	insertEventAttributes(t, ctx, pool, "a_note1", "activitypub", "https://mastodon.example/a/1")
	insertEvent(t, ctx, pool, "a_note2", "A", 1, 102, seen)
	insertEventAttributes(t, ctx, pool, "a_note2", "", "https://mastodon.example/a/2") // proxy_id only still counts
	insertAuthorProjections(t, ctx, pool, "A")

	// Author B has one bridged note and one native note: projections stay.
	insertEvent(t, ctx, pool, "b_bridged", "B", 1, 200, seen)
	insertEventAttributes(t, ctx, pool, "b_bridged", "rss", "https://feed.example/1")
	insertEvent(t, ctx, pool, "b_native", "B", 1, 201, seen)
	insertEventAttributes(t, ctx, pool, "b_native", "", "")
	insertAuthorProjections(t, ctx, pool, "B")

	// Author C is native with no event_attributes row at all.
	insertEvent(t, ctx, pool, "c_native", "C", 1, 300, seen)
	insertAuthorProjections(t, ctx, pool, "C")

	// Batch of 3 out of 4 bridged rows: first call is a full batch.
	first, err := s.PurgeBridgedEvents(ctx, 3)
	if err != nil {
		t.Fatalf("first purge: %v", err)
	}
	if first.Deleted != 3 {
		t.Fatalf("first batch deleted %d, want 3", first.Deleted)
	}

	second, err := s.PurgeBridgedEvents(ctx, 3)
	if err != nil {
		t.Fatalf("second purge: %v", err)
	}
	if second.Deleted != 1 {
		t.Fatalf("second batch deleted %d, want 1", second.Deleted)
	}

	orphaned := append(append([]string{}, first.OrphanedAuthors...), second.OrphanedAuthors...)
	sort.Strings(orphaned)
	if len(orphaned) != 1 || orphaned[0] != "A" {
		t.Fatalf("orphaned authors = %v, want [A]", orphaned)
	}

	got := remainingEventIDs(t, ctx, pool)
	want := []string{"b_native", "c_native"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("remaining events = %v, want %v", got, want)
	}
	if n := countAuthorProjections(t, ctx, pool, "A"); n != 0 {
		t.Fatalf("author A still has %d projection rows", n)
	}
	for _, pk := range []string{"B", "C"} {
		if n := countAuthorProjections(t, ctx, pool, pk); n != 4 {
			t.Fatalf("author %s has %d projection rows, want 4", pk, n)
		}
	}

	// Drained: the idle probe finds nothing and reports no authors.
	idle, err := s.PurgeBridgedEvents(ctx, 3)
	if err != nil {
		t.Fatalf("idle purge: %v", err)
	}
	if idle.Deleted != 0 || len(idle.OrphanedAuthors) != 0 {
		t.Fatalf("idle purge = %+v, want empty", idle)
	}
}
