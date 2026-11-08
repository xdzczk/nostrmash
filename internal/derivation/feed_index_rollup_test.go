package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestBackfillAuthorFeedIndexSkipsRepliesAndCompletes(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)

	insert := func(id, pubkey string, kind int, created int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			VALUES ($1, $2, $3, $4, 'sig', 'hello', '{}')
		`, id, pubkey, created, kind); err != nil {
			t.Fatalf("insert %s: %v", id, err)
		}
	}
	insert("note", "author", 1, 300)
	insert("reply", "author", 1, 200)
	insert("parent", "author", 1, 100)
	insert("repost", "author", 6, 250)
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id, derivation_version
		) VALUES ('reply', 200, 'parent', 'parent', 1)
	`); err != nil {
		t.Fatalf("insert thread edge: %v", err)
	}

	var more bool
	for i := 0; i < 5; i++ {
		advanced, err := handlers.BackfillAuthorFeedIndex(ctx)
		if err != nil {
			t.Fatalf("backfill: %v", err)
		}
		more = advanced
		if !advanced {
			break
		}
	}
	if more {
		t.Fatal("backfill did not finish")
	}
	var indexed []string
	rows, err := pool.Query(ctx, `SELECT event_id FROM author_feed_events ORDER BY event_id`)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan index: %v", err)
		}
		indexed = append(indexed, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read index rows: %v", err)
	}
	if len(indexed) != 3 || indexed[0] != "note" || indexed[1] != "parent" || indexed[2] != "repost" {
		t.Fatalf("indexed = %#v", indexed)
	}
	var complete bool
	if err := pool.QueryRow(ctx, `SELECT complete FROM author_feed_index_state WHERE id`).Scan(&complete); err != nil {
		t.Fatalf("read index state: %v", err)
	}
	if !complete {
		t.Fatal("index was not marked complete")
	}
}

func TestAdvanceDiscoveryDailyRollupSealsYesterday(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)

	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	created := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 12, 0, 0, 0, time.UTC).Unix()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ('noted', 'author', $1, 1, 'sig', '#nostr', '{}')
	`, created); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES ('noted', 'author', $1, 'nostr', 1)
	`, created); err != nil {
		t.Fatalf("insert hashtag: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO event_urls (
			event_id, author_pubkey, created_at, url, domain, canonical_domain, derivation_version, is_media
		) VALUES (
			'noted', 'author', $1, 'https://example.com/a', 'example.com', 'example.com', 1, false
		)
	`, created); err != nil {
		t.Fatalf("insert url: %v", err)
	}

	for i := 0; i < 80; i++ {
		more, err := handlers.AdvanceDiscoveryDailyRollup(ctx)
		if err != nil {
			t.Fatalf("advance rollup: %v", err)
		}
		if !more {
			var events int
			if err := pool.QueryRow(ctx, `
				SELECT events
				FROM hashtag_author_daily
				WHERE hashtag = 'nostr' AND author_pubkey = 'author'
			`).Scan(&events); err != nil {
				t.Fatalf("read daily hashtag: %v", err)
			}
			if events != 1 {
				t.Fatalf("daily events = %d", events)
			}
			var notes int
			if err := pool.QueryRow(ctx, `
				SELECT notes
				FROM domain_author_daily
				WHERE canonical_domain = 'example.com' AND author_pubkey = 'author'
			`).Scan(&notes); err != nil {
				t.Fatalf("read daily domain: %v", err)
			}
			if notes != 1 {
				t.Fatalf("daily notes = %d", notes)
			}
			return
		}
	}
	t.Fatal("rollup did not catch up")
}
