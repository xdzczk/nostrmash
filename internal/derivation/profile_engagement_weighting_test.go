package derivation

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/dbmigrate"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
)

// Pages of two must still return every in-window reply, including two replies
// that share a timestamp, and must keep a reply to a non-root note out of the
// trending flags without dropping the replies that sort after it.
func TestLoadProfileReplyVotesPageTx_WalksEveryPage(t *testing.T) {
	ctx := context.Background()
	dbURL := dbtest.DatabaseURL(t, "profile reply pages")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "profile_reply_pages")
	if err := dbmigrate.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	now := time.Now().UTC()
	rootAt := now.Add(-2 * time.Hour).Unix()
	replyAt := now.Add(-30 * time.Minute).Unix()
	author := "page_author"

	events := []struct {
		id, pubkey, content string
		created             int64
	}{
		{"page_root", author, "root", rootAt},
		{"page_child", author, "child note", replyAt},
		{"page_r1", "replier_a", "alpha", replyAt},
		{"page_r2", "replier_b", "beta", replyAt},
		{"page_r3", "replier_c", "gamma", replyAt + 1},
		{"page_r4", "replier_d", "delta", replyAt + 2},
		{"page_r5", "replier_e", "epsilon", replyAt + 3},
		{"page_self", author, "self", replyAt},
		{"page_old", "replier_old", "stale", now.Add(-40 * 24 * time.Hour).Unix()},
	}
	for _, event := range events {
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
			VALUES ($1, $2, $3, 1, 'sig', $4, '{}'::jsonb)
		`, event.id, event.pubkey, event.created, event.content); err != nil {
			t.Fatalf("insert event %s: %v", event.id, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO thread_edges (child_event_id, child_created_at, parent_event_id, derivation_version)
		VALUES ('page_child', $1, 'page_root', 1)
	`, replyAt); err != nil {
		t.Fatalf("insert thread edge: %v", err)
	}

	type contribution struct {
		source, target, pubkey string
		created                int64
	}
	contributions := []contribution{
		{"page_r1", "page_root", "replier_a", replyAt},
		{"page_r2", "page_root", "replier_b", replyAt},
		{"page_r3", "page_root", "replier_c", replyAt + 1},
		{"page_r4", "page_child", "replier_d", replyAt + 2},
		{"page_r5", "page_root", "replier_e", replyAt + 3},
		{"page_self", "page_root", author, replyAt},
		{"page_old", "page_root", "replier_old", now.Add(-40 * 24 * time.Hour).Unix()},
	}
	for _, row := range contributions {
		if _, err := pool.Exec(ctx, `
			INSERT INTO reply_count_contributions (
				source_event_id, target_event_id, derivation_version,
				source_pubkey, source_created_at, target_pubkey
			) VALUES ($1, $2, 1, $3, $4, $5)
		`, row.source, row.target, row.pubkey, row.created, author); err != nil {
			t.Fatalf("insert contribution %s: %v", row.source, err)
		}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	nowUnix := now.Unix()
	noteArgs := []any{
		author,
		nowUnix - int64((30 * 24 * time.Hour).Seconds()),
		nowUnix - int64((24 * time.Hour).Seconds()),
		false,
		3,
		0.0,
		nowUnix - int64((7 * 24 * time.Hour).Seconds()),
		maxSaneUnixCreatedAt,
	}
	votes, _, _, _, err := loadProfileReplyVotesPageTx(ctx, tx, noteArgs, 2)
	if err != nil {
		t.Fatalf("load profile reply votes: %v", err)
	}
	if len(votes) != 5 {
		t.Fatalf("got %d votes, want 5 across pages of 2", len(votes))
	}
	got := map[string]profileVote{}
	for _, vote := range votes {
		got[vote.Content] = vote
		if vote.Weight != 1 {
			t.Fatalf("content %q weight = %v, want 1", vote.Content, vote.Weight)
		}
	}
	for _, content := range []string{"alpha", "beta", "gamma", "epsilon"} {
		vote, ok := got[content]
		if !ok {
			t.Fatalf("missing reply %q", content)
		}
		if !vote.Trending30d || !vote.Rising24h {
			t.Fatalf("root reply %q flags trending30=%v rising24=%v", content, vote.Trending30d, vote.Rising24h)
		}
	}
	child, ok := got["delta"]
	if !ok {
		t.Fatal("missing reply to the child note")
	}
	if child.Trending30d || child.Trending24h || child.Trending7d {
		t.Fatalf("reply to a non-root note counted as trending: %+v", child)
	}
	if !child.Rising24h {
		t.Fatal("reply to a non-root note was dropped from rising")
	}
}
