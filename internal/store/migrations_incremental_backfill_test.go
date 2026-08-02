package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/migrations"
)

// TestIncrementalStatDailyBackfillMigrationSeedsFineGrainedTables is the
// migration-content regression test for
// migrations/000064_backfill_incremental_stat_daily_tables.sql, following
// the same rerun-the-migration-SQL pattern as
// TestCanonicalDomainMigrationBackfillsExistingAliases.
//
// It seeds raw source-of-truth rows (events, event_hashtags,
// note_discovery_stats, event_references, reaction_events) exactly like
// production data that predates the incremental-stats writer, re-executes
// the backfill migration's SQL text, and asserts the three fine-grained
// daily tables it targets (author_hashtag_daily, author_media_daily,
// author_hourly_activity) end up with the correct aggregated rows. This is
// the regression test for the "windowed rollups silently undercount
// pre-deploy history" gap the migration exists to close.
func TestIncrementalStatDailyBackfillMigrationSeedsFineGrainedTables(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	now := time.Now().Unix()
	aliceNoteAt := now - 3600 // 1h ago: within the 90-day backfill window.
	bobReplyAt := now - 1800  // 30m ago.

	reactionAt := now - 900 // 15m ago.
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('bf_alice_note', 'alice', $1, 1, 'sig', 'note', '{}'),
			('bf_bob_reply', 'bob', $2, 1, 'sig', 'reply', '{}'),
			('bf_carol_reaction', 'carol', $3, 7, 'sig', '+', '{}')
	`, aliceNoteAt, bobReplyAt, reactionAt); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO event_hashtags (event_id, author_pubkey, created_at, hashtag, derivation_version)
		VALUES ('bf_alice_note', 'alice', $1, 'nostr', 1)
	`, aliceNoteAt); err != nil {
		t.Fatalf("seed event_hashtags: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO note_discovery_stats (
			event_id, author_pubkey, created_at, has_image, has_video, has_link, has_article,
			attachment_count, derivation_version
		)
		VALUES ('bf_alice_note', 'alice', $1, true, false, false, false, 1, 1)
	`, aliceNoteAt); err != nil {
		t.Fatalf("seed note_discovery_stats: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO event_references (source_event_id, tag_index, referenced_event_id, relation, derivation_version)
		VALUES ('bf_bob_reply', 0, 'bf_alice_note', 'reply', 1)
	`); err != nil {
		t.Fatalf("seed event_references: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO reaction_events (event_id, target_event_id, reactor_pubkey, content, created_at, derivation_version)
		VALUES ('bf_carol_reaction', 'bf_alice_note', 'carol', '+', $1, 1)
	`, reactionAt); err != nil {
		t.Fatalf("seed reaction_events: %v", err)
	}

	migrationSQL, err := migrations.Files.ReadFile("000064_backfill_incremental_stat_daily_tables.sql")
	if err != nil {
		t.Fatalf("read incremental-stat backfill migration: %v", err)
	}
	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("rerun incremental-stat backfill migration: %v", err)
	}

	var hashtagUsage int64
	if err := pool.QueryRow(ctx, `
		SELECT usage_count FROM author_hashtag_daily
		WHERE pubkey = 'alice' AND hashtag = 'nostr'
	`).Scan(&hashtagUsage); err != nil {
		t.Fatalf("query backfilled author_hashtag_daily: %v", err)
	}
	if hashtagUsage != 1 {
		t.Fatalf("expected backfilled hashtag usage_count=1, got %d", hashtagUsage)
	}

	var totalPosts, withImage int64
	if err := pool.QueryRow(ctx, `
		SELECT total_posts, with_image_count FROM author_media_daily WHERE pubkey = 'alice'
	`).Scan(&totalPosts, &withImage); err != nil {
		t.Fatalf("query backfilled author_media_daily: %v", err)
	}
	if totalPosts != 1 || withImage != 1 {
		t.Fatalf("expected backfilled media total_posts=1 with_image_count=1, got total=%d image=%d", totalPosts, withImage)
	}

	var alicePostCount int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(post_count), 0) FROM author_hourly_activity WHERE pubkey = 'alice'
	`).Scan(&alicePostCount); err != nil {
		t.Fatalf("query backfilled author_hourly_activity authored side: %v", err)
	}
	if alicePostCount != 1 {
		t.Fatalf("expected backfilled alice post_count=1, got %d", alicePostCount)
	}

	var aliceReceived, aliceReplyReceived, aliceReactionReceived int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(engagement_received), 0), COALESCE(SUM(reply_received), 0), COALESCE(SUM(reaction_received), 0)
		FROM author_hourly_activity WHERE pubkey = 'alice'
	`).Scan(&aliceReceived, &aliceReplyReceived, &aliceReactionReceived); err != nil {
		t.Fatalf("query backfilled author_hourly_activity received side: %v", err)
	}
	if aliceReceived != 2 || aliceReplyReceived != 1 || aliceReactionReceived != 1 {
		t.Fatalf(
			"expected backfilled alice engagement_received=2 (1 reply + 1 reaction), got received=%d reply=%d reaction=%d",
			aliceReceived, aliceReplyReceived, aliceReactionReceived,
		)
	}

	// Re-running the migration must be idempotent (overwrite, not add).
	if _, err := pool.Exec(ctx, string(migrationSQL)); err != nil {
		t.Fatalf("rerun incremental-stat backfill migration a second time: %v", err)
	}
	if err := pool.QueryRow(ctx, `
		SELECT usage_count FROM author_hashtag_daily WHERE pubkey = 'alice' AND hashtag = 'nostr'
	`).Scan(&hashtagUsage); err != nil {
		t.Fatalf("query backfilled author_hashtag_daily after rerun: %v", err)
	}
	if hashtagUsage != 1 {
		t.Fatalf("expected idempotent rerun to leave usage_count=1, got %d", hashtagUsage)
	}
}
