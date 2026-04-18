package derivation_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectAuthorAnalytics_WindowedStatsAndRebuild(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	target := newEventForTest("aa_target", "bob", now.Add(-48*time.Hour).Unix(), 1, nil, `{"content":"seed"}`, now)
	aliceNote := newEventForTest(
		"aa_alice_note",
		"alice",
		now.Add(-24*time.Hour).Unix(),
		1,
		[][]string{{"t", "ai"}, {"image", "https://cdn/img.png"}},
		`{"content":"image note"}`,
		now,
	)
	aliceReply := newEventForTest(
		"aa_alice_reply",
		"alice",
		now.Add(-23*time.Hour).Unix(),
		1,
		[][]string{{"e", "aa_target", "", "reply"}, {"t", "nostr"}},
		`{"content":"reply"}`,
		now,
	)
	bobReply := newEventForTest(
		"aa_bob_reply",
		"bob",
		now.Add(-22*time.Hour).Unix(),
		1,
		[][]string{{"e", "aa_alice_note", "", "reply"}},
		`{"content":"nice post"}`,
		now,
	)
	bobReaction := newEventForTest(
		"aa_bob_reaction",
		"bob",
		now.Add(-21*time.Hour).Unix(),
		7,
		[][]string{{"e", "aa_alice_note"}},
		`{"content":"+"}`,
		now,
	)
	bobRepost := newEventForTest(
		"aa_bob_repost",
		"bob",
		now.Add(-20*time.Hour).Unix(),
		6,
		[][]string{{"e", "aa_alice_note"}},
		`{"content":"repost"}`,
		now,
	)

	for _, event := range []model.Event{target, aliceNote, aliceReply, bobReply, bobReaction, bobRepost} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingAuthorAnalyticsForTest(t, ctx, handlers)

	var postCount int64
	var noteCount int64
	var replyCount int64
	var received int64
	var given int64
	if err := pool.QueryRow(ctx, `
		SELECT post_count, note_count, reply_count, engagement_received, engagement_given
		FROM author_engagement_stats
		WHERE pubkey = 'alice' AND window_days = 7
	`).Scan(&postCount, &noteCount, &replyCount, &received, &given); err != nil {
		t.Fatalf("query author_engagement_stats: %v", err)
	}
	if postCount != 2 || noteCount != 1 || replyCount != 1 {
		t.Fatalf("unexpected 7d post counts: post=%d note=%d reply=%d", postCount, noteCount, replyCount)
	}
	if received < 3 {
		t.Fatalf("expected engagement_received >= 3, got %d", received)
	}
	if given < 1 {
		t.Fatalf("expected engagement_given >= 1, got %d", given)
	}

	var aiCount int64
	if err := pool.QueryRow(ctx, `
		SELECT usage_count
		FROM author_topic_stats
		WHERE pubkey = 'alice' AND window_days = 7 AND hashtag = 'ai'
	`).Scan(&aiCount); err != nil {
		t.Fatalf("query author_topic_stats ai: %v", err)
	}
	if aiCount != 1 {
		t.Fatalf("unexpected ai usage_count: got=%d want=1", aiCount)
	}

	var totalPosts int64
	var imageCount int64
	var articleCount int64
	var textOnlyCount int64
	var attachmentTotal int64
	if err := pool.QueryRow(ctx, `
		SELECT total_posts, with_image_count, with_article_count, text_only_count, total_attachment_count
		FROM author_media_mix_stats
		WHERE pubkey = 'alice' AND window_days = 7
	`).Scan(&totalPosts, &imageCount, &articleCount, &textOnlyCount, &attachmentTotal); err != nil {
		t.Fatalf("query author_media_mix_stats: %v", err)
	}
	if totalPosts != 2 || imageCount != 1 || articleCount != 0 || textOnlyCount != 1 || attachmentTotal != 1 {
		t.Fatalf(
			"unexpected media mix counts: total=%d image=%d article=%d text_only=%d attachments=%d",
			totalPosts,
			imageCount,
			articleCount,
			textOnlyCount,
			attachmentTotal,
		)
	}

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationAuthorActivityDaily,
		TargetVersion:  2,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger author analytics rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute author analytics rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)

	var version int
	if err := pool.QueryRow(ctx, `
		SELECT derivation_version
		FROM author_engagement_stats
		WHERE pubkey = 'alice' AND window_days = 7
	`).Scan(&version); err != nil {
		t.Fatalf("query author_engagement_stats version: %v", err)
	}
	if version != 2 {
		t.Fatalf("unexpected author_engagement_stats derivation_version: got=%d want=2", version)
	}
}

func TestProjectAuthorAnalytics_ActivityAndPostingBuckets_AreCorrectAndBounded(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	now := time.Now().UTC()
	postAt := now.Add(-48 * time.Hour).Truncate(time.Hour).Add(9 * time.Hour)
	replyAt := now.Add(-47 * time.Hour).Truncate(time.Hour).Add(21 * time.Hour)
	engageAt := now.Add(-46 * time.Hour).Truncate(time.Hour).Add(10 * time.Hour)

	target := newEventForTest("aa_target_bucket", "bob", now.Add(-72*time.Hour).Unix(), 1, nil, `{"content":"seed"}`, now)
	aliceNote := newEventForTest("aa_bucket_note", "alice", postAt.Unix(), 1, nil, `{"content":"note"}`, now)
	aliceReply := newEventForTest(
		"aa_bucket_reply",
		"alice",
		replyAt.Unix(),
		1,
		[][]string{{"e", "aa_target_bucket", "", "reply"}},
		`{"content":"reply"}`,
		now,
	)
	bobReply := newEventForTest(
		"aa_bucket_bob_reply",
		"bob",
		engageAt.Unix(),
		1,
		[][]string{{"e", "aa_bucket_note", "", "reply"}},
		`{"content":"nice"}`,
		now,
	)
	bobReaction := newEventForTest(
		"aa_bucket_bob_reaction",
		"bob",
		engageAt.Unix(),
		7,
		[][]string{{"e", "aa_bucket_note"}},
		`{"content":"+"}`,
		now,
	)
	bobRepost := newEventForTest(
		"aa_bucket_bob_repost",
		"bob",
		engageAt.Add(time.Hour).Unix(),
		6,
		[][]string{{"e", "aa_bucket_note"}},
		`{"content":"repost"}`,
		now,
	)

	for _, event := range []model.Event{target, aliceNote, aliceReply, bobReply, bobReaction, bobRepost} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingAuthorAnalyticsForTest(t, ctx, handlers)

	postDay := int(postAt.Weekday())
	postHour := postAt.Hour()
	replyDay := int(replyAt.Weekday())
	replyHour := replyAt.Hour()
	engageDay := int(engageAt.Weekday())
	engageHour := engageAt.Hour()

	var postingPostCount int64
	var postingNoteCount int64
	if err := pool.QueryRow(ctx, `
		SELECT post_count, note_count
		FROM author_posting_patterns
		WHERE pubkey = 'alice' AND window_days = 7 AND day_of_week = $1 AND hour_of_day = $2
	`, postDay, postHour).Scan(&postingPostCount, &postingNoteCount); err != nil {
		t.Fatalf("query posting note bucket: %v", err)
	}
	if postingPostCount != 1 || postingNoteCount != 1 {
		t.Fatalf("unexpected posting note bucket counts: post=%d note=%d", postingPostCount, postingNoteCount)
	}

	var postingReplyCount int64
	if err := pool.QueryRow(ctx, `
		SELECT reply_count
		FROM author_posting_patterns
		WHERE pubkey = 'alice' AND window_days = 7 AND day_of_week = $1 AND hour_of_day = $2
	`, replyDay, replyHour).Scan(&postingReplyCount); err != nil {
		t.Fatalf("query posting reply bucket: %v", err)
	}
	if postingReplyCount != 1 {
		t.Fatalf("unexpected posting reply bucket count: got=%d want=1", postingReplyCount)
	}

	var engagementReceived int64
	var engagementReply int64
	var engagementReaction int64
	if err := pool.QueryRow(ctx, `
		SELECT engagement_received, reply_received, reaction_received
		FROM author_activity_windows
		WHERE pubkey = 'alice' AND window_days = 7 AND day_of_week = $1 AND hour_of_day = $2
	`, engageDay, engageHour).Scan(&engagementReceived, &engagementReply, &engagementReaction); err != nil {
		t.Fatalf("query activity bucket: %v", err)
	}
	if engagementReceived < 2 || engagementReply != 1 || engagementReaction != 1 {
		t.Fatalf(
			"unexpected engagement bucket counts: received=%d reply=%d reaction=%d",
			engagementReceived,
			engagementReply,
			engagementReaction,
		)
	}

	activityBuckets, err := pgStore.GetAuthorActivityWindowBuckets(ctx, "alice", 7)
	if err != nil {
		t.Fatalf("GetAuthorActivityWindowBuckets: %v", err)
	}
	if len(activityBuckets) != 7*24 {
		t.Fatalf("unexpected activity bucket count: got=%d want=%d", len(activityBuckets), 7*24)
	}
	postingBuckets, err := pgStore.GetAuthorPostingPatternBuckets(ctx, "alice", 7)
	if err != nil {
		t.Fatalf("GetAuthorPostingPatternBuckets: %v", err)
	}
	if len(postingBuckets) != 7*24 {
		t.Fatalf("unexpected posting bucket count: got=%d want=%d", len(postingBuckets), 7*24)
	}
}
