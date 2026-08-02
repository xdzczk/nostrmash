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

func boolPtr(v bool) *bool { return &v }

func TestApplyIncrementalAuthorStats_ProfileAndActivityDaily(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:  boolPtr(true),
		IncrementalAuthorActivityDaily: boolPtr(true),
		IncrementalWindowedRollups:     boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	target := newEventForTest("inc_target", "bob", now.Add(-2*time.Hour).Unix(), 1, nil, `{"content":"seed"}`, now)
	aliceNote := newEventForTest(
		"inc_alice_note",
		"alice",
		now.Add(-1*time.Hour).Unix(),
		1,
		[][]string{{"t", "ai"}},
		`{"content":"note"}`,
		now,
	)
	aliceReply := newEventForTest(
		"inc_alice_reply",
		"alice",
		now.Add(-50*time.Minute).Unix(),
		1,
		[][]string{{"e", "inc_target", "", "reply"}},
		`{"content":"reply"}`,
		now,
	)
	bobReaction := newEventForTest(
		"inc_bob_reaction",
		"bob",
		now.Add(-40*time.Minute).Unix(),
		7,
		[][]string{{"e", "inc_alice_note"}},
		`{"content":"+"}`,
		now,
	)
	bobFollowsAlice := newEventForTest(
		"inc_bob_contacts",
		"bob",
		now.Add(-30*time.Minute).Unix(),
		3,
		[][]string{{"p", "alice"}},
		"",
		now,
	)

	for _, event := range []model.Event{target, aliceNote, aliceReply, bobReaction, bobFollowsAlice} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	aliceStats := mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.NoteCount != 1 || aliceStats.ReplyCount != 1 {
		t.Fatalf("unexpected alice note/reply stats: %#v", aliceStats)
	}
	if aliceStats.FollowerCount != 1 {
		t.Fatalf("expected alice follower_count=1, got %#v", aliceStats)
	}

	var postCount, noteCount, replyCount, received, given int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(post_count),0), COALESCE(SUM(note_count),0), COALESCE(SUM(reply_count),0),
		       COALESCE(SUM(engagement_received),0), COALESCE(SUM(engagement_given),0)
		FROM author_activity_daily
		WHERE pubkey = 'alice'
	`).Scan(&postCount, &noteCount, &replyCount, &received, &given); err != nil {
		t.Fatalf("query author_activity_daily: %v", err)
	}
	if postCount != 2 || noteCount != 1 || replyCount != 1 {
		t.Fatalf("unexpected alice activity daily posts: post=%d note=%d reply=%d", postCount, noteCount, replyCount)
	}
	if received != 1 {
		t.Fatalf("expected alice engagement_received=1 from bob reaction, got %d", received)
	}
	if given != 1 {
		t.Fatalf("expected alice engagement_given=1 from reply, got %d", given)
	}

	var hashtagUsage int64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(usage_count),0) FROM author_hashtag_daily WHERE pubkey = 'alice' AND hashtag = 'ai'
	`).Scan(&hashtagUsage); err != nil {
		t.Fatalf("query author_hashtag_daily: %v", err)
	}
	if hashtagUsage != 1 {
		t.Fatalf("expected hashtag usage=1, got %d", hashtagUsage)
	}

	// Idempotency: re-running the bundle must not double-count.
	if err := handlers.DeriveEventBundle(ctx, aliceNote.ID); err != nil {
		t.Fatalf("re-derive alice note: %v", err)
	}
	aliceStats = mustReadProfilePublicStats(t, ctx, pgStore, "alice")
	if aliceStats.NoteCount != 1 || aliceStats.ReplyCount != 1 {
		t.Fatalf("idempotency failed for profile stats: %#v", aliceStats)
	}
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(post_count),0) FROM author_activity_daily WHERE pubkey = 'alice'
	`).Scan(&postCount); err != nil {
		t.Fatalf("query author_activity_daily after retry: %v", err)
	}
	if postCount != 2 {
		t.Fatalf("idempotency failed for activity daily post_count: %d", postCount)
	}
}

func TestApplyIncrementalAuthorStats_WindowedRollupsFromDaily(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:  boolPtr(true),
		IncrementalAuthorActivityDaily: boolPtr(true),
		IncrementalWindowedRollups:     boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	aliceNote := newEventForTest(
		"inc_roll_note",
		"alice",
		now.Add(-2*time.Hour).Unix(),
		1,
		[][]string{{"t", "nostr"}, {"image", "https://cdn/img.png"}},
		`{"content":"image note"}`,
		now,
	)
	bobReply := newEventForTest(
		"inc_roll_reply",
		"bob",
		now.Add(-1*time.Hour).Unix(),
		1,
		[][]string{{"e", "inc_roll_note", "", "reply"}},
		`{"content":"nice"}`,
		now,
	)

	for _, event := range []model.Event{aliceNote, bobReply} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingAuthorAnalyticsForTest(t, ctx, handlers)

	var postCount, noteCount, received int64
	if err := pool.QueryRow(ctx, `
		SELECT post_count, note_count, engagement_received
		FROM author_engagement_stats
		WHERE pubkey = 'alice' AND window_days = 7
	`).Scan(&postCount, &noteCount, &received); err != nil {
		t.Fatalf("query author_engagement_stats: %v", err)
	}
	if postCount != 1 || noteCount != 1 {
		t.Fatalf("unexpected engagement posts: post=%d note=%d", postCount, noteCount)
	}
	if received < 1 {
		t.Fatalf("expected engagement_received >= 1, got %d", received)
	}

	var topicUsage int64
	if err := pool.QueryRow(ctx, `
		SELECT usage_count FROM author_topic_stats
		WHERE pubkey = 'alice' AND window_days = 7 AND hashtag = 'nostr'
	`).Scan(&topicUsage); err != nil {
		t.Fatalf("query author_topic_stats: %v", err)
	}
	if topicUsage != 1 {
		t.Fatalf("expected topic usage=1, got %d", topicUsage)
	}
}
