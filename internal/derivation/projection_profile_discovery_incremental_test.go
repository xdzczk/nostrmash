package derivation_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestIncrementalProfileDiscoveryStats_RollsFromDailyTables(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:    boolPtr(true),
		IncrementalAuthorActivityDaily:   boolPtr(true),
		IncrementalWindowedRollups:       boolPtr(true),
		IncrementalProfileDiscoveryStats: boolPtr(true),
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("inc_disc_note", "disc_author", now.Add(-2*time.Hour).Unix(), 1, nil, "note", now.Add(-2*time.Hour)),
		newEventForTest("inc_disc_reply", "disc_replier", now.Add(-90*time.Minute).Unix(), 1, [][]string{{"e", "inc_disc_note", "", "reply"}}, "reply", now.Add(-90*time.Minute)),
		newEventForTest("inc_disc_react", "disc_reactor", now.Add(-80*time.Minute).Unix(), 7, [][]string{{"e", "inc_disc_note"}}, "+", now.Add(-80*time.Minute)),
		newEventForTest("inc_disc_zap", "disc_zapper", now.Add(-70*time.Minute).Unix(), 9735, [][]string{
			{"p", "disc_author"},
			{"e", "inc_disc_note"},
			{"amount", "21000"},
		}, "zap", now.Add(-70*time.Minute)),
	}
	for i := 0; i < 5; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("inc_disc_follow_%d", i),
			fmt.Sprintf("inc_disc_follower_%d", i),
			now.Add(-60*time.Minute).Unix(),
			3,
			[][]string{{"p", "disc_author"}},
			"contacts",
			now.Add(-60*time.Minute),
		))
	}

	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	var (
		score24h, rising24h                              float64
		posts, replies, engagement, zapMsats, activeDays int64
		recentActivityAt                                 *int64
	)
	if err := pool.QueryRow(ctx, `
		SELECT score_24h, rising_score_24h, recent_post_count, recent_reply_count,
		       recent_engagement_received, recent_zap_volume_msats, recent_active_days,
		       recent_activity_at
		FROM profile_discovery_stats
		WHERE pubkey = 'disc_author'
	`).Scan(&score24h, &rising24h, &posts, &replies, &engagement, &zapMsats, &activeDays, &recentActivityAt); err != nil {
		t.Fatalf("query profile_discovery_stats: %v", err)
	}
	if score24h <= 0 || rising24h <= 0 {
		t.Fatalf("expected positive discovery scores, got score=%f rising=%f", score24h, rising24h)
	}
	if posts != 1 {
		t.Fatalf("expected recent_post_count=1, got %d", posts)
	}
	if replies != 0 {
		t.Fatalf("expected author recent_reply_count=0, got %d", replies)
	}
	if engagement < 3 {
		t.Fatalf("expected engagement >= 3 (reply+reaction+zap), got %d", engagement)
	}
	// amount tag "21000" is millisats → parseZapAmountSats → 21 sats → *1000 msats.
	if zapMsats != 21_000 {
		t.Fatalf("expected zap volume 21 sats in msats, got %d", zapMsats)
	}
	if activeDays < 1 {
		t.Fatalf("expected active_days >= 1, got %d", activeDays)
	}
	if recentActivityAt == nil || *recentActivityAt <= 0 {
		t.Fatalf("expected recent_activity_at to be set, got %#v", recentActivityAt)
	}

	countGains := func(pubkey string) int64 {
		t.Helper()
		var gained int64
		if err := pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM follower_gain_events WHERE followed_pubkey = $1
		`, pubkey).Scan(&gained); err != nil {
			t.Fatalf("query follower_gain_events: %v", err)
		}
		return gained
	}
	if gained := countGains("disc_author"); gained != 5 {
		t.Fatalf("expected 5 follower gains, got %d", gained)
	}

	// A contact-list rewrite that still follows disc_author must not
	// re-count as a new follower: only the genuinely new edge gains.
	rewrite := newEventForTest(
		"inc_disc_follow_0_rewrite",
		"inc_disc_follower_0",
		now.Add(-30*time.Minute).Unix(),
		3,
		[][]string{{"p", "disc_author"}, {"p", "disc_other"}},
		"contacts",
		now.Add(-30*time.Minute),
	)
	if err := pgStore.InsertCanonicalEvent(ctx, rewrite, extractTagsFromRaw(t, rewrite.RawJSON), "wss://relay.one", rewrite.FirstSeenAt); err != nil {
		t.Fatalf("insert rewrite event: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, rewrite.ID); err != nil {
		t.Fatalf("derive rewrite bundle: %v", err)
	}
	if gained := countGains("disc_author"); gained != 5 {
		t.Fatalf("expected rewrite to not re-count disc_author gains, got %d", gained)
	}
	if gained := countGains("disc_other"); gained != 1 {
		t.Fatalf("expected 1 gain for newly-followed disc_other, got %d", gained)
	}

	// Unfollow then refollow inside the retention horizon dedupes on the
	// (followed, follower) primary key: no second gain row.
	unfollow := newEventForTest(
		"inc_disc_follow_0_unfollow",
		"inc_disc_follower_0",
		now.Add(-20*time.Minute).Unix(),
		3,
		[][]string{{"p", "disc_other"}},
		"contacts",
		now.Add(-20*time.Minute),
	)
	refollow := newEventForTest(
		"inc_disc_follow_0_refollow",
		"inc_disc_follower_0",
		now.Add(-10*time.Minute).Unix(),
		3,
		[][]string{{"p", "disc_author"}, {"p", "disc_other"}},
		"contacts",
		now.Add(-10*time.Minute),
	)
	for _, event := range []model.Event{unfollow, refollow} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	if gained := countGains("disc_author"); gained != 5 {
		t.Fatalf("expected refollow churn to dedupe, got %d gains", gained)
	}

	var discoveryRecent int64
	if err := pool.QueryRow(ctx, `
		SELECT recent_activity_at FROM profile_discovery_recent_activity WHERE pubkey = 'disc_author'
	`).Scan(&discoveryRecent); err != nil {
		t.Fatalf("query profile_discovery_recent_activity: %v", err)
	}
	if discoveryRecent <= 0 {
		t.Fatalf("expected discovery recent activity row, got %d", discoveryRecent)
	}
}

func TestIncrementalProfileDiscoveryStats_FlagOffUsesFullScan(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfilePublicStats:    boolPtr(true),
		IncrementalAuthorActivityDaily:   boolPtr(true),
		IncrementalWindowedRollups:       boolPtr(true),
		IncrementalProfileDiscoveryStats: boolPtr(false),
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	note := newEventForTest("flag_off_note", "flag_off_author", now.Add(-1*time.Hour).Unix(), 1, nil, "note", now.Add(-1*time.Hour))
	reply := newEventForTest("flag_off_reply", "flag_off_replier", now.Add(-50*time.Minute).Unix(), 1, [][]string{{"e", "flag_off_note", "", "reply"}}, "reply", now.Add(-50*time.Minute))
	for _, event := range []model.Event{note, reply} {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	var score24h float64
	var engagement int64
	if err := pool.QueryRow(ctx, `
		SELECT score_24h, recent_engagement_received
		FROM profile_discovery_stats
		WHERE pubkey = 'flag_off_author'
	`).Scan(&score24h, &engagement); err != nil {
		t.Fatalf("query profile_discovery_stats: %v", err)
	}
	if score24h <= 0 || engagement < 1 {
		t.Fatalf("expected flag-off full scan to populate discovery stats, got score=%f engagement=%d", score24h, engagement)
	}
}
