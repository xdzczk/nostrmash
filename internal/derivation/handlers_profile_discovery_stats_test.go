package derivation_test

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestProjectProfileDiscoveryStats_TracksScoresAndRisingOrder(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()

	events := []model.Event{
		newEventForTest("small_note_evt", "small_author", now.Add(-2*time.Hour).Unix(), 1, nil, "small", now.Add(-2*time.Hour)),
		newEventForTest("big_note_evt", "big_author", now.Add(-2*time.Hour).Unix(), 1, nil, "big", now.Add(-2*time.Hour)),
		newEventForTest("small_reply_evt", "small_reply", now.Add(-70*time.Minute).Unix(), 1, [][]string{{"e", "small_note_evt", "", "reply"}}, "reply", now.Add(-70*time.Minute)),
		newEventForTest("small_reaction_evt", "small_reactor", now.Add(-65*time.Minute).Unix(), 7, [][]string{{"e", "small_note_evt"}}, "+", now.Add(-65*time.Minute)),
		newEventForTest("big_reply_evt", "big_reply", now.Add(-70*time.Minute).Unix(), 1, [][]string{{"e", "big_note_evt", "", "reply"}}, "reply", now.Add(-70*time.Minute)),
		newEventForTest("big_reaction_evt", "big_reactor", now.Add(-65*time.Minute).Unix(), 7, [][]string{{"e", "big_note_evt"}}, "+", now.Add(-65*time.Minute)),
	}
	for i := 0; i < 20; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("big_follower_evt_%d", i),
			fmt.Sprintf("big_follower_%d", i),
			now.Add(-4*time.Hour).Unix(),
			3,
			[][]string{{"p", "big_author"}},
			"contacts",
			now.Add(-4*time.Hour),
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

	var smallScore24h float64
	var smallRising24h float64
	if err := pool.QueryRow(ctx, `
		SELECT score_24h, rising_score_24h
		FROM profile_discovery_stats
		WHERE pubkey = 'small_author'
	`).Scan(&smallScore24h, &smallRising24h); err != nil {
		t.Fatalf("query small_author profile discovery stats: %v", err)
	}
	if smallScore24h <= 0 || smallRising24h <= 0 {
		t.Fatalf("expected small author to have positive scores, got score=%f rising=%f", smallScore24h, smallRising24h)
	}

	var topRisingPubkey string
	if err := pool.QueryRow(ctx, `
		SELECT pubkey
		FROM profile_discovery_stats
		WHERE rising_score_24h > 0
		ORDER BY rising_score_24h DESC, pubkey ASC
		LIMIT 1
	`).Scan(&topRisingPubkey); err != nil {
		t.Fatalf("query top rising pubkey: %v", err)
	}
	// big_author gained 20 real followers; small_author only has two
	// single-day interactions. Credited follower growth must outrank a
	// tiny engagement burst.
	if topRisingPubkey != "big_author" {
		t.Fatalf("expected big_author's credited follower growth to lead the rising ranking, got %s", topRisingPubkey)
	}
}

// With trust-weighted discovery engagement enabled, a bot ring farming an
// account's profile (reactions, zaps, follower ring, self-engagement) must
// buy exactly zero trending score and no rising momentum: the farmed
// account scores the same as an identical account with no engagement at
// all, while genuinely trusted engagement still lifts the score.
func TestProjectProfileDiscoveryStats_TrustWeightedEngagement(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		EngagementWeighting: derivation.EngagementWeightingOptions{
			TrustWeighted:   true,
			UntrustedWeight: 0,
			MaxHops:         3,
		},
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	// All notes and all engagement share one timestamp so recency decay is
	// identical across authors and score differences isolate engagement.
	at := now.Add(-2 * time.Hour)

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_pubkeys_latest (pubkey, min_hops, score, rank)
		VALUES ('pw_hop1', 1, 0.5, 1)
	`); err != nil {
		t.Fatalf("seed trust_pubkeys_latest: %v", err)
	}

	events := []model.Event{
		newEventForTest("pw_note_farmed", "pw_farmed_author", at.Unix(), 1, nil, "farmed", at),
		newEventForTest("pw_note_baseline", "pw_baseline_author", at.Unix(), 1, nil, "baseline", at),
		newEventForTest("pw_note_engaged", "pw_engaged_author", at.Unix(), 1, nil, "engaged", at),
		// Bot ring engagement on the farmed profile: reactions, a zap, and a
		// self-reaction from the author. All untrusted or self => weight 0.
		newEventForTest("pw_bot_react_1", "pw_bot_1", at.Unix(), 7, [][]string{{"e", "pw_note_farmed"}}, "+", at),
		newEventForTest("pw_bot_react_2", "pw_bot_2", at.Unix(), 7, [][]string{{"e", "pw_note_farmed"}}, "+", at),
		newEventForTest("pw_self_react", "pw_farmed_author", at.Unix(), 7, [][]string{{"e", "pw_note_farmed"}}, "+", at),
		newEventForTest("pw_bot_zap", "pw_bot_3", at.Unix(), 9735, [][]string{{"e", "pw_note_farmed"}, {"p", "pw_farmed_author"}, {"amount", "900000000"}}, "", at),
		// Trusted engagement on the engaged profile: two reactions from the
		// same hop-1 pubkey on the same note dedupe to one weighted vote.
		newEventForTest("pw_hop1_react_1", "pw_hop1", at.Unix(), 7, [][]string{{"e", "pw_note_engaged"}}, "+", at),
		newEventForTest("pw_hop1_react_2", "pw_hop1", at.Unix(), 7, [][]string{{"e", "pw_note_engaged"}}, "+", at),
	}
	// Untrusted follower ring on the farmed profile.
	for i := 0; i < 6; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("pw_bot_follow_%d", i),
			fmt.Sprintf("pw_bot_follower_%d", i),
			at.Unix(),
			3,
			[][]string{{"p", "pw_farmed_author"}},
			"contacts",
			at,
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

	readStats := func(pubkey string) (score24h, rising24h float64, engagementDisplay int64, scoredEngagement, scoredFollowers float64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT score_24h, rising_score_24h, recent_engagement_received,
			       COALESCE(scored_engagement_24h, -1), COALESCE(scored_new_followers_24h, -1)
			FROM profile_discovery_stats
			WHERE pubkey = $1
		`, pubkey).Scan(&score24h, &rising24h, &engagementDisplay, &scoredEngagement, &scoredFollowers); err != nil {
			t.Fatalf("query profile discovery stats for %s: %v", pubkey, err)
		}
		return score24h, rising24h, engagementDisplay, scoredEngagement, scoredFollowers
	}

	farmedScore, farmedRising, farmedDisplay, farmedScoredEngagement, farmedScoredFollowers := readStats("pw_farmed_author")
	baselineScore, baselineRising, _, baselineScoredEngagement, _ := readStats("pw_baseline_author")
	engagedScore, _, engagedDisplay, engagedScoredEngagement, _ := readStats("pw_engaged_author")

	// Display counters keep reflecting raw (self-excluded) activity.
	if farmedDisplay != 3 {
		t.Fatalf("expected farmed display engagement=3 (2 bot reactions + 1 bot zap), got %d", farmedDisplay)
	}
	if engagedDisplay != 2 {
		t.Fatalf("expected engaged display engagement=2 raw reactions, got %d", engagedDisplay)
	}
	// Scored inputs persist the trust-weighted votes the score used.
	if farmedScoredEngagement != 0 || farmedScoredFollowers != 0 {
		t.Fatalf("expected farmed scored inputs to be 0, got engagement=%f followers=%f", farmedScoredEngagement, farmedScoredFollowers)
	}
	if baselineScoredEngagement != 0 {
		t.Fatalf("expected baseline scored engagement=0, got %f", baselineScoredEngagement)
	}
	// One trusted reaction and no reply or zap sits on the cheap floor.
	if !near(engagedScoredEngagement, 0.12) {
		t.Fatalf("expected engaged scored engagement at the cheap floor, got %f", engagedScoredEngagement)
	}
	// Bot engagement buys zero trending score: farmed == baseline.
	if diff := farmedScore - baselineScore; diff > 1e-9 || diff < -1e-9 {
		t.Fatalf("expected farmed score to equal no-engagement baseline, got farmed=%f baseline=%f", farmedScore, baselineScore)
	}
	// The untrusted follower ring must not buy rising momentum. (It may
	// even lower rising via the audience penalty on raw follower count.)
	if farmedRising > baselineRising {
		t.Fatalf("expected bot follower ring to buy no rising momentum, got farmed=%f baseline=%f", farmedRising, baselineRising)
	}
	// Trusted engagement still lifts the score.
	if engagedScore <= baselineScore {
		t.Fatalf("expected trusted engagement to outrank baseline, got engaged=%f baseline=%f", engagedScore, baselineScore)
	}

	derive := func(event model.Event) {
		t.Helper()
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	readCounts := func(pubkey string) (trusted, followers int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT trusted_follower_count, follower_count
			FROM profile_public_stats
			WHERE pubkey = $1
		`, pubkey).Scan(&trusted, &followers); err != nil {
			t.Fatalf("read follower counts for %s: %v", pubkey, err)
		}
		return trusted, followers
	}
	seedTrust := func(pubkey string, hops int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO trust_pubkeys_latest (pubkey, min_hops, score, rank)
			VALUES ($1, $2, 0.5, 1)
			ON CONFLICT (pubkey) DO UPDATE SET min_hops = EXCLUDED.min_hops
		`, pubkey, hops); err != nil {
			t.Fatalf("seed trust %s: %v", pubkey, err)
		}
	}

	derive(newEventForTest("pw_farmed_meta", "pw_farmed_author", at.Unix(), 0, nil, `{"name":"Farmed"}`, at))
	trusted, followers := readCounts("pw_farmed_author")
	if trusted != 0 || followers != 6 {
		t.Fatalf("untrusted ring trusted=%d follower_count=%d, want 0 and 6", trusted, followers)
	}
	followed, err := pgStore.GetMostFollowedProfiles(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetMostFollowedProfiles: %v", err)
	}
	for _, profile := range followed {
		if profile.Pubkey == "pw_farmed_author" {
			t.Fatalf("untrusted ring must not lead most followed: %#v", followed)
		}
	}

	seedTrust("pw_hop4", 4)
	derive(newEventForTest("pw_hop4_follow", "pw_hop4", at.Unix(), 3, [][]string{{"p", "pw_farmed_author"}}, "contacts", at))
	trusted, followers = readCounts("pw_farmed_author")
	if trusted != 0 || followers != 7 {
		t.Fatalf("hop past the limit trusted=%d follower_count=%d, want 0 and 7", trusted, followers)
	}

	seedTrust("pw_farmed_author", 1)
	derive(newEventForTest("pw_farmed_self", "pw_farmed_author", at.Unix(), 3, [][]string{{"p", "pw_farmed_author"}}, "contacts", at))
	trusted, followers = readCounts("pw_farmed_author")
	if trusted != 0 || followers != 8 {
		t.Fatalf("self-follow trusted=%d follower_count=%d, want 0 and 8", trusted, followers)
	}

	derive(newEventForTest("pw_hop1_follow", "pw_hop1", at.Unix(), 3, [][]string{{"p", "pw_farmed_author"}}, "contacts", at))
	trusted, followers = readCounts("pw_farmed_author")
	if trusted != 1 || followers != 9 {
		t.Fatalf("one in-graph follower trusted=%d follower_count=%d, want 1 and 9", trusted, followers)
	}

	seedTrust("pw_rival_fan_a", 1)
	seedTrust("pw_rival_fan_b", 1)
	derive(newEventForTest("pw_rival_meta", "pw_rival_author", at.Unix(), 0, nil, `{"name":"Rival"}`, at))
	derive(newEventForTest("pw_rival_follow_a", "pw_rival_fan_a", at.Unix(), 3, [][]string{{"p", "pw_rival_author"}}, "contacts", at))
	derive(newEventForTest("pw_rival_follow_b", "pw_rival_fan_b", at.Unix(), 3, [][]string{{"p", "pw_rival_author"}}, "contacts", at))
	followed, err = pgStore.GetMostFollowedProfiles(ctx, 10, 0)
	if err != nil {
		t.Fatalf("GetMostFollowedProfiles after rival: %v", err)
	}
	if len(followed) < 2 || followed[0].Pubkey != "pw_rival_author" || followed[0].Score != 2 || followed[0].FollowerCount != 2 || followed[0].TrustedFollowerCount != 2 {
		t.Fatalf("rival should rank by two trusted followers, got %#v", followed)
	}
	if followed[1].Pubkey != "pw_farmed_author" || followed[1].Score != 1 || followed[1].TrustedFollowerCount != 1 || followed[1].FollowerCount != 9 {
		t.Fatalf("farm should rank by the one in-graph follower while keeping the raw ring, got %#v", followed[1])
	}

	if _, err := pool.Exec(ctx, `
		UPDATE profile_public_stats
		SET follower_count = 67860,
		    trusted_follower_count = 0
		WHERE pubkey = 'pw_farmed_author'
	`); err != nil {
		t.Fatalf("inflate raw follower count: %v", err)
	}
	candidates, err := pgStore.ListLargeAccountFrontierCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("list frontier before a trusted audience: %v", err)
	}
	for _, candidate := range candidates {
		if candidate.Pubkey == "pw_farmed_author" {
			t.Fatalf("raw count above 1000 with trusted count 0 is not a frontier candidate: %#v", candidates)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO profile_public_stats (
			pubkey, follower_count, trusted_follower_count, derivation_version
		) VALUES ('pw_popular', 5, 1000, 1)
	`); err != nil {
		t.Fatalf("insert in-graph audience: %v", err)
	}
	candidates, err = pgStore.ListLargeAccountFrontierCandidates(ctx, 10)
	if err != nil {
		t.Fatalf("list frontier: %v", err)
	}
	foundPopular := false
	for _, candidate := range candidates {
		if candidate.Pubkey == "pw_farmed_author" {
			t.Fatalf("farm stayed off the frontier only while trusted count stayed under the floor: %#v", candidates)
		}
		if candidate.Pubkey == "pw_popular" {
			foundPopular = true
			if candidate.Rank >= 0 {
				t.Fatalf("in-graph audience should sort ahead of ordinary trust ranks, got %#v", candidate)
			}
		}
	}
	if !foundPopular {
		t.Fatalf("trusted audience at the floor should be a frontier candidate, got %#v", candidates)
	}
}

// The legacy full-scan metric loader (incremental stats disabled) must
// exclude self-engagement, matching the incremental delta path: an account
// reacting to or zapping its own notes buys no engagement input.
func TestProjectProfileDiscoveryStats_LegacyFullScanExcludesSelfEngagement(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	incremental := false
	handlers := derivation.NewHandlersWithOptions(pool, derivation.HandlersOptions{
		IncrementalProfileDiscoveryStats: &incremental,
	})
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	at := now.Add(-90 * time.Minute)

	events := []model.Event{
		newEventForTest("pl_self_note", "pl_self_author", at.Unix(), 1, nil, "note", at),
		newEventForTest("pl_self_react", "pl_self_author", at.Unix(), 7, [][]string{{"e", "pl_self_note"}}, "+", at),
		newEventForTest("pl_self_repost", "pl_self_author", at.Unix(), 6, [][]string{{"e", "pl_self_note"}}, "", at),
		newEventForTest("pl_self_zap", "pl_self_author", at.Unix(), 9735, [][]string{{"e", "pl_self_note"}, {"p", "pl_self_author"}, {"amount", "700000000"}}, "", at),
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

	var engagement, zapMSats int64
	if err := pool.QueryRow(ctx, `
		SELECT recent_engagement_received, recent_zap_volume_msats
		FROM profile_discovery_stats
		WHERE pubkey = 'pl_self_author'
	`).Scan(&engagement, &zapMSats); err != nil {
		t.Fatalf("query legacy profile discovery stats: %v", err)
	}
	if engagement != 0 || zapMSats != 0 {
		t.Fatalf("expected self-engagement to be excluded from legacy metrics, got engagement=%d zap_msats=%d", engagement, zapMSats)
	}
}

// Likes on notes from outside the window, and likes on replies, must not
// become a per-post trending rate. One person liking several in-window root
// notes counts once, and a like with no reply or zap counts at the cheap
// floor. Rising still counts each person who showed up, including on older
// notes. Like-only attention stays under the rising noise floor.
func TestProjectProfileDiscoveryStats_EngagementFollowsInWindowRootNotes(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	old := now.Add(-72 * time.Hour)
	fresh := now.Add(-1 * time.Hour)
	like := now.Add(-20 * time.Minute)

	events := []model.Event{
		newEventForTest("win_old", "win_author", old.Unix(), 1, nil, "old root", old),
		newEventForTest("win_new_a", "win_author", fresh.Unix(), 1, nil, "new root a", fresh),
		newEventForTest("win_new_b", "win_author", fresh.Add(time.Minute).Unix(), 1, nil, "new root b", fresh.Add(time.Minute)),
		newEventForTest("win_parent", "win_parent_author", old.Unix(), 1, nil, "parent", old),
		newEventForTest("win_reply", "win_author", fresh.Unix(), 1, [][]string{{"e", "win_parent", "", "reply"}}, "reply", fresh),
		// One person likes the old root, both new roots, and the reply.
		newEventForTest("win_boost_old", "win_booster", like.Unix(), 7, [][]string{{"e", "win_old"}}, "+", like),
		newEventForTest("win_boost_a", "win_booster", like.Unix(), 7, [][]string{{"e", "win_new_a"}}, "+", like),
		newEventForTest("win_boost_b", "win_booster", like.Unix(), 7, [][]string{{"e", "win_new_b"}}, "+", like),
		newEventForTest("win_boost_reply", "win_booster", like.Unix(), 7, [][]string{{"e", "win_reply"}}, "+", like),
		// A second person only likes the old root. A third only likes the reply.
		newEventForTest("win_stranger_old", "win_stranger", like.Unix(), 7, [][]string{{"e", "win_old"}}, "+", like),
		newEventForTest("win_reply_fan", "win_reply_fan", like.Unix(), 7, [][]string{{"e", "win_reply"}}, "+", like),
		// No in-window root note. One like on a three-day-old note.
		newEventForTest("quiet_old", "quiet_author", old.Unix(), 1, nil, "quiet", old),
		newEventForTest("quiet_like", "quiet_fan", like.Unix(), 7, [][]string{{"e", "quiet_old"}}, "+", like),
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

	var raw int64
	var trending, rising float64
	var score, risingScore float64
	if err := pool.QueryRow(ctx, `
		SELECT recent_engagement_received,
		       COALESCE(scored_engagement_24h, -1),
		       COALESCE(scored_rising_engagement_24h, -1),
		       score_24h, rising_score_24h
		FROM profile_discovery_stats
		WHERE pubkey = 'win_author'
	`).Scan(&raw, &trending, &rising, &score, &risingScore); err != nil {
		t.Fatalf("query win_author stats: %v", err)
	}
	// Four likes from the booster plus one old-note like and one reply like.
	if raw != 6 {
		t.Fatalf("display engagement = %d, want 6 raw events", raw)
	}
	// Booster liked both fresh roots: one cheap vote, not two.
	if !near(trending, 0.12) {
		t.Fatalf("trending engagement = %v, want 0.12 (one person, cheap floor)", trending)
	}
	// Booster, stranger, and reply fan. No replies and no zaps, so each person is cheap.
	if !near(rising, 0.36) {
		t.Fatalf("rising engagement = %v, want 0.36 (three people at the cheap floor)", rising)
	}
	if score <= 0 {
		t.Fatalf("in-window root note with a real engager should trend, score=%v", score)
	}
	if risingScore != 0 {
		t.Fatalf("like-only attention must stay under the rising noise floor, score=%v", risingScore)
	}

	var quietTrending, quietRising, quietScore, quietRisingScore float64
	if err := pool.QueryRow(ctx, `
		SELECT COALESCE(scored_engagement_24h, -1),
		       COALESCE(scored_rising_engagement_24h, -1),
		       score_24h, rising_score_24h
		FROM profile_discovery_stats
		WHERE pubkey = 'quiet_author'
	`).Scan(&quietTrending, &quietRising, &quietScore, &quietRisingScore); err != nil {
		t.Fatalf("query quiet_author stats: %v", err)
	}
	if quietTrending != 0 || quietScore != 0 {
		t.Fatalf("old-note like must not trend a profile with no in-window root note: engagement=%v score=%v", quietTrending, quietScore)
	}
	if !near(quietRising, 0.12) {
		t.Fatalf("rising engagement = %v, want the one like at the cheap floor", quietRising)
	}
	if quietRisingScore != 0 {
		t.Fatalf("one person is noise for rising, score=%v", quietRisingScore)
	}
}

// A reaction pile and a greeting pile must not buy the profile rank that a
// short real conversation does. Display counters stay raw.
func TestProjectProfileDiscoveryStats_DiscountsFarmedEngagement(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	at := now.Add(-2 * time.Hour)

	events := []model.Event{
		newEventForTest("farm_note", "farm_author", at.Unix(), 1, nil, "farmed", at),
		newEventForTest("thread_note", "thread_author", at.Unix(), 1, nil, "thread", at),
		newEventForTest("greet_note", "greet_author", at.Unix(), 1, nil, "greet", at),
	}
	for i := 0; i < 20; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("farm_react_%d", i),
			fmt.Sprintf("farm_reactor_%d", i),
			at.Unix(),
			7,
			[][]string{{"e", "farm_note"}},
			"+",
			at,
		))
	}
	replies := []string{
		"Archival relays still matter for this conversation.",
		"Paid filters changed who actually showed up here.",
		"I would not route private messages through that box.",
		"The media policy is the part I disagree with.",
	}
	for i, content := range replies {
		events = append(events, newEventForTest(
			fmt.Sprintf("thread_reply_%d", i),
			fmt.Sprintf("thread_replier_%d", i),
			at.Unix(),
			1,
			[][]string{{"e", "thread_note", "", "reply"}},
			content,
			at,
		))
	}
	for i := 0; i < 2; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("thread_react_%d", i),
			fmt.Sprintf("thread_reactor_%d", i),
			at.Unix(),
			7,
			[][]string{{"e", "thread_note"}},
			"+",
			at,
		))
	}
	for i := 0; i < 6; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("greet_reply_%d", i),
			fmt.Sprintf("greet_replier_%d", i),
			at.Unix(),
			1,
			[][]string{{"e", "greet_note", "", "reply"}},
			"GM",
			at,
		))
	}
	for i := 0; i < 10; i++ {
		events = append(events, newEventForTest(
			fmt.Sprintf("greet_react_%d", i),
			fmt.Sprintf("greet_reactor_%d", i),
			at.Unix(),
			7,
			[][]string{{"e", "greet_note"}},
			"+",
			at,
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

	type scored struct {
		display    int64
		engagement float64
		score      float64
	}
	read := func(pubkey string) scored {
		t.Helper()
		var row scored
		if err := pool.QueryRow(ctx, `
			SELECT recent_engagement_received, COALESCE(scored_engagement_24h, -1), score_24h
			FROM profile_discovery_stats
			WHERE pubkey = $1
		`, pubkey).Scan(&row.display, &row.engagement, &row.score); err != nil {
			t.Fatalf("query %s: %v", pubkey, err)
		}
		return row
	}
	farm := read("farm_author")
	thread := read("thread_author")
	greet := read("greet_author")

	if farm.display != 20 {
		t.Fatalf("farm display engagement = %d, want 20 raw reactions", farm.display)
	}
	if !near(farm.engagement, 2.4) {
		t.Fatalf("farm scored engagement = %v, want 20 reactions at the cheap floor", farm.engagement)
	}
	if !near(thread.engagement, 6) {
		t.Fatalf("thread scored engagement = %v, want 4 replies + 2 likes", thread.engagement)
	}
	if !near(greet.engagement, math.Sqrt(6)+1.2) {
		t.Fatalf("greeting scored engagement = %v, want sqrt(6) plus 10 likes at the floor", greet.engagement)
	}
	if thread.score <= farm.score || thread.score <= greet.score {
		t.Fatalf("conversation should outrank both farms: thread=%v farm=%v greet=%v", thread.score, farm.score, greet.score)
	}
}

func near(got, want float64) bool {
	const eps = 0.001
	return got <= want+eps && got >= want-eps
}

func TestProjectionRebuildScopes_ProfileDiscoveryStatsFull(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")

	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)

	note := newEventForTest("profile_rebuild_note", "profile_rebuild_author", time.Now().UTC().Add(-1*time.Hour).Unix(), 1, nil, "rebuild profile stats", time.Now().UTC().Add(-1*time.Hour))
	if err := pgStore.InsertCanonicalEvent(ctx, note, extractTagsFromRaw(t, note.RawJSON), "wss://relay.one", note.FirstSeenAt); err != nil {
		t.Fatalf("insert note: %v", err)
	}
	if err := handlers.DeriveEventBundle(ctx, note.ID); err != nil {
		t.Fatalf("derive note bundle: %v", err)
	}
	drainPendingProfileStatsForTest(t, ctx, handlers)

	run, err := handlers.TriggerProjectionRebuild(ctx, derivation.TriggerProjectionRebuildParams{
		DerivationName: derivation.DerivationProfileDiscoveryStats,
		TargetVersion:  derivation.ProfileDiscoveryStatsVersion,
		Scope: derivation.ProjectionRebuildScope{
			Type: derivation.RebuildScopeFull,
		},
	})
	if err != nil {
		t.Fatalf("trigger profile discovery rebuild: %v", err)
	}
	if err := handlers.ExecuteProjectionRebuildRun(ctx, run.ID); err != nil {
		t.Fatalf("execute profile discovery rebuild: %v", err)
	}
	assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationProfileDiscoveryStats, derivation.ProfileDiscoveryStatsVersion, derivation.ProfileDiscoveryStatsVersion)

	var version int
	if err := pool.QueryRow(ctx, `
		SELECT derivation_version
		FROM profile_discovery_stats
		WHERE pubkey = $1
	`, "profile_rebuild_author").Scan(&version); err != nil {
		t.Fatalf("query profile discovery derivation version: %v", err)
	}
	if version != derivation.ProfileDiscoveryStatsVersion {
		t.Fatalf("unexpected profile discovery derivation version: got=%d want=%d", version, derivation.ProfileDiscoveryStatsVersion)
	}
}
