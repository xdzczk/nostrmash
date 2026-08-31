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
	if topRisingPubkey != "small_author" {
		t.Fatalf("expected small_author to lead rising ranking via relative engagement vs. a larger audience, got %s", topRisingPubkey)
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

	readStats := func(pubkey string) (score24h, rising24h float64, engagementDisplay int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `
			SELECT score_24h, rising_score_24h, recent_engagement_received
			FROM profile_discovery_stats
			WHERE pubkey = $1
		`, pubkey).Scan(&score24h, &rising24h, &engagementDisplay); err != nil {
			t.Fatalf("query profile discovery stats for %s: %v", pubkey, err)
		}
		return score24h, rising24h, engagementDisplay
	}

	farmedScore, farmedRising, farmedDisplay := readStats("pw_farmed_author")
	baselineScore, baselineRising, _ := readStats("pw_baseline_author")
	engagedScore, _, engagedDisplay := readStats("pw_engaged_author")

	// Display counters keep reflecting raw (self-excluded) activity.
	if farmedDisplay != 3 {
		t.Fatalf("expected farmed display engagement=3 (2 bot reactions + 1 bot zap), got %d", farmedDisplay)
	}
	if engagedDisplay != 2 {
		t.Fatalf("expected engaged display engagement=2 raw reactions, got %d", engagedDisplay)
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
		TargetVersion:  2,
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
	assertActiveAndTargetVersion(t, ctx, pool, derivation.DerivationProfileDiscoveryStats, 2, 2)

	var version int
	if err := pool.QueryRow(ctx, `
		SELECT derivation_version
		FROM profile_discovery_stats
		WHERE pubkey = $1
	`, "profile_rebuild_author").Scan(&version); err != nil {
		t.Fatalf("query profile discovery derivation version: %v", err)
	}
	if version != 2 {
		t.Fatalf("unexpected profile discovery derivation version: got=%d want=2", version)
	}
}
