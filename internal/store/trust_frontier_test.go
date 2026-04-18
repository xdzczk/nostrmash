package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestTrustFrontierRefreshClaimAndSuggestions(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (derivation_name, target_version, status)
		VALUES ($1, $2, 'succeeded')
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('pk1', 9.0, 1, $1, $2, $3),
			('pk2', 8.0, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert trust scores: %v", err)
	}

	refresh, err := s.RefreshTrustPubkeyFrontier(ctx, 2, 0, 2)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier: %v", err)
	}
	if refresh.ActiveCount == 0 {
		t.Fatalf("expected active frontier rows, got %+v", refresh)
	}

	claimed, err := s.ClaimTrustPubkeyFrontierForFetch(ctx, 1, time.Minute)
	if err != nil {
		t.Fatalf("claim trust pubkey frontier: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed pubkey, got %d", len(claimed))
	}
	if claimed[0].Pubkey != "pk1" {
		t.Fatalf("expected top-ranked pubkey first, got %+v", claimed[0])
	}
	if err := s.MarkTrustPubkeyFetchSuccess(ctx, claimed[0].Pubkey, time.Minute); err != nil {
		t.Fatalf("mark trust pubkey fetch success: %v", err)
	}

	active, candidate, cooldown, failed, err := s.GetTrustPubkeyFrontierStats(ctx)
	if err != nil {
		t.Fatalf("get trust pubkey frontier stats: %v", err)
	}
	if active == 0 {
		t.Fatalf("expected active frontier entries, got active=%d candidate=%d cooldown=%d failed=%d", active, candidate, cooldown, failed)
	}

	now := time.Now().UTC()
	runIDCopy := runID
	relayRefresh, err := s.RefreshTrustRelaySuggestions(ctx, []TrustRelayCandidate{
		{
			RelayURL:               "wss://relay.one",
			WeightedScore:          7.5,
			SupportingPubkeysCount: 2,
			SupportingPubkeysSample: []string{
				"pk1",
				"pk2",
			},
			SourceRunID:      &runIDCopy,
			SourceComputedAt: &now,
		},
	}, 0, 1)
	if err != nil {
		t.Fatalf("refresh trust relay suggestions: %v", err)
	}
	if relayRefresh.NewPromotions == 0 {
		t.Fatalf("expected at least one promoted relay suggestion, got %+v", relayRefresh)
	}
	suggestions, err := s.ListTrustRelaySuggestions(ctx, 10, true)
	if err != nil {
		t.Fatalf("list trust relay suggestions: %v", err)
	}
	if len(suggestions) == 0 {
		t.Fatalf("expected recommended relay suggestions")
	}
	if suggestions[0].RelayURL != "wss://relay.one" {
		t.Fatalf("unexpected relay suggestion: %+v", suggestions[0])
	}
}

func TestTrustPubkeyFrontier_RespectsStableWindowAndReactivatesFailedRows(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	runID := insertTrustRunForFrontierTest(t, ctx, pool)
	insertTrustScoresForFrontierTest(t, ctx, pool, runID,
		trustScoreSeed{Pubkey: "pk1", Score: 9.0, Rank: 1},
	)

	refresh, err := s.RefreshTrustPubkeyFrontier(ctx, 10, time.Hour, 10)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier: %v", err)
	}
	if refresh.PromotedToActive != 0 || refresh.ActiveCount != 0 {
		t.Fatalf("expected stable window to block promotion, got %+v", refresh)
	}

	claimed, err := s.ClaimTrustPubkeyFrontierForFetch(ctx, 1, time.Minute)
	if err != nil {
		t.Fatalf("claim trust pubkey frontier: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("expected no active frontier rows to claim, got %+v", claimed)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE ingest_pubkey_frontier
		SET first_seen_at = now() - interval '2 hours'
		WHERE pubkey = 'pk1'
	`); err != nil {
		t.Fatalf("age frontier row: %v", err)
	}

	if err := s.MarkTrustPubkeyFetchFailure(ctx, "pk1", 30*time.Minute, errors.New("relay timeout")); err != nil {
		t.Fatalf("mark trust pubkey fetch failure: %v", err)
	}

	var state string
	var lastError string
	if err := pool.QueryRow(ctx, `
		SELECT state, COALESCE(last_error, '')
		FROM ingest_pubkey_frontier
		WHERE pubkey = 'pk1'
	`).Scan(&state, &lastError); err != nil {
		t.Fatalf("query failed frontier row: %v", err)
	}
	if state != "failed" {
		t.Fatalf("expected failed state after fetch failure, got %q", state)
	}
	if lastError != "relay timeout" {
		t.Fatalf("expected failure cause to be stored, got %q", lastError)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE ingest_pubkey_frontier
		SET next_eligible_at = now() - interval '1 minute'
		WHERE pubkey = 'pk1'
	`); err != nil {
		t.Fatalf("make failed row eligible again: %v", err)
	}

	refresh, err = s.RefreshTrustPubkeyFrontier(ctx, 10, 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier after failure: %v", err)
	}
	if refresh.ReactivatedFailed != 1 {
		t.Fatalf("expected one failed row to reactivate, got %+v", refresh)
	}

	active, candidate, cooldown, failed, err := s.GetTrustPubkeyFrontierStats(ctx)
	if err != nil {
		t.Fatalf("get trust pubkey frontier stats: %v", err)
	}
	if active != 0 || candidate != 1 || cooldown != 0 || failed != 0 {
		t.Fatalf("unexpected frontier stats after reactivation: active=%d candidate=%d cooldown=%d failed=%d", active, candidate, cooldown, failed)
	}
}

func TestTrustPubkeyFrontier_PromotionCapClaimOrderAndConflictRefresh(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	runID := insertTrustRunForFrontierTest(t, ctx, pool)
	insertTrustScoresForFrontierTest(t, ctx, pool, runID,
		trustScoreSeed{Pubkey: "pk1", Score: 9.0, Rank: 1},
		trustScoreSeed{Pubkey: "pk2", Score: 8.0, Rank: 2},
		trustScoreSeed{Pubkey: "pk3", Score: 7.0, Rank: 3},
	)

	refresh, err := s.RefreshTrustPubkeyFrontier(ctx, 10, 0, 2)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier: %v", err)
	}
	if refresh.PromotedToActive != 2 || refresh.ActiveCount != 2 {
		t.Fatalf("expected two active frontier rows, got %+v", refresh)
	}

	active, candidate, cooldown, failed, err := s.GetTrustPubkeyFrontierStats(ctx)
	if err != nil {
		t.Fatalf("get trust pubkey frontier stats: %v", err)
	}
	if active != 2 || candidate != 1 || cooldown != 0 || failed != 0 {
		t.Fatalf("unexpected initial frontier stats: active=%d candidate=%d cooldown=%d failed=%d", active, candidate, cooldown, failed)
	}

	claimed, err := s.ClaimTrustPubkeyFrontierForFetch(ctx, 2, time.Minute)
	if err != nil {
		t.Fatalf("claim trust pubkey frontier: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("expected two claimed frontier rows, got %d", len(claimed))
	}
	if claimed[0].Pubkey != "pk1" || claimed[1].Pubkey != "pk2" {
		t.Fatalf("expected claim order by trust rank, got %+v", claimed)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE ingest_pubkey_frontier
		SET state = 'active',
		    first_seen_at = now() - interval '2 hours'
		WHERE pubkey = 'pk1'
	`); err != nil {
		t.Fatalf("prepare active frontier row for refresh: %v", err)
	}
	var originalFirstSeen time.Time
	if err := pool.QueryRow(ctx, `
		SELECT first_seen_at
		FROM ingest_pubkey_frontier
		WHERE pubkey = 'pk1'
	`).Scan(&originalFirstSeen); err != nil {
		t.Fatalf("query original first_seen_at: %v", err)
	}

	runID2 := insertTrustRunForFrontierTest(t, ctx, pool)
	insertTrustScoresForFrontierTest(t, ctx, pool, runID2,
		trustScoreSeed{Pubkey: "pk1", Score: 11.0, Rank: 5},
	)
	if _, err := pool.Exec(ctx, `DELETE FROM trust_scores_global WHERE pubkey IN ('pk2', 'pk3')`); err != nil {
		t.Fatalf("trim trust scores for focused refresh: %v", err)
	}

	refresh, err = s.RefreshTrustPubkeyFrontier(ctx, 10, 0, 10)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier second pass: %v", err)
	}
	if refresh.ActiveCount == 0 {
		t.Fatalf("expected active frontier rows after refresh, got %+v", refresh)
	}

	var refreshedState string
	var refreshedRank int64
	var refreshedScore float64
	var refreshedRunID int64
	var refreshedFirstSeen time.Time
	if err := pool.QueryRow(ctx, `
		SELECT state, trust_rank, trust_score, source_run_id, first_seen_at
		FROM ingest_pubkey_frontier
		WHERE pubkey = 'pk1'
	`).Scan(&refreshedState, &refreshedRank, &refreshedScore, &refreshedRunID, &refreshedFirstSeen); err != nil {
		t.Fatalf("query refreshed frontier row: %v", err)
	}
	if refreshedState != "active" {
		t.Fatalf("expected active state to be preserved on conflict refresh, got %q", refreshedState)
	}
	if !refreshedFirstSeen.Equal(originalFirstSeen) {
		t.Fatalf("expected first_seen_at to remain unchanged, got %v want %v", refreshedFirstSeen, originalFirstSeen)
	}
	if refreshedRank != 5 || refreshedScore != 11.0 || refreshedRunID != runID2 {
		t.Fatalf("expected trust fields to refresh, got rank=%d score=%f runID=%d", refreshedRank, refreshedScore, refreshedRunID)
	}
}

func TestTrustRelaySuggestions_DemotesStaleAndRetainsRecommendationsOnEmptyBatch(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	runID := insertTrustRunForFrontierTest(t, ctx, pool)
	now := time.Now().UTC()

	refresh, err := s.RefreshTrustRelaySuggestions(ctx, []TrustRelayCandidate{
		{
			RelayURL:               "wss://relay.one",
			WeightedScore:          9.0,
			SupportingPubkeysCount: 3,
			SupportingPubkeysSample: []string{
				"pk1",
				"pk2",
			},
			SourceRunID:      &runID,
			SourceComputedAt: &now,
		},
		{
			RelayURL:               "wss://relay.two",
			WeightedScore:          7.0,
			SupportingPubkeysCount: 2,
			SupportingPubkeysSample: []string{
				"pk3",
			},
			SourceRunID:      &runID,
			SourceComputedAt: &now,
		},
	}, 0, 1)
	if err != nil {
		t.Fatalf("refresh trust relay suggestions: %v", err)
	}
	if refresh.NewPromotions != 1 {
		t.Fatalf("expected one relay promotion due to maxPromotions=1, got %+v", refresh)
	}

	suggestions, err := s.ListTrustRelaySuggestions(ctx, 10, false)
	if err != nil {
		t.Fatalf("list trust relay suggestions: %v", err)
	}
	if len(suggestions) != 2 {
		t.Fatalf("expected two relay suggestions, got %d", len(suggestions))
	}
	if suggestions[0].RelayURL != "wss://relay.one" || !suggestions[0].IsRecommended {
		t.Fatalf("expected highest-weight relay to be recommended first, got %+v", suggestions[0])
	}

	refresh, err = s.RefreshTrustRelaySuggestions(ctx, []TrustRelayCandidate{
		{
			RelayURL:               "wss://relay.two",
			WeightedScore:          10.0,
			SupportingPubkeysCount: 5,
			SupportingPubkeysSample: []string{
				"pk4",
				"pk5",
			},
			SourceRunID:      &runID,
			SourceComputedAt: &now,
		},
	}, 0, 1)
	if err != nil {
		t.Fatalf("refresh trust relay suggestions second pass: %v", err)
	}
	if refresh.NewPromotions != 1 {
		t.Fatalf("expected one newly promoted relay on second pass, got %+v", refresh)
	}

	suggestions, err = s.ListTrustRelaySuggestions(ctx, 10, false)
	if err != nil {
		t.Fatalf("list trust relay suggestions after stale demotion: %v", err)
	}
	if len(suggestions) != 2 {
		t.Fatalf("expected two relay suggestions after second pass, got %d", len(suggestions))
	}
	if suggestions[0].RelayURL != "wss://relay.two" || !suggestions[0].IsRecommended {
		t.Fatalf("expected relay.two to be the only recommended relay, got %+v", suggestions[0])
	}
	if suggestions[1].RelayURL != "wss://relay.one" || suggestions[1].IsRecommended {
		t.Fatalf("expected stale relay.one recommendation to be demoted, got %+v", suggestions[1])
	}

	refresh, err = s.RefreshTrustRelaySuggestions(ctx, nil, 0, 1)
	if err != nil {
		t.Fatalf("refresh trust relay suggestions with empty batch: %v", err)
	}
	if refresh.Upserted != 0 || refresh.NewPromotions != 0 {
		t.Fatalf("expected empty batch to do no new work, got %+v", refresh)
	}

	recommendedOnly, err := s.ListTrustRelaySuggestions(ctx, 10, true)
	if err != nil {
		t.Fatalf("list recommended relay suggestions after empty batch: %v", err)
	}
	if len(recommendedOnly) != 1 || recommendedOnly[0].RelayURL != "wss://relay.two" {
		t.Fatalf("expected existing recommendation to persist on empty batch, got %+v", recommendedOnly)
	}
}

func TestTrustPubkeyFrontier_ConcurrentClaimsDoNotDuplicatePubkeys(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	runID := insertTrustRunForFrontierTest(t, ctx, pool)
	insertTrustScoresForFrontierTest(t, ctx, pool, runID,
		trustScoreSeed{Pubkey: "pk1", Score: 10, Rank: 1},
		trustScoreSeed{Pubkey: "pk2", Score: 9, Rank: 2},
		trustScoreSeed{Pubkey: "pk3", Score: 8, Rank: 3},
		trustScoreSeed{Pubkey: "pk4", Score: 7, Rank: 4},
		trustScoreSeed{Pubkey: "pk5", Score: 6, Rank: 5},
		trustScoreSeed{Pubkey: "pk6", Score: 5, Rank: 6},
	)
	refresh, err := s.RefreshTrustPubkeyFrontier(ctx, 10, 0, 10)
	if err != nil {
		t.Fatalf("refresh trust pubkey frontier: %v", err)
	}
	if refresh.ActiveCount != 6 {
		t.Fatalf("expected six active frontier rows, got %+v", refresh)
	}

	const workers = 8
	start := make(chan struct{})
	results := make(chan []TrustPubkeyFrontierEntry, workers)
	errs := make(chan error, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			claimed, err := s.ClaimTrustPubkeyFrontierForFetch(ctx, 1, time.Minute)
			if err != nil {
				errs <- err
				return
			}
			results <- claimed
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent claim failed: %v", err)
		}
	}

	claimedPubkeys := make(map[string]bool)
	claimCount := 0
	for claimed := range results {
		for _, entry := range claimed {
			claimCount++
			if claimedPubkeys[entry.Pubkey] {
				t.Fatalf("pubkey %q claimed more than once", entry.Pubkey)
			}
			claimedPubkeys[entry.Pubkey] = true
		}
	}
	if claimCount != 6 {
		t.Fatalf("expected exactly six distinct claimed pubkeys, got %d (%v)", claimCount, claimedPubkeys)
	}

	active, candidate, cooldown, failed, err := s.GetTrustPubkeyFrontierStats(ctx)
	if err != nil {
		t.Fatalf("get trust pubkey frontier stats: %v", err)
	}
	if active != 0 || candidate != 0 || cooldown != 6 || failed != 0 {
		t.Fatalf("unexpected frontier stats after concurrent claims: active=%d candidate=%d cooldown=%d failed=%d", active, candidate, cooldown, failed)
	}
}

type trustScoreSeed struct {
	Pubkey string
	Score  float64
	Rank   int64
}

func insertTrustRunForFrontierTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (derivation_name, target_version, status)
		VALUES ($1, $2, 'succeeded')
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	return runID
}

func insertTrustScoresForFrontierTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, runID int64, seeds ...trustScoreSeed) {
	t.Helper()
	for _, seed := range seeds {
		if _, err := pool.Exec(ctx, `
			INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (pubkey) DO UPDATE
			SET score = EXCLUDED.score,
			    rank = EXCLUDED.rank,
			    run_id = EXCLUDED.run_id,
			    derivation_name = EXCLUDED.derivation_name,
			    target_version = EXCLUDED.target_version
		`, seed.Pubkey, seed.Score, seed.Rank, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
			t.Fatalf("insert trust score for %s: %v", seed.Pubkey, err)
		}
	}
}
