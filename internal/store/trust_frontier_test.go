package store

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestTrustFrontierRefreshClaimAndSuggestions(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
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
