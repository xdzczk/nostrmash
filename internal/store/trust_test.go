package store

import (
	"context"
	"errors"
	"testing"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

func TestTrustReads_GetAndList(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)

	var runID int64
	err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name, target_version, status, attempts,
			input_follower_edges_count, score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 12, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID)
	if err != nil {
		t.Fatalf("insert trust run: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('pk1', 10.5, 1, $1, $2, $3),
			('pk2', 8.0, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion)
	if err != nil {
		t.Fatalf("insert trust scores: %v", err)
	}

	score, err := s.GetTrustScore(ctx, "pk1")
	if err != nil {
		t.Fatalf("GetTrustScore: %v", err)
	}
	if score.Pubkey != "pk1" || score.Rank != 1 {
		t.Fatalf("unexpected trust score: %+v", score)
	}

	top, err := s.ListTopTrustedPubkeys(ctx, 2)
	if err != nil {
		t.Fatalf("ListTopTrustedPubkeys: %v", err)
	}
	if len(top) != 2 || top[0].Pubkey != "pk1" || top[1].Pubkey != "pk2" {
		t.Fatalf("unexpected top trust list: %+v", top)
	}

	run, err := s.GetTrustRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetTrustRun: %v", err)
	}
	if run.ID != runID || run.ScoreRowsPublished != 2 {
		t.Fatalf("unexpected trust run: %+v", run)
	}

	runs, err := s.ListTrustRuns(ctx, 5)
	if err != nil {
		t.Fatalf("ListTrustRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != runID {
		t.Fatalf("unexpected trust runs list: %+v", runs)
	}
}

func TestTrustReads_NotFoundAndValidation(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)

	if _, err := s.GetTrustScore(ctx, " "); err == nil {
		t.Fatalf("expected validation error for empty pubkey")
	}
	if _, err := s.GetTrustScore(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing trust score, got %v", err)
	}
	if _, err := s.GetTrustRun(ctx, 9999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing trust run, got %v", err)
	}
}
