package trust_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/store/trust"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func setupTrustStore(t *testing.T) (context.Context, *pgxpool.Pool, *trust.Trust) {
	t.Helper()
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "trust_reads"), "trust_reads")
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	return ctx, pool, trust.New(pool)
}

func TestTrustReadsAndSeeds(t *testing.T) {
	ctx, pool, s := setupTrustStore(t)

	n, err := s.UpsertActiveSeeds(ctx, []string{" Seed_A ", "", "seed_a", "seed_b"})
	if err != nil {
		t.Fatalf("UpsertActiveSeeds: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 seed upserts, got %d", n)
	}

	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name, target_version, status, attempts,
			input_follower_edges_count, score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 12, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('pk1', 10.5, 1, $1, $2, $3),
			('pk2', 8.0, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert trust scores: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ('evt-seed-pk1', 'seed_a', 100, 3, 'sig', '', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("insert source event: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (source_event_id, follower_pubkey, followed_pubkey, contact_list_created_at, derivation_version)
		VALUES ('evt-seed-pk1', 'seed_a', 'pk1', 100, 1)
	`); err != nil {
		t.Fatalf("insert follower edge: %v", err)
	}

	score, err := s.GetTrustScore(ctx, "pk1")
	if err != nil {
		t.Fatalf("GetTrustScore: %v", err)
	}
	if score.Pubkey != "pk1" || score.Rank != 1 {
		t.Fatalf("unexpected trust score: %+v", score)
	}

	top, err := s.ListTopTrustedPubkeys(ctx, 10)
	if err != nil {
		t.Fatalf("ListTopTrustedPubkeys: %v", err)
	}
	if len(top) < 2 {
		t.Fatalf("expected top trusted rows, got %d", len(top))
	}

	run, err := s.GetTrustRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetTrustRun: %v", err)
	}
	if run.ID != runID {
		t.Fatalf("unexpected run: %+v", run)
	}

	runs, err := s.ListTrustRuns(ctx, 10)
	if err != nil {
		t.Fatalf("ListTrustRuns: %v", err)
	}
	if len(runs) == 0 {
		t.Fatal("expected trust runs")
	}

	refresh, err := s.RefreshTrustGraphSnapshot(ctx, 3)
	if err != nil {
		t.Fatalf("RefreshTrustGraphSnapshot: %v", err)
	}
	if refresh.RowsUpserted < 1 {
		t.Fatalf("expected snapshot rows, got %+v", refresh)
	}

	latest, err := s.GetTrustPubkeyLatest(ctx, "pk1")
	if err != nil {
		t.Fatalf("GetTrustPubkeyLatest: %v", err)
	}
	if latest.Pubkey != "pk1" || latest.Score == nil || *latest.Score != 10.5 || latest.Rank == nil || *latest.Rank != 1 {
		t.Fatalf("unexpected trust_pubkeys_latest row: %+v", latest)
	}
	if latest.MinHops == nil || *latest.MinHops != 1 {
		t.Fatalf("expected pk1 hop distance 1, got %+v", latest)
	}

	ranked, err := s.CountRankedPubkeys(ctx)
	if err != nil {
		t.Fatalf("CountRankedPubkeys: %v", err)
	}
	if ranked != 2 {
		t.Fatalf("expected 2 ranked pubkeys, got %d", ranked)
	}

	trusted, err := s.IsTrustedAuthor(ctx, "seed_a", trust.TrustQualificationPolicy{MaxHops: 3})
	if err != nil {
		t.Fatalf("IsTrustedAuthor: %v", err)
	}
	if !trusted {
		t.Fatal("seed_a should be trusted")
	}
}
