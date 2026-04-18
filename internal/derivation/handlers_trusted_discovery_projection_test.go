package derivation_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestTrustedDiscoveryProjection_RebuildMatchesBaselineAfterTruncate(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	handlers := derivation.NewHandlers(pool)
	pgStore := store.NewPostgresStore(pool)
	now := time.Now().UTC()
	events := []model.Event{
		newEventForTest("trusted_proj_note_1", "author_alpha", now.Add(-2*time.Hour).Unix(), 1, nil, "note 1", now.Add(-2*time.Hour)),
		newEventForTest("trusted_proj_note_2", "author_beta", now.Add(-90*time.Minute).Unix(), 1, nil, "note 2", now.Add(-90*time.Minute)),
	}
	for _, event := range events {
		if err := pgStore.InsertCanonicalEvent(ctx, event, extractTagsFromRaw(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed, source_run_id, refreshed_at)
		VALUES
			('author_alpha', 1, false, NULL, now()),
			('author_beta', 4, false, NULL, now())
	`); err != nil {
		t.Fatalf("seed trust_graph_snapshot: %v", err)
	}
	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name,
			target_version,
			status,
			attempts,
			input_follower_edges_count,
			score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 0, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("seed trust run: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('author_alpha', 0.9, 1, $1, $2, $3),
			('author_beta', 0.2, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("seed trust_scores_global: %v", err)
	}

	for _, derivationName := range []string{
		derivation.DerivationTrustedNoteDiscovery,
		derivation.DerivationTrustedProfileDiscovery,
	} {
		run := triggerAndExecuteFullRebuild(t, ctx, handlers, derivationName, 2)
		assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
		assertActiveAndTargetVersion(t, ctx, pool, derivationName, 2, 2)
	}
	baselineNotes := readTrustedNoteProjectionRows(t, ctx, pool)
	baselineProfiles := readTrustedProfileProjectionRows(t, ctx, pool)

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE
			trusted_note_discovery_candidates,
			trusted_profile_discovery_candidates,
			trusted_discovery_projection_state
	`); err != nil {
		t.Fatalf("truncate trusted discovery projections: %v", err)
	}
	for _, derivationName := range []string{
		derivation.DerivationTrustedNoteDiscovery,
		derivation.DerivationTrustedProfileDiscovery,
	} {
		run := triggerAndExecuteFullRebuild(t, ctx, handlers, derivationName, 2)
		assertRebuildRunSucceeded(t, ctx, handlers, run.ID)
	}
	rebuiltNotes := readTrustedNoteProjectionRows(t, ctx, pool)
	rebuiltProfiles := readTrustedProfileProjectionRows(t, ctx, pool)
	if !reflect.DeepEqual(baselineNotes, rebuiltNotes) {
		t.Fatalf("trusted note projection mismatch\nbaseline=%#v\nrebuilt=%#v", baselineNotes, rebuiltNotes)
	}
	if !reflect.DeepEqual(baselineProfiles, rebuiltProfiles) {
		t.Fatalf("trusted profile projection mismatch\nbaseline=%#v\nrebuilt=%#v", baselineProfiles, rebuiltProfiles)
	}
}

type trustedNoteProjectionRow struct {
	EventID           string
	AuthorPubkey      string
	MinHops           int
	TrustScore        float64
	DerivationVersion int
}

func readTrustedNoteProjectionRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []trustedNoteProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT event_id, author_pubkey, COALESCE(min_hops, -1), COALESCE(trust_score, 0), derivation_version
		FROM trusted_note_discovery_candidates
		ORDER BY event_id ASC
	`)
	if err != nil {
		t.Fatalf("query trusted note projection rows: %v", err)
	}
	defer rows.Close()
	out := make([]trustedNoteProjectionRow, 0)
	for rows.Next() {
		var row trustedNoteProjectionRow
		if err := rows.Scan(&row.EventID, &row.AuthorPubkey, &row.MinHops, &row.TrustScore, &row.DerivationVersion); err != nil {
			t.Fatalf("scan trusted note projection row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read trusted note projection rows: %v", err)
	}
	return out
}

type trustedProfileProjectionRow struct {
	Pubkey            string
	MinHops           int
	TrustScore        float64
	DerivationVersion int
}

func readTrustedProfileProjectionRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []trustedProfileProjectionRow {
	t.Helper()
	rows, err := pool.Query(ctx, `
		SELECT pubkey, COALESCE(min_hops, -1), COALESCE(trust_score, 0), derivation_version
		FROM trusted_profile_discovery_candidates
		ORDER BY pubkey ASC
	`)
	if err != nil {
		t.Fatalf("query trusted profile projection rows: %v", err)
	}
	defer rows.Close()
	out := make([]trustedProfileProjectionRow, 0)
	for rows.Next() {
		var row trustedProfileProjectionRow
		if err := rows.Scan(&row.Pubkey, &row.MinHops, &row.TrustScore, &row.DerivationVersion); err != nil {
			t.Fatalf("scan trusted profile projection row: %v", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read trusted profile projection rows: %v", err)
	}
	return out
}
