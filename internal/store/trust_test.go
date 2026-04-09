package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/model"
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

func TestTrustQualification_TrustedUntrustedAndMissing(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)

	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name, target_version, status, attempts,
			input_follower_edges_count, score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 3, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_seeds (pubkey, is_active)
		VALUES ('seed', true)
	`); err != nil {
		t.Fatalf("insert trust seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('evt-seed-alice', 'seed', 100, 3, 'sig-seed-alice', '', '{}'::jsonb),
			('evt-alice-bob', 'alice', 101, 3, 'sig-alice-bob', '', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("insert source events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (source_event_id, follower_pubkey, followed_pubkey, contact_list_created_at, derivation_version)
		VALUES
			('evt-seed-alice', 'seed', 'alice', 100, 1),
			('evt-alice-bob', 'alice', 'bob', 101, 1)
	`); err != nil {
		t.Fatalf("insert follower edges: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('alice', 0.75, 1, $1, $2, $3),
			('bob', 0.20, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert trust scores: %v", err)
	}

	refresh, err := s.RefreshTrustGraphSnapshot(ctx, 3)
	if err != nil {
		t.Fatalf("RefreshTrustGraphSnapshot: %v", err)
	}
	if refresh.RowsUpserted != 3 {
		t.Fatalf("unexpected snapshot rows upserted: got=%d want=3", refresh.RowsUpserted)
	}

	rows, err := s.GetTrustQualifications(ctx, []string{"seed", "alice", "bob", "unknown"}, TrustQualificationPolicy{
		MaxHops:      3,
		MinimumScore: 0.5,
	})
	if err != nil {
		t.Fatalf("GetTrustQualifications: %v", err)
	}

	if !rows["seed"].Trusted {
		t.Fatalf("expected seed to be trusted")
	}
	if !rows["alice"].Trusted {
		t.Fatalf("expected alice to be trusted")
	}
	if rows["bob"].Trusted {
		t.Fatalf("expected bob to be untrusted due to minimum score")
	}
	if rows["unknown"].Trusted {
		t.Fatalf("expected unknown to be untrusted")
	}
	if rows["unknown"].DistanceHops != nil {
		t.Fatalf("expected unknown hops to be nil, got=%v", *rows["unknown"].DistanceHops)
	}
}

func TestTrustQualification_RespectsHopLimitAndBatchLookups(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)

	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_seeds (pubkey, is_active)
		VALUES ('seed', true)
	`); err != nil {
		t.Fatalf("insert trust seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('evt-seed-a', 'seed', 10, 3, 'sig-seed-a', '', '{}'::jsonb),
			('evt-a-b', 'a', 11, 3, 'sig-a-b', '', '{}'::jsonb),
			('evt-b-c', 'b', 12, 3, 'sig-b-c', '', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("insert source events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (source_event_id, follower_pubkey, followed_pubkey, contact_list_created_at, derivation_version)
		VALUES
			('evt-seed-a', 'seed', 'a', 10, 1),
			('evt-a-b', 'a', 'b', 11, 1),
			('evt-b-c', 'b', 'c', 12, 1)
	`); err != nil {
		t.Fatalf("insert follower edges: %v", err)
	}

	if _, err := s.RefreshTrustGraphSnapshot(ctx, 5); err != nil {
		t.Fatalf("RefreshTrustGraphSnapshot: %v", err)
	}

	batch, err := s.GetTrustQualifications(ctx, []string{"a", "b", "c", "missing", "a"}, TrustQualificationPolicy{
		MaxHops: 1,
	})
	if err != nil {
		t.Fatalf("GetTrustQualifications: %v", err)
	}
	if len(batch) != 4 {
		t.Fatalf("expected deduplicated batch size 4, got %d", len(batch))
	}
	if !batch["a"].Trusted {
		t.Fatalf("expected a to be trusted at hop=1")
	}
	if batch["b"].Trusted {
		t.Fatalf("expected b to be untrusted at hop=2 with max_hops=1")
	}
	if batch["c"].Trusted {
		t.Fatalf("expected c to be untrusted at hop=3 with max_hops=1")
	}
	if batch["missing"].Trusted {
		t.Fatalf("expected missing to be untrusted")
	}

	isTrusted, err := s.IsTrustedAuthor(ctx, "b", TrustQualificationPolicy{MaxHops: 2})
	if err != nil {
		t.Fatalf("IsTrustedAuthor: %v", err)
	}
	if !isTrusted {
		t.Fatalf("expected b to be trusted with max_hops=2")
	}
}

func TestTrustState_BatchLookupUnknownAndFreshnessGeneration(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)

	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name, target_version, status, attempts,
			input_follower_edges_count, score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 3, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO trust_seeds (pubkey, is_active) VALUES ('seed', true)`); err != nil {
		t.Fatalf("insert trust seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES ('edge_seed_alice', 'seed', 100, 3, 'sig_seed_alice', '', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("insert edge event: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (source_event_id, follower_pubkey, followed_pubkey, contact_list_created_at, derivation_version)
		VALUES ('edge_seed_alice', 'seed', 'alice', 100, 1)
	`); err != nil {
		t.Fatalf("insert follower edge: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES ('alice', 0.91, 1, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert trust score: %v", err)
	}
	if _, err := s.RefreshTrustGraphSnapshot(ctx, 3); err != nil {
		t.Fatalf("RefreshTrustGraphSnapshot: %v", err)
	}

	states, err := s.GetTrustStates(ctx, []string{"alice", "unknown", "alice"})
	if err != nil {
		t.Fatalf("GetTrustStates: %v", err)
	}
	if len(states) != 2 {
		t.Fatalf("expected deduplicated trust state map of size 2, got %d", len(states))
	}
	alice := states["alice"]
	if !alice.Qualified || alice.HopDistance == nil || *alice.HopDistance != 1 {
		t.Fatalf("unexpected trust state qualification/hops for alice: %#v", alice)
	}
	if alice.Tier != "core" || alice.HopBucket != "1" {
		t.Fatalf("unexpected tier/bucket for alice: %#v", alice)
	}
	if alice.ComputedAt == nil {
		t.Fatalf("expected computed_at to be populated for alice")
	}
	if alice.GenerationID == nil || *alice.GenerationID != runID {
		t.Fatalf("expected generation id=%d, got %#v", runID, alice.GenerationID)
	}
	if states["unknown"].Qualified || states["unknown"].Tier != "unknown" || states["unknown"].HopBucket != "unknown" {
		t.Fatalf("unexpected unknown trust state: %#v", states["unknown"])
	}

	single, err := s.GetTrustState(ctx, "alice")
	if err != nil {
		t.Fatalf("GetTrustState alice: %v", err)
	}
	if single.Pubkey != "alice" || single.GenerationID == nil {
		t.Fatalf("unexpected single trust state: %#v", single)
	}
	if _, err := s.GetTrustState(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound for unknown trust state, got %v", err)
	}
}

func TestTrustQualifiedDiscoveryProjection_RefreshAndQueryModes(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	s := NewPostgresStore(pool)
	handlers := derivation.NewHandlers(pool)
	now := time.Now().UTC()

	notes := []model.Event{
		newDiscoveryTrustEvent("trusted_note", "author_trusted", now.Add(-90*time.Minute), 1, nil),
		newDiscoveryTrustEvent("untrusted_note", "author_untrusted", now.Add(-80*time.Minute), 1, nil),
		newDiscoveryTrustEvent("trusted_note_reply", "replier", now.Add(-70*time.Minute), 1, [][]string{{"e", "trusted_note", "", "reply"}}),
		newDiscoveryTrustEvent("untrusted_note_reply", "replier2", now.Add(-60*time.Minute), 1, [][]string{{"e", "untrusted_note", "", "reply"}}),
	}
	for _, event := range notes {
		if err := s.InsertCanonicalEvent(ctx, event, extractDiscoveryTagsForStoreTest(t, event.RawJSON), "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO trust_seeds (pubkey, is_active) VALUES ('seed', true)`); err != nil {
		t.Fatalf("insert trust seed: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json)
		VALUES
			('edge_seed_trusted', 'seed', 100, 3, 'sig_seed_trusted', '', '{}'::jsonb),
			('edge_seed_untrusted', 'seed', 101, 3, 'sig_seed_untrusted', '', '{}'::jsonb)
	`); err != nil {
		t.Fatalf("insert trust edge events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (source_event_id, follower_pubkey, followed_pubkey, contact_list_created_at, derivation_version)
		VALUES
			('edge_seed_trusted', 'seed', 'author_trusted', 100, 1),
			('edge_seed_untrusted', 'seed', 'author_untrusted', 101, 1)
	`); err != nil {
		t.Fatalf("insert follower edges: %v", err)
	}
	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (
			derivation_name, target_version, status, attempts,
			input_follower_edges_count, score_rows_published
		)
		VALUES ($1, $2, 'succeeded', 1, 2, 2)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO trust_scores_global (pubkey, score, rank, run_id, derivation_name, target_version)
		VALUES
			('author_trusted', 0.9, 1, $1, $2, $3),
			('author_untrusted', 0.1, 2, $1, $2, $3)
	`, runID, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert trust scores: %v", err)
	}
	if _, err := s.RefreshTrustGraphSnapshot(ctx, 3); err != nil {
		t.Fatalf("RefreshTrustGraphSnapshot: %v", err)
	}

	trustedOnly, ready, err := s.GetTrustQualifiedTrendingNotes(ctx, 24*time.Hour, 10, 0, "trusted_only", TrustQualificationPolicy{
		MaxHops:      3,
		MinimumScore: 0.5,
	}, time.Hour)
	if err != nil {
		t.Fatalf("GetTrustQualifiedTrendingNotes trusted_only: %v", err)
	}
	if !ready {
		t.Fatalf("expected trusted discovery projection to be ready")
	}
	if len(trustedOnly) != 1 || trustedOnly[0].Note.EventID != "trusted_note" || !trustedOnly[0].Trusted {
		t.Fatalf("unexpected trusted-only results: %#v", trustedOnly)
	}

	preferTrusted, ready, err := s.GetTrustQualifiedTrendingNotes(ctx, 24*time.Hour, 10, 0, "prefer_trusted", TrustQualificationPolicy{
		MaxHops:      3,
		MinimumScore: 0.5,
	}, time.Hour)
	if err != nil {
		t.Fatalf("GetTrustQualifiedTrendingNotes prefer_trusted: %v", err)
	}
	if !ready {
		t.Fatalf("expected prefer_trusted projection to be ready")
	}
	if len(preferTrusted) < 2 {
		t.Fatalf("expected both trusted and untrusted candidates, got %#v", preferTrusted)
	}
	if !preferTrusted[0].Trusted || preferTrusted[0].Note.EventID != "trusted_note" {
		t.Fatalf("expected trusted note to rank first in prefer_trusted mode, got %#v", preferTrusted)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE trust_graph_snapshot
		SET min_hops = 9,
		    refreshed_at = now() + interval '5 seconds'
		WHERE pubkey = 'author_trusted'
	`); err != nil {
		t.Fatalf("mutate trust graph snapshot: %v", err)
	}
	resultsAfterSnapshotDrift, ready, err := s.GetTrustQualifiedTrendingNotes(ctx, 24*time.Hour, 10, 0, "trusted_only", TrustQualificationPolicy{
		MaxHops:      3,
		MinimumScore: 0.5,
	}, time.Hour)
	if err != nil {
		t.Fatalf("GetTrustQualifiedTrendingNotes after snapshot drift: %v", err)
	}
	if ready || len(resultsAfterSnapshotDrift) != 0 {
		t.Fatalf("expected projection to be marked stale and bypassed after snapshot drift")
	}
}

func newDiscoveryTrustEvent(id, pubkey string, ts time.Time, kind int, tags [][]string) model.Event {
	createdAt := ts.Unix()
	raw := []byte(fmt.Sprintf(`{"id":"%s","pubkey":"%s","created_at":%d,"kind":%d,"tags":%s,"content":"%s","sig":"sig_%s"}`,
		id, pubkey, createdAt, kind, mustJSONTags(tags), id, id))
	return model.Event{
		ID:          id,
		Pubkey:      pubkey,
		CreatedAt:   createdAt,
		Kind:        kind,
		Sig:         "sig_" + id,
		Content:     id,
		RawJSON:     raw,
		FirstSeenAt: ts,
		InsertedAt:  ts,
	}
}

func mustJSONTags(tags [][]string) string {
	if len(tags) == 0 {
		return "[]"
	}
	out := "["
	for i, tag := range tags {
		if i > 0 {
			out += ","
		}
		out += "["
		for j, value := range tag {
			if j > 0 {
				out += ","
			}
			out += fmt.Sprintf("%q", value)
		}
		out += "]"
	}
	out += "]"
	return out
}
