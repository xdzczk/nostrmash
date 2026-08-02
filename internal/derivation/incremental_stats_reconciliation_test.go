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

// TestReconcileIncrementalAuthorStatsSample_NoMismatchWhenConsistent is the
// happy-path check: after normal incremental processing, a reconciliation
// pass over the affected pubkeys must report zero mismatches. This is the
// steady-state expectation the periodic reconciliation loop relies on to
// treat any mismatch as a real signal, not background noise.
func TestReconcileIncrementalAuthorStatsSample_NoMismatchWhenConsistent(t *testing.T) {
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
		"recon_alice_note",
		"recon_alice",
		now.Add(-2*time.Hour).Unix(),
		1,
		nil,
		`{"content":"note"}`,
		now,
	)
	bobReply := newEventForTest(
		"recon_bob_reply",
		"recon_bob",
		now.Add(-1*time.Hour).Unix(),
		1,
		[][]string{{"e", "recon_alice_note", "", "reply"}},
		`{"content":"nice"}`,
		now,
	)
	carolReaction := newEventForTest(
		"recon_carol_reaction",
		"recon_carol",
		now.Add(-30*time.Minute).Unix(),
		7,
		[][]string{{"e", "recon_alice_note"}},
		`{"content":"+"}`,
		now,
	)

	for _, event := range []model.Event{aliceNote, bobReply, carolReaction} {
		tags := extractTagsFromRaw(t, event.RawJSON)
		if err := pgStore.InsertCanonicalEvent(ctx, event, tags, "wss://relay.one", event.FirstSeenAt); err != nil {
			t.Fatalf("insert event %s: %v", event.ID, err)
		}
		if err := handlers.DeriveEventBundle(ctx, event.ID); err != nil {
			t.Fatalf("derive event bundle %s: %v", event.ID, err)
		}
	}

	report, err := handlers.ReconcileIncrementalAuthorStatsSample(ctx, 50)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if report.SampledPubkeys == 0 {
		t.Fatal("expected at least one sampled pubkey")
	}
	if len(report.Mismatches) != 0 {
		t.Fatalf("expected zero mismatches, got %#v", report.Mismatches)
	}
}

// TestReconcileIncrementalAuthorStatsSample_DetectsInjectedDrift proves the
// reconciliation pass actually catches drift: after normal processing, we
// directly corrupt the incrementally-maintained profile_public_stats and
// author_activity_daily rows (simulating a fan-out bug that silently skips
// or double-applies a delta) and assert the mismatch is reported with the
// correct before/after values.
func TestReconcileIncrementalAuthorStatsSample_DetectsInjectedDrift(t *testing.T) {
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

	driftNote := newEventForTest(
		"recon_drift_note",
		"recon_drift",
		now.Add(-1*time.Hour).Unix(),
		1,
		nil,
		`{"content":"note"}`,
		now,
	)
	tags := extractTagsFromRaw(t, driftNote.RawJSON)
	if err := pgStore.InsertCanonicalEvent(ctx, driftNote, tags, "wss://relay.one", driftNote.FirstSeenAt); err != nil {
		t.Fatalf("insert event %s: %v", driftNote.ID, err)
	}
	if err := handlers.DeriveEventBundle(ctx, driftNote.ID); err != nil {
		t.Fatalf("derive event bundle %s: %v", driftNote.ID, err)
	}

	// Simulate a fan-out bug: silently double-count note_count and
	// under-count author_activity_daily's post_count for this pubkey,
	// without ever re-running the real derivation path.
	if _, err := pool.Exec(ctx, `UPDATE profile_public_stats SET note_count = note_count + 1 WHERE pubkey = $1`, driftNote.Pubkey); err != nil {
		t.Fatalf("inject profile_public_stats drift: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE author_activity_daily SET post_count = 0 WHERE pubkey = $1`, driftNote.Pubkey); err != nil {
		t.Fatalf("inject author_activity_daily drift: %v", err)
	}

	report, err := handlers.ReconcileIncrementalAuthorStatsSample(ctx, 50)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	foundProfileMismatch := false
	foundActivityMismatch := false
	for _, m := range report.Mismatches {
		if m.Pubkey != driftNote.Pubkey {
			continue
		}
		if m.Projection == "profile_public_stats" && m.Field == "note_count" {
			foundProfileMismatch = true
			if m.Incremental != 2 || m.Recomputed != 1 {
				t.Fatalf("unexpected note_count mismatch values: %#v", m)
			}
		}
		if m.Projection == "author_activity_daily" && m.Field == "post_count_total" {
			foundActivityMismatch = true
			if m.Incremental != 0 || m.Recomputed != 1 {
				t.Fatalf("unexpected post_count_total mismatch values: %#v", m)
			}
		}
	}
	if !foundProfileMismatch {
		t.Fatalf("expected a profile_public_stats note_count mismatch, got %#v", report.Mismatches)
	}
	if !foundActivityMismatch {
		t.Fatalf("expected an author_activity_daily post_count_total mismatch, got %#v", report.Mismatches)
	}
}
