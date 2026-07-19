package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/model"
)

// TestInvariant_RetentionNeverDeletesTrustedAuthorEvents asserts the core
// retention safety invariant: PurgeUntrustedAuthorEvents removes only events
// whose author is absent from trust_graph_snapshot. A trusted author's
// canonical events survive the purge even when they are well past the horizon.
func TestInvariant_RetentionNeverDeletesTrustedAuthorEvents(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	old := time.Now().AddDate(-1, 0, 0)
	oldUnix := old.UTC().Unix()

	// One trusted author (present in the graph snapshot) and one untrusted.
	if _, err := pool.Exec(ctx, `INSERT INTO trust_graph_snapshot (pubkey, min_hops, is_seed) VALUES ($1, 1, false)`, "trusted_pk"); err != nil {
		t.Fatalf("seed trust snapshot: %v", err)
	}
	insertRawEvent := func(id, pubkey string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `
			INSERT INTO events (id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at)
			VALUES ($1, $2, $3, 1, 'sig', '', '{}'::jsonb, $4, $4)
		`, id, pubkey, oldUnix, old.UTC()); err != nil {
			t.Fatalf("insert event %s: %v", id, err)
		}
	}
	insertRawEvent("evt_trusted", "trusted_pk")
	insertRawEvent("evt_untrusted", "untrusted_pk")

	now := time.Now()
	deleted, err := s.PurgeUntrustedAuthorEvents(ctx, now, now, 100)
	if err != nil {
		t.Fatalf("PurgeUntrustedAuthorEvents: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected exactly 1 untrusted event purged, got %d", deleted)
	}

	assertEventExists := func(id string, want bool) {
		t.Helper()
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = $1)`, id).Scan(&exists); err != nil {
			t.Fatalf("check event %s: %v", id, err)
		}
		if exists != want {
			t.Fatalf("event %s existence: got %v want %v", id, exists, want)
		}
	}
	assertEventExists("evt_trusted", true)
	assertEventExists("evt_untrusted", false)
}

// TestInvariant_CanonicalInsertEnqueuesOutboxJob asserts the outbox parity
// invariant: every newly inserted canonical event enqueues exactly one
// derive_event_bundle job in the same transaction, and a duplicate insert does
// not enqueue a second (idempotent).
func TestInvariant_CanonicalInsertEnqueuesOutboxJob(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)
	s.SetCanonicalEventJobPublisher(jobs.NewQueuePublisher(5))

	now := time.Now().UTC()
	evt := model.Event{
		ID:          "evt_canonical_1",
		Pubkey:      "author_pk",
		CreatedAt:   now.Unix(),
		Kind:        1,
		Sig:         "sig",
		Content:     "hello",
		RawJSON:     json.RawMessage(`{"id":"evt_canonical_1","kind":1}`),
		FirstSeenAt: now,
		InsertedAt:  now,
	}

	if err := s.InsertCanonicalEvent(ctx, evt, nil, "wss://relay.example", now); err != nil {
		t.Fatalf("InsertCanonicalEvent: %v", err)
	}

	idempotencyKey := "derive_event_bundle:" + evt.ID
	countJobs := func() int64 {
		t.Helper()
		var count int64
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE idempotency_key = $1 AND job_type = 'derive_event_bundle'`, idempotencyKey).Scan(&count); err != nil {
			t.Fatalf("count jobs: %v", err)
		}
		return count
	}
	if got := countJobs(); got != 1 {
		t.Fatalf("expected exactly 1 outbox job after canonical insert, got %d", got)
	}

	// Re-inserting the same event id is not a new canonical row, so it must not
	// enqueue a second job.
	if err := s.InsertCanonicalEvent(ctx, evt, nil, "wss://relay.example", now); err != nil {
		t.Fatalf("InsertCanonicalEvent (duplicate): %v", err)
	}
	if got := countJobs(); got != 1 {
		t.Fatalf("duplicate canonical insert must not enqueue a second job, got %d", got)
	}
}

// TestInvariant_AccountStateTransitionsAuditedOnEffectiveChange asserts that an
// account-state audit transition is written exactly when the effective state
// changes, and never when it does not.
func TestInvariant_AccountStateTransitionsAuditedOnEffectiveChange(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")
	s := NewPostgresStore(pool)

	const pubkey = "account_pk"
	if err := s.BatchIncrementAccountObservations(ctx, map[string]int64{pubkey: 1}); err != nil {
		t.Fatalf("BatchIncrementAccountObservations: %v", err)
	}

	countTransitions := func() int64 {
		t.Helper()
		var count int64
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM account_state_transitions WHERE pubkey = $1`, pubkey).Scan(&count); err != nil {
			t.Fatalf("count transitions: %v", err)
		}
		return count
	}

	// Effective state does not change: unknown -> unknown. No audit row.
	if err := s.ApplyAccountState(ctx, pubkey, "unknown", "unknown", "unknown", "derived", "noop"); err != nil {
		t.Fatalf("ApplyAccountState (no change): %v", err)
	}
	if got := countTransitions(); got != 0 {
		t.Fatalf("no transition expected when effective state is unchanged, got %d", got)
	}

	// Effective state changes: unknown -> candidate. Exactly one audit row.
	if err := s.ApplyAccountState(ctx, pubkey, "unknown", "candidate", "candidate", "derived", "promote"); err != nil {
		t.Fatalf("ApplyAccountState (change): %v", err)
	}
	if got := countTransitions(); got != 1 {
		t.Fatalf("exactly one transition expected on effective change, got %d", got)
	}

	// A manual override that changes the effective state (candidate -> blocked)
	// audits from the DB-observed previous state.
	if _, err := s.SetAccountManualOverride(ctx, pubkey, "blocked", "ops block"); err != nil {
		t.Fatalf("SetAccountManualOverride: %v", err)
	}
	if got := countTransitions(); got != 2 {
		t.Fatalf("manual override effective change must audit, got %d transitions", got)
	}

	var from, to, source string
	if err := pool.QueryRow(ctx, `
		SELECT from_state, to_state, source
		FROM account_state_transitions
		WHERE pubkey = $1
		ORDER BY id DESC
		LIMIT 1
	`, pubkey).Scan(&from, &to, &source); err != nil {
		t.Fatalf("read latest transition: %v", err)
	}
	if from != "candidate" || to != "blocked" || source != "manual" {
		t.Fatalf("unexpected latest transition: from=%q to=%q source=%q", from, to, source)
	}
}
