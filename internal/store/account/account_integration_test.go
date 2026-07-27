package account_test

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/store/account"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func setupAccountStore(t *testing.T) (context.Context, *account.Accounts) {
	t.Helper()
	ctx := context.Background()
	pool := dbtest.SetupSchemaPool(t, ctx, dbtest.DatabaseURL(t, "account"), "account")
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	return ctx, account.New(pool)
}

func TestAccountsLifecycle(t *testing.T) {
	ctx, s := setupAccountStore(t)
	const pubkey = "Account_PK"

	if err := s.BatchIncrementAccountObservations(ctx, map[string]int64{pubkey: 2, "": 9, "other": 0}); err != nil {
		t.Fatalf("BatchIncrementAccountObservations: %v", err)
	}

	state, err := s.GetAccountState(ctx, pubkey)
	if err != nil {
		t.Fatalf("GetAccountState: %v", err)
	}
	if !state.Exists || state.Pubkey != "account_pk" || state.ObservedCount != 2 || state.State != "unknown" {
		t.Fatalf("unexpected initial state: %+v", state)
	}

	missing, err := s.GetAccountState(ctx, "missing_pk")
	if err != nil {
		t.Fatalf("GetAccountState missing: %v", err)
	}
	if missing.Exists {
		t.Fatalf("missing pubkey should not exist: %+v", missing)
	}

	if err := s.ApplyAccountState(ctx, pubkey, "unknown", "unknown", "unknown", "derived", "noop"); err != nil {
		t.Fatalf("ApplyAccountState noop: %v", err)
	}
	if err := s.ApplyAccountState(ctx, pubkey, "unknown", "candidate", "candidate", "derived", "promote"); err != nil {
		t.Fatalf("ApplyAccountState promote: %v", err)
	}

	from, err := s.SetAccountManualOverride(ctx, pubkey, "blocked", "ops")
	if err != nil {
		t.Fatalf("SetAccountManualOverride: %v", err)
	}
	if from != "candidate" {
		t.Fatalf("override from-state = %q, want candidate", from)
	}
	state, err = s.GetAccountState(ctx, pubkey)
	if err != nil {
		t.Fatalf("GetAccountState after override: %v", err)
	}
	if state.ManualOverride == nil || *state.ManualOverride != "blocked" || state.State != "blocked" {
		t.Fatalf("unexpected overridden state: %+v", state)
	}

	from, err = s.SetAccountManualOverride(ctx, pubkey, "", "clear")
	if err != nil {
		t.Fatalf("clear override: %v", err)
	}
	if from != "blocked" {
		t.Fatalf("clear from-state = %q, want blocked", from)
	}

	from, err = s.PromoteAccountToTracked(ctx, pubkey, "hydrate")
	if err != nil {
		t.Fatalf("PromoteAccountToTracked: %v", err)
	}
	if from == "" {
		t.Fatal("expected previous state from promote")
	}
	state, err = s.GetAccountState(ctx, pubkey)
	if err != nil {
		t.Fatalf("GetAccountState after promote: %v", err)
	}
	if state.FirstTrackedAt == nil {
		t.Fatalf("expected first_tracked_at after promote: %+v", state)
	}

	now := time.Now().UTC()
	days := 30
	if err := s.UpdateAccountCoverage(ctx, pubkey, &now, &now, &now, &now, &now, &days); err != nil {
		t.Fatalf("UpdateAccountCoverage: %v", err)
	}
	state, err = s.GetAccountState(ctx, pubkey)
	if err != nil {
		t.Fatalf("GetAccountState after coverage: %v", err)
	}
	if state.CoverageWindowDays == nil || *state.CoverageWindowDays != 30 || state.LastHydratedAt == nil {
		t.Fatalf("unexpected coverage fields: %+v", state)
	}

	accept, err := s.LoadIngestAcceptPubkeys(ctx)
	if err != nil {
		t.Fatalf("LoadIngestAcceptPubkeys: %v", err)
	}
	if len(accept) == 0 {
		t.Fatal("expected at least one ingest-accept pubkey after promote")
	}

	if _, err := s.SetAccountManualOverride(ctx, "blocked_pk", "blocked", "seed block"); err != nil {
		t.Fatalf("create blocked account: %v", err)
	}
	blocked, err := s.LoadBlockedPubkeys(ctx)
	if err != nil {
		t.Fatalf("LoadBlockedPubkeys: %v", err)
	}
	if len(blocked) == 0 {
		t.Fatal("expected blocked pubkeys")
	}

	counts, err := s.CountAccountStates(ctx)
	if err != nil {
		t.Fatalf("CountAccountStates: %v", err)
	}
	if len(counts) == 0 {
		t.Fatal("expected account state counts")
	}

	signals, err := s.ListAccountSignalsForRecompute(ctx, 10, time.Time{})
	if err != nil {
		t.Fatalf("ListAccountSignalsForRecompute: %v", err)
	}
	if len(signals) == 0 {
		t.Fatal("expected recompute signals")
	}

	deleted, err := s.PurgeAccountStateTransitionsOlderThan(ctx, time.Now().UTC().Add(time.Hour), 100)
	if err != nil {
		t.Fatalf("PurgeAccountStateTransitionsOlderThan: %v", err)
	}
	if deleted < 1 {
		t.Fatalf("expected to purge audit transitions, got %d", deleted)
	}
}

func TestAccountsValidationErrors(t *testing.T) {
	ctx, s := setupAccountStore(t)
	if err := s.ApplyAccountState(ctx, "  ", "unknown", "unknown", "unknown", "derived", "x"); err == nil {
		t.Fatal("expected empty pubkey error")
	}
	if _, err := s.SetAccountManualOverride(ctx, "", "blocked", "x"); err == nil {
		t.Fatal("expected empty pubkey override error")
	}
	if _, err := s.PromoteAccountToTracked(ctx, " ", "x"); err == nil {
		t.Fatal("expected empty pubkey promote error")
	}
	if _, err := s.GetAccountState(ctx, ""); err == nil {
		t.Fatal("expected empty pubkey get error")
	}
}
