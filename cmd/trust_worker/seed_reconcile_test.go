package main

import (
	"context"
	"errors"
	"testing"
)

type fakeSeedStore struct {
	upsertArgs     [][]string
	deactivateArgs [][]string
	upsertResult   int64
	deactivateRes  int64
	upsertErr      error
	deactivateErr  error
}

func (f *fakeSeedStore) UpsertActiveSeeds(_ context.Context, pubkeys []string) (int64, error) {
	f.upsertArgs = append(f.upsertArgs, append([]string(nil), pubkeys...))
	return f.upsertResult, f.upsertErr
}

func (f *fakeSeedStore) DeactivateMissingSeeds(_ context.Context, keep []string) (int64, error) {
	f.deactivateArgs = append(f.deactivateArgs, append([]string(nil), keep...))
	return f.deactivateRes, f.deactivateErr
}

func TestReconcileTrustSeeds_SkipsWhenNoSeeds(t *testing.T) {
	store := &fakeSeedStore{}
	if err := reconcileTrustSeeds(context.Background(), fakeTrustWorkerLogger{}, store, nil); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(store.upsertArgs) != 0 || len(store.deactivateArgs) != 0 {
		t.Fatal("reconcile must not touch the store when no seeds are configured")
	}
}

func TestReconcileTrustSeeds_UpsertsThenDeactivatesAuthoritatively(t *testing.T) {
	store := &fakeSeedStore{upsertResult: 2, deactivateRes: 1}
	seeds := []string{"alice", "bob"}
	if err := reconcileTrustSeeds(context.Background(), fakeTrustWorkerLogger{}, store, seeds); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if len(store.upsertArgs) != 1 || len(store.deactivateArgs) != 1 {
		t.Fatalf("expected one upsert and one deactivate call, got %d/%d", len(store.upsertArgs), len(store.deactivateArgs))
	}
	// The keep set passed to deactivate must be exactly the configured seeds.
	if len(store.deactivateArgs[0]) != 2 {
		t.Fatalf("expected deactivate keep set of 2, got %v", store.deactivateArgs[0])
	}
}

func TestReconcileTrustSeeds_PropagatesUpsertError(t *testing.T) {
	store := &fakeSeedStore{upsertErr: errors.New("boom")}
	if err := reconcileTrustSeeds(context.Background(), fakeTrustWorkerLogger{}, store, []string{"alice"}); err == nil {
		t.Fatal("expected error from failing upsert")
	}
	if len(store.deactivateArgs) != 0 {
		t.Fatal("deactivate must not run when upsert fails")
	}
}
