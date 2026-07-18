package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeTrustRetentionStore struct {
	mu                  sync.Mutex
	candidateResults    []int64
	accountResults      []int64
	candidateCalls      int
	accountCalls        int
	candidateErr        error
	accountErr          error
	lastTrustedBefore   time.Time
	lastUntrustedBefore time.Time
	lastLimit           int
}

func (f *fakeTrustRetentionStore) PurgeStaleTrustedDiscoveryCandidates(_ context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.candidateCalls++
	f.lastTrustedBefore = trustedBefore
	f.lastUntrustedBefore = untrustedBefore
	f.lastLimit = limit
	if f.candidateErr != nil {
		return 0, f.candidateErr
	}
	if len(f.candidateResults) == 0 {
		return 0, nil
	}
	n := f.candidateResults[0]
	f.candidateResults = f.candidateResults[1:]
	return n, nil
}

func (f *fakeTrustRetentionStore) PurgeIdleAccountStates(_ context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.accountCalls++
	if f.accountErr != nil {
		return 0, f.accountErr
	}
	if len(f.accountResults) == 0 {
		return 0, nil
	}
	n := f.accountResults[0]
	f.accountResults = f.accountResults[1:]
	return n, nil
}

func trustHooksCfg() TrustRetentionHooksLoopConfig {
	return TrustRetentionHooksLoopConfig{
		DiscoveryCandidates: TrustRetentionHookScope{
			Enabled:          true,
			TrustedHorizon:   24 * time.Hour,
			UntrustedHorizon: 6 * time.Hour,
		},
		EnrichmentState: TrustRetentionHookScope{
			Enabled:          true,
			TrustedHorizon:   12 * time.Hour,
			UntrustedHorizon: 3 * time.Hour,
		},
		RunInterval:      time.Hour,
		DeleteBatchLimit: 100,
	}
}

func TestRunTrustRetentionHooksLoop_AllScopesDisabledNoOp(t *testing.T) {
	store := &fakeTrustRetentionStore{}
	log := &recorderLogger{}
	cfg := trustHooksCfg()
	cfg.DiscoveryCandidates.Enabled = false
	cfg.EnrichmentState.Enabled = false

	RunTrustRetentionHooksLoop(context.Background(), log, store, cfg)

	if store.candidateCalls != 0 || store.accountCalls != 0 {
		t.Fatalf("disabled loop must not purge, got candidates=%d accounts=%d", store.candidateCalls, store.accountCalls)
	}
	if log.countInfo("trust_retention_hooks_disabled") != 1 {
		t.Fatal("expected trust_retention_hooks_disabled log")
	}
}

func TestRunTrustRetentionHooksLoop_InvalidConfig(t *testing.T) {
	store := &fakeTrustRetentionStore{}
	log := &recorderLogger{}
	cfg := trustHooksCfg()
	cfg.DeleteBatchLimit = 0

	RunTrustRetentionHooksLoop(context.Background(), log, store, cfg)

	if store.candidateCalls != 0 || store.accountCalls != 0 {
		t.Fatalf("invalid config must not purge, got candidates=%d accounts=%d", store.candidateCalls, store.accountCalls)
	}
	if len(log.errMsgs) == 0 {
		t.Fatal("expected an invalid-config error log")
	}
}

func TestRunTrustRetentionHooksDrain_BothScopesAndCutoffs(t *testing.T) {
	withShortCatchupPause(t, time.Millisecond)
	store := &fakeTrustRetentionStore{
		candidateResults: []int64{100, 20},
		accountResults:   []int64{5},
	}
	log := &recorderLogger{}
	cfg := trustHooksCfg()

	before := time.Now().UTC()
	runTrustRetentionHooksDrain(context.Background(), log, store, cfg)
	after := time.Now().UTC()

	if store.candidateCalls != 2 {
		t.Fatalf("expected 2 candidate purge calls (1 saturated + 1 below limit), got %d", store.candidateCalls)
	}
	if store.accountCalls != 1 {
		t.Fatalf("expected 1 account purge call, got %d", store.accountCalls)
	}
	if store.lastLimit != cfg.DeleteBatchLimit {
		t.Fatalf("expected limit %d, got %d", cfg.DeleteBatchLimit, store.lastLimit)
	}
	wantTrusted := before.Add(-cfg.DiscoveryCandidates.TrustedHorizon)
	if store.lastTrustedBefore.Before(wantTrusted.Add(-2*time.Second)) || store.lastTrustedBefore.After(after.Add(-cfg.DiscoveryCandidates.TrustedHorizon).Add(2*time.Second)) {
		t.Fatalf("trustedBefore %v not within expected window around %v", store.lastTrustedBefore, wantTrusted)
	}
}

func TestRunTrustRetentionHooksDrain_ScopeErrorDoesNotBlockOtherScope(t *testing.T) {
	store := &fakeTrustRetentionStore{
		candidateErr:   errors.New("db down"),
		accountResults: []int64{3},
	}
	log := &recorderLogger{}

	runTrustRetentionHooksDrain(context.Background(), log, store, trustHooksCfg())

	if store.candidateCalls != 1 {
		t.Fatalf("expected 1 failed candidate call, got %d", store.candidateCalls)
	}
	if store.accountCalls != 1 {
		t.Fatalf("account scope must still run after candidate scope error, got %d calls", store.accountCalls)
	}
}
