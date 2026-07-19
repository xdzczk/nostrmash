package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	storeaccount "github.com/xdzczk/nostrmash/internal/store/account"
)

func accountStateTestConfig() config.WorkerAccountStateConfig {
	return config.WorkerAccountStateConfig{
		Enabled:   true,
		Interval:  time.Minute,
		BatchSize: 100,
	}
}

func TestRunAccountStateRecomputeLoop_GuardRails(t *testing.T) {
	t.Run("disabled", func(t *testing.T) {
		log := &recordingLogger{}
		cfg := accountStateTestConfig()
		cfg.Enabled = false
		RunAccountStateRecomputeLoop(context.Background(), log, &fakeAccountStateStore{}, cfg)
		if !log.sawInfo("account_state_recompute_disabled") {
			t.Fatal("expected disabled info log")
		}
	})

	t.Run("nil store", func(t *testing.T) {
		log := &recordingLogger{}
		RunAccountStateRecomputeLoop(context.Background(), log, nil, accountStateTestConfig())
		if !log.sawError("account_state_recompute_no_store") {
			t.Fatal("expected no-store error log")
		}
	})

	t.Run("invalid config", func(t *testing.T) {
		log := &recordingLogger{}
		cfg := accountStateTestConfig()
		cfg.BatchSize = 0
		RunAccountStateRecomputeLoop(context.Background(), log, &fakeAccountStateStore{counts: map[string]int64{}}, cfg)
		if !log.sawError("account_state_recompute_invalid_config") {
			t.Fatal("expected invalid-config error log")
		}
	})
}

func TestRecomputeAccountStatesOnce_AppliesChangedStates(t *testing.T) {
	log := &recordingLogger{}
	store := &fakeAccountStateStore{
		signals: []storeaccount.AccountSignalRow{
			// unknown -> observed (ObservedCount>=2), state changes.
			{Pubkey: "pk_change", TrustHops: -1, ObservedCount: 2, CurrentState: "unknown", CurrentDerived: "unknown"},
			// already observed, no change.
			{Pubkey: "pk_same", TrustHops: -1, ObservedCount: 2, CurrentState: "observed", CurrentDerived: "observed"},
		},
	}
	recomputeAccountStatesOnce(context.Background(), log, store, accountStateTestConfig())

	if len(store.applied) != 1 {
		t.Fatalf("expected exactly one apply, got %d: %+v", len(store.applied), store.applied)
	}
	got := store.applied[0]
	if got.pubkey != "pk_change" || got.derived != "observed" || got.effective != "observed" {
		t.Fatalf("unexpected applied state: %+v", got)
	}
}

func TestRecomputeAccountStatesOnce_ListErrorIsHandled(t *testing.T) {
	log := &recordingLogger{}
	store := &fakeAccountStateStore{listErr: errors.New("db down")}
	recomputeAccountStatesOnce(context.Background(), log, store, accountStateTestConfig())
	if !log.sawError("account_state_recompute_list_failed") {
		t.Fatal("expected list-failed error log")
	}
	if len(store.applied) != 0 {
		t.Fatal("no states should be applied when listing fails")
	}
}

func TestRecomputeAccountStatesOnce_ApplyErrorContinues(t *testing.T) {
	log := &recordingLogger{}
	store := &fakeAccountStateStore{
		applyErr: errors.New("apply failed"),
		signals: []storeaccount.AccountSignalRow{
			{Pubkey: "pk1", TrustHops: -1, ObservedCount: 2, CurrentState: "unknown", CurrentDerived: "unknown"},
			{Pubkey: "pk2", TrustHops: -1, ObservedCount: 2, CurrentState: "unknown", CurrentDerived: "unknown"},
		},
	}
	recomputeAccountStatesOnce(context.Background(), log, store, accountStateTestConfig())
	if !log.sawError("account_state_apply_failed") {
		t.Fatal("expected apply-failed error log")
	}
}

func TestRefreshCounts(t *testing.T) {
	t.Run("success does not error", func(t *testing.T) {
		log := &recordingLogger{}
		store := &fakeAccountStateStore{counts: map[string]int64{"observed": 3, "trusted": 1}}
		refreshCounts(context.Background(), log, store)
		if log.sawError("account_state_count_failed") {
			t.Fatal("did not expect count-failed error")
		}
	})

	t.Run("count error is logged", func(t *testing.T) {
		log := &recordingLogger{}
		store := &fakeAccountStateStore{countErr: errors.New("boom")}
		refreshCounts(context.Background(), log, store)
		if !log.sawError("account_state_count_failed") {
			t.Fatal("expected count-failed error log")
		}
	})
}
