package runtime

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/account"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

// AccountStateStore is the slice of *store.PostgresStore the recompute loop
// needs.
type AccountStateStore interface {
	ListAccountSignalsForRecompute(ctx context.Context, limit int, staleBefore time.Time) ([]store.AccountSignalRow, error)
	ApplyAccountState(ctx context.Context, pubkey, fromState, derived, effective, source, reason string) error
	CountAccountStates(ctx context.Context) (map[string]int64, error)
	PurgeAccountStateTransitionsOlderThan(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

var allAccountStates = []string{
	string(account.StateUnknown),
	string(account.StateObserved),
	string(account.StateCandidate),
	string(account.StateMeaningful),
	string(account.StateTrusted),
	string(account.StateTracked),
	string(account.StateStrategic),
	string(account.StateBlocked),
}

// RunAccountStateRecomputeLoop periodically recomputes derived account states
// from cheap signals, records transitions, refreshes per-state count metrics,
// and prunes the transition audit table. The derivation itself is the pure
// account.Resolve/EffectiveState pair, so the loop is a thin orchestration
// shell around tested logic.
func RunAccountStateRecomputeLoop(ctx context.Context, log Logger, s AccountStateStore, cfg config.WorkerAccountStateConfig) {
	if !cfg.Enabled {
		log.Info("account_state_recompute_disabled")
		return
	}
	if s == nil {
		log.Error("account_state_recompute_no_store")
		return
	}
	if cfg.Interval <= 0 || cfg.BatchSize <= 0 {
		log.Error("account_state_recompute_invalid_config", "interval", cfg.Interval.String(), "batch_size", cfg.BatchSize)
		return
	}
	log.Info(
		"account_state_recompute_enabled",
		"interval", cfg.Interval.String(),
		"batch_size", cfg.BatchSize,
		"stale_after", cfg.StaleAfter.String(),
		"transition_max_age", cfg.TransitionRetentionMaxAge.String(),
	)

	refreshCounts(ctx, log, s)

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			recomputeAccountStatesOnce(ctx, log, s, cfg)
			refreshCounts(ctx, log, s)
			if cfg.TransitionRetentionMaxAge > 0 {
				cutoff := time.Now().UTC().Add(-cfg.TransitionRetentionMaxAge)
				if deleted, err := s.PurgeAccountStateTransitionsOlderThan(ctx, cutoff, 5000); err != nil {
					log.Error("account_state_transition_purge_failed", "error", err)
				} else if deleted > 0 {
					log.Info("account_state_transitions_purged", "deleted", deleted)
				}
			}
		}
	}
}

func recomputeAccountStatesOnce(ctx context.Context, log Logger, s AccountStateStore, cfg config.WorkerAccountStateConfig) {
	var staleBefore time.Time
	if cfg.StaleAfter > 0 {
		staleBefore = time.Now().UTC().Add(-cfg.StaleAfter)
	}
	signals, err := s.ListAccountSignalsForRecompute(ctx, cfg.BatchSize, staleBefore)
	if err != nil {
		log.Error("account_state_recompute_list_failed", "error", err)
		return
	}
	changed := 0
	for _, row := range signals {
		derived := account.Resolve(account.Signals{
			TrustHops:          row.TrustHops,
			ObservedCount:      row.ObservedCount,
			HasProfileMetadata: row.HasProfile,
			NoteCount:          row.NoteCount,
		}, account.DefaultParams)

		var override account.State
		if row.ManualOverride != nil {
			if parsed, ok := account.Parse(*row.ManualOverride); ok {
				override = parsed
			}
		}
		effective := account.EffectiveState(derived, override, row.Tracked)

		if string(derived) == row.CurrentDerived && string(effective) == row.CurrentState {
			continue
		}
		if err := s.ApplyAccountState(ctx, row.Pubkey, row.CurrentState, string(derived), string(effective), "derived", "recompute"); err != nil {
			log.Error("account_state_apply_failed", "pubkey", row.Pubkey, "error", err)
			continue
		}
		if string(effective) != row.CurrentState {
			metrics.IncAccountStateTransition(string(effective))
			changed++
		}
	}
	if changed > 0 {
		log.Info("account_state_recompute_applied", "transitions", changed, "scanned", len(signals))
	}
}

func refreshCounts(ctx context.Context, log Logger, s AccountStateStore) {
	counts, err := s.CountAccountStates(ctx)
	if err != nil {
		log.Error("account_state_count_failed", "error", err)
		return
	}
	for _, state := range allAccountStates {
		metrics.SetAccountStateCount(state, float64(counts[state]))
	}
}
