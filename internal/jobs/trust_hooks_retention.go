package jobs

import (
	"context"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

const (
	trustDiscoveryCandidatesTarget = "trusted_discovery_candidates"
	trustAccountStatesTarget       = "account_states_idle"
)

// TrustRetentionStore performs the durable deletes behind the trust retention
// hooks. Satisfied by *store.PostgresStore.
type TrustRetentionStore interface {
	PurgeStaleTrustedDiscoveryCandidates(ctx context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error)
	PurgeIdleAccountStates(ctx context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error)
}

// TrustRetentionHookScope is one trust-aware retention scope: rows classified
// as trusted keep the longer horizon, everything else the shorter one.
type TrustRetentionHookScope struct {
	Enabled          bool
	TrustedHorizon   time.Duration
	UntrustedHorizon time.Duration
}

// TrustRetentionHooksLoopConfig configures the durable trust-retention loop.
// The in-process scopes (discovery cache TTLs, fallback transient metadata)
// are enforced inside internal/query and are not part of this loop.
type TrustRetentionHooksLoopConfig struct {
	DiscoveryCandidates TrustRetentionHookScope
	EnrichmentState     TrustRetentionHookScope
	RunInterval         time.Duration
	DeleteBatchLimit    int
}

func (c TrustRetentionHooksLoopConfig) anyEnabled() bool {
	return c.DiscoveryCandidates.Enabled || c.EnrichmentState.Enabled
}

// RunTrustRetentionHooksLoop periodically applies the durable trust-retention
// hooks: stale trusted discovery candidates and idle low-value account_states
// rows. Blocks until ctx is done.
func RunTrustRetentionHooksLoop(ctx context.Context, log RetentionLogger, store TrustRetentionStore, cfg TrustRetentionHooksLoopConfig) {
	if !cfg.anyEnabled() {
		log.Info("trust_retention_hooks_disabled")
		return
	}
	if cfg.RunInterval <= 0 || cfg.DeleteBatchLimit <= 0 {
		log.Error(
			"trust_retention_hooks_invalid_config",
			"run_interval", cfg.RunInterval.String(),
			"delete_batch_limit", cfg.DeleteBatchLimit,
		)
		return
	}
	log.Info(
		"trust_retention_hooks_enabled",
		"discovery_candidates_enabled", cfg.DiscoveryCandidates.Enabled,
		"enrichment_state_enabled", cfg.EnrichmentState.Enabled,
		"run_interval", cfg.RunInterval.String(),
		"delete_batch_limit", cfg.DeleteBatchLimit,
	)

	ticker := time.NewTicker(cfg.RunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		runTrustRetentionHooksDrain(ctx, log, store, cfg)
	}
}

func runTrustRetentionHooksDrain(ctx context.Context, log RetentionLogger, store TrustRetentionStore, cfg TrustRetentionHooksLoopConfig) {
	if cfg.DiscoveryCandidates.Enabled {
		drainTrustScope(
			ctx, log, cfg, trustDiscoveryCandidatesTarget, cfg.DiscoveryCandidates,
			store.PurgeStaleTrustedDiscoveryCandidates,
		)
	}
	if cfg.EnrichmentState.Enabled {
		drainTrustScope(
			ctx, log, cfg, trustAccountStatesTarget, cfg.EnrichmentState,
			store.PurgeIdleAccountStates,
		)
	}
}

func drainTrustScope(
	ctx context.Context,
	log RetentionLogger,
	cfg TrustRetentionHooksLoopConfig,
	target string,
	scope TrustRetentionHookScope,
	purge func(ctx context.Context, trustedBefore, untrustedBefore time.Time, limit int) (int64, error),
) {
	consecutiveSaturated := 0
	for {
		now := time.Now().UTC()
		trustedBefore := now.Add(-scope.TrustedHorizon)
		untrustedBefore := now.Add(-scope.UntrustedHorizon)
		deleted, err := purge(ctx, trustedBefore, untrustedBefore, cfg.DeleteBatchLimit)
		if err != nil {
			metrics.IncRetentionPurgeRun(target, "error")
			log.Error("trust_retention_hooks_purge_failed", "target", target, "error", err)
			return
		}
		metrics.IncRetentionPurgeRun(target, "ok")
		metrics.AddRetentionPurgedRows(target, deleted)
		if deleted > 0 {
			log.Info(
				"trust_retention_hooks_purged",
				"target", target,
				"deleted", deleted,
				"trusted_before", trustedBefore.Format(time.RFC3339),
				"untrusted_before", untrustedBefore.Format(time.RFC3339),
			)
		}
		if int(deleted) < cfg.DeleteBatchLimit {
			return
		}
		consecutiveSaturated++
		if consecutiveSaturated%retentionCatchupReportEvery == 0 {
			log.Info(
				"trust_retention_hooks_catchup",
				"target", target,
				"consecutive_full_batches", consecutiveSaturated,
				"delete_batch_limit", cfg.DeleteBatchLimit,
			)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retentionCatchupPause):
		}
	}
}
