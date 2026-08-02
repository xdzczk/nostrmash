package relaycontrol

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayadmission"
	"github.com/xdzczk/nostrmash/internal/relaydiscovery"
	"github.com/xdzczk/nostrmash/internal/relayprobe"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// Controller orchestrates periodic relay registry refresh: seed bootstrap,
// discovery, probing, admission, and desired set publication.
// It lives in the worker runtime.
type Controller struct {
	log                 *slog.Logger
	store               *relayregistry.Store
	pool                *pgxpool.Pool
	cfg                 config.RelayRegistryConfig
	discoveryRunner     *relaydiscovery.Runner
	probeScheduler      *relayprobe.Scheduler
	admissionController *relayadmission.Controller
}

func NewController(log *slog.Logger, store *relayregistry.Store, pool *pgxpool.Pool, cfg config.RelayRegistryConfig) *Controller {
	c := &Controller{log: log, store: store, pool: pool, cfg: cfg}
	if cfg.Discovery.Enabled {
		c.discoveryRunner = relaydiscovery.NewRunner(
			log, pool, store, cfg.Discovery, cfg.AllowPrivateNetwork,
		)
	}
	if cfg.Probing.Enabled {
		c.probeScheduler = relayprobe.NewScheduler(log, store, cfg.Probing)
	}
	c.admissionController = relayadmission.NewController(log, store, cfg.Admission)
	return c
}

// BootstrapSeeds makes RELAY_REGISTRY_SEED_RELAYS authoritative: configured
// seeds are upserted as pinned, and any former source_seed relay no longer in
// the configured set is unpinned (seed flag cleared, pinned → inactive).
func (c *Controller) BootstrapSeeds(ctx context.Context) error {
	return reconcileSeedRelays(ctx, c.log, c.store, c.cfg)
}

type seedRelayStore interface {
	UpsertSeedRelay(ctx context.Context, urlKey, normalizedURL string) error
	ClearMissingSeedRelays(ctx context.Context, keepURLKeys []string) (int64, error)
}

func reconcileSeedRelays(
	ctx context.Context,
	log *slog.Logger,
	store seedRelayStore,
	cfg config.RelayRegistryConfig,
) error {
	opts := relayurl.NormalizeOptions{RequireTLS: !cfg.AllowPrivateNetwork}
	validateOpts := relayurl.ValidateOptions{AllowPrivateNetwork: cfg.AllowPrivateNetwork}

	type seedRef struct {
		urlKey     string
		normalized string
	}
	keep := make([]seedRef, 0, len(cfg.SeedRelays))
	for _, raw := range cfg.SeedRelays {
		normalized, err := relayurl.Normalize(raw, opts)
		if err != nil {
			log.Warn("relay_registry_seed_normalize_failed", "relay", raw, "error", err)
			continue
		}
		if err := relayurl.Validate(normalized, validateOpts); err != nil {
			log.Warn("relay_registry_seed_validate_failed", "relay", normalized, "error", err)
			continue
		}
		keep = append(keep, seedRef{
			urlKey:     relayurl.CanonicalKey(normalized),
			normalized: normalized,
		})
	}

	var bootstrapped int
	for _, seed := range keep {
		if err := store.UpsertSeedRelay(ctx, seed.urlKey, seed.normalized); err != nil {
			// Keep the URL in the keep set even on upsert failure so a
			// transient write error cannot unpin a still-configured seed.
			log.Error("relay_registry_seed_upsert_failed", "relay", seed.normalized, "error", err)
			continue
		}
		bootstrapped++
	}

	keepKeys := make([]string, 0, len(keep))
	for _, seed := range keep {
		keepKeys = append(keepKeys, seed.urlKey)
	}
	cleared, err := store.ClearMissingSeedRelays(ctx, keepKeys)
	if err != nil {
		return fmt.Errorf("clear missing seed relays: %w", err)
	}

	log.Info("relay_registry_seeds_reconciled",
		"total_configured", len(cfg.SeedRelays),
		"bootstrapped", bootstrapped,
		"cleared", cleared,
	)
	return nil
}

// PublishDesiredSetFromRegistry derives and publishes the desired active relay set
// from the current registry state.
func (c *Controller) PublishDesiredSetFromRegistry(ctx context.Context) error {
	urls, err := c.store.GetActiveAndPinnedRelayURLs(ctx)
	if err != nil {
		return fmt.Errorf("get active/pinned urls: %w", err)
	}
	if len(urls) == 0 {
		c.log.Warn("relay_registry_desired_set_empty")
		return nil
	}
	if err := c.store.PublishDesiredSet(ctx, urls, "controller", "periodic refresh"); err != nil {
		return fmt.Errorf("publish desired set: %w", err)
	}
	metrics.SetRelayDesiredActiveCount(float64(len(urls)))
	c.log.Info("relay_registry_desired_set_published", "relay_count", len(urls))
	return nil
}

// RunRefreshLoop starts all control-plane loops and blocks until ctx is cancelled.
// Probing and retention run on their own configured intervals; discovery, admission,
// and desired-set publication run on the main RefreshInterval.
func (c *Controller) RunRefreshLoop(ctx context.Context) {
	if !c.cfg.Enabled {
		c.log.Info("relay_registry_disabled")
		return
	}

	// Launch probing on its own interval if enabled.
	if c.probeScheduler != nil {
		go c.runProbeLoop(ctx)
	}

	// Launch retention on its own interval.
	go c.runRetentionLoop(ctx)

	// Initial refresh cycle.
	c.runRefreshCycle(ctx)

	ticker := time.NewTicker(c.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runRefreshCycle(ctx)
		}
	}
}

// runRefreshCycle handles seeds, discovery, admission, and publication.
// Probing and retention run independently.
func (c *Controller) runRefreshCycle(ctx context.Context) {
	if err := c.BootstrapSeeds(ctx); err != nil {
		c.log.Error("relay_registry_bootstrap_failed", "error", err)
	}
	if c.discoveryRunner != nil {
		if err := c.discoveryRunner.Run(ctx); err != nil {
			c.log.Error("relay_registry_discovery_failed", "error", err)
		}
	}
	if err := c.admissionController.Run(ctx); err != nil {
		c.log.Error("relay_registry_admission_failed", "error", err)
	}
	if err := c.PublishDesiredSetFromRegistry(ctx); err != nil {
		c.log.Error("relay_registry_publish_failed", "error", err)
	}
}

// runProbeLoop runs probe cycles on the configured Probing.Interval.
func (c *Controller) runProbeLoop(ctx context.Context) {
	if err := c.probeScheduler.RunOnce(ctx); err != nil {
		c.log.Error("relay_probe_initial_failed", "error", err)
	}

	ticker := time.NewTicker(c.cfg.Probing.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.probeScheduler.RunOnce(ctx); err != nil {
				c.log.Error("relay_probe_cycle_failed", "error", err)
			}
		}
	}
}

// runRetentionLoop runs observation purge on the configured Retention.PurgeRunInterval.
func (c *Controller) runRetentionLoop(ctx context.Context) {
	ticker := time.NewTicker(c.cfg.Retention.PurgeRunInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runRetention(ctx)
		}
	}
}

func (c *Controller) runRetention(ctx context.Context) {
	if c.cfg.Retention.RawProbeDays <= 0 {
		return
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -c.cfg.Retention.RawProbeDays)
	deleted, err := c.store.PurgeOldObservations(ctx, cutoff, c.cfg.Retention.PurgeBatchLimit)
	if err != nil {
		c.log.Error("relay_registry_retention_purge_failed", "error", err)
		return
	}
	if deleted > 0 {
		c.log.Info("relay_registry_retention_purged", "deleted", deleted)
	}
}
