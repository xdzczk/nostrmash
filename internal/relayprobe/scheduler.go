package relayprobe

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// Scheduler runs periodic probe cycles against registry relays.
type Scheduler struct {
	log           *slog.Logger
	registryStore *relayregistry.Store
	prober        *Prober
	cfg           config.RelayRegistryProbingConfig
}

func NewScheduler(
	log *slog.Logger,
	registryStore *relayregistry.Store,
	cfg config.RelayRegistryProbingConfig,
) *Scheduler {
	return &Scheduler{
		log:           log,
		registryStore: registryStore,
		prober: NewProber(ProbeConfig{
			ConnectTimeout: cfg.TimeoutConnect,
			EOSETimeout:    cfg.TimeoutEOSE,
		}),
		cfg: cfg,
	}
}

// RunOnce executes a single probe cycle over eligible relays.
//
// The live/discovery split from ListRelaysForProbing is unchanged. Catalog
// targets are an extra slice so keeping popular relays fresh does not steal
// the discovery budget.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}

	base, err := s.registryStore.ListRelaysForProbing(ctx, s.cfg.MaxParallel*2)
	if err != nil {
		return err
	}
	var catalog []relayregistry.RelayRecord
	if budget := CatalogProbeBudget(s.cfg.CatalogHead, s.cfg.Interval, s.cfg.CatalogFreshness); budget > 0 {
		catalog, err = s.registryStore.ListCatalogProbeTargets(ctx, s.cfg.CatalogHead, budget)
		if err != nil {
			return err
		}
	}
	relays := mergeProbeTargets(base, catalog)
	if len(relays) == 0 {
		return nil
	}

	sem := make(chan struct{}, s.cfg.MaxParallel)
	var wg sync.WaitGroup

	for _, relay := range relays {
		select {
		case <-ctx.Done():
			break
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(rec relayregistry.RelayRecord) {
			defer wg.Done()
			defer func() { <-sem }()

			result := s.prober.Probe(ctx, rec.NormalizedURL)
			metrics.IncRelayProbeResult(string(result.Status))
			if result.ConnectOK {
				metrics.ObserveRelayProbeLatency("connect", result.ConnectLatencyMs/1000.0)
			}
			if result.EOSEOK {
				metrics.ObserveRelayProbeLatency("eose", result.EOSELatencyMs/1000.0)
			}
			s.persistResult(ctx, rec, result)
		}(relay)
	}

	wg.Wait()
	s.log.Info("relay_probe_cycle_completed",
		"probed", len(relays),
		"catalog_added", len(relays)-len(base),
	)
	return nil
}

// CatalogProbeBudget is how many extra catalog-head dials one cycle needs so
// the head is refreshed inside freshness. It is not taken from the live or
// discovery budgets. 80 relays, a 5m interval, and a 60m window is 7 dials.
func CatalogProbeBudget(head int, interval, freshness time.Duration) int {
	if head <= 0 || interval <= 0 || freshness <= 0 {
		return 0
	}
	cycles := int(freshness / interval)
	if cycles < 1 {
		cycles = 1
	}
	n := (head + cycles - 1) / cycles
	if n > head {
		return head
	}
	return n
}

// mergeProbeTargets appends catalog targets whose URL key is not already in
// the base probe list. Base order and length are preserved.
func mergeProbeTargets(base, extra []relayregistry.RelayRecord) []relayregistry.RelayRecord {
	if len(extra) == 0 {
		return base
	}
	seen := make(map[string]struct{}, len(base)+len(extra))
	for _, rec := range base {
		seen[rec.URLKey] = struct{}{}
	}
	out := base
	for _, rec := range extra {
		if _, ok := seen[rec.URLKey]; ok {
			continue
		}
		seen[rec.URLKey] = struct{}{}
		out = append(out, rec)
	}
	return out
}

// RunLoop runs probe cycles at the configured interval.
func (s *Scheduler) RunLoop(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info("relay_probing_disabled")
		return
	}

	if err := s.RunOnce(ctx); err != nil {
		s.log.Error("relay_probe_initial_failed", "error", err)
	}

	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil {
				s.log.Error("relay_probe_cycle_failed", "error", err)
			}
		}
	}
}

func (s *Scheduler) persistResult(ctx context.Context, rec relayregistry.RelayRecord, result ProbeResult) {
	now := time.Now().UTC()
	var errCode, errText *string
	if result.ErrorCode != "" {
		errCode = &result.ErrorCode
	}
	if result.ErrorText != "" {
		errText = &result.ErrorText
	}

	var connectLatency, eoseLatency *float64
	if result.ConnectOK {
		connectLatency = &result.ConnectLatencyMs
	}
	if result.EOSEOK {
		eoseLatency = &result.EOSELatencyMs
	}

	obs := relayregistry.ProbeObservation{
		URLKey:           rec.URLKey,
		ProbedAt:         now,
		ConnectOK:        result.ConnectOK,
		SubscribeOK:      result.SubscribeOK,
		EOSEOK:           result.EOSEOK,
		ConnectLatencyMs: connectLatency,
		EOSELatencyMs:    eoseLatency,
		ErrorCode:        errCode,
		ErrorTextShort:   errText,
	}
	if result.SampleYieldCount > 0 {
		obs.SampleYieldCount = &result.SampleYieldCount
	}

	if err := s.registryStore.InsertProbeObservation(ctx, obs); err != nil {
		s.log.Warn("relay_probe_observation_persist_failed",
			"relay", rec.NormalizedURL,
			"error", err,
		)
	}

	probeFailRate := rec.ProbeFailRate
	if result.Status == relayregistry.ProbeStatusOK {
		probeFailRate = probeFailRate * 0.9
	} else {
		probeFailRate = probeFailRate*0.9 + 0.1
	}

	if err := s.registryStore.UpdateProbeRollup(
		ctx, rec.URLKey, result.Status,
		result.ConnectOK, result.SubscribeOK, result.EOSEOK,
		connectLatency, eoseLatency,
		probeFailRate, rec.YieldScore, rec.DuplicateRatio,
	); err != nil {
		s.log.Warn("relay_probe_rollup_update_failed",
			"relay", rec.NormalizedURL,
			"error", err,
		)
	}
}
