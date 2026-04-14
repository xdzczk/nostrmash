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
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if !s.cfg.Enabled {
		return nil
	}

	relays, err := s.registryStore.ListRelaysForProbing(ctx, s.cfg.MaxParallel*2)
	if err != nil {
		return err
	}
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
	s.log.Info("relay_probe_cycle_completed", "probed", len(relays))
	return nil
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
