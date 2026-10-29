package relaynip11

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

type documentFetcher interface {
	Fetch(ctx context.Context, relayURL string) (Info, error)
}

type capabilityStore interface {
	ListRelaysForNIP11(ctx context.Context, staleBefore time.Time, limit int) ([]relayregistry.RelayRecord, error)
	UpdateCapabilitySummary(ctx context.Context, urlKey string, summary json.RawMessage) error
}

// Scheduler refreshes stored NIP-11 documents. A failed fetch is recorded on
// the registry row and does not change probe status.
type Scheduler struct {
	log     *slog.Logger
	store   capabilityStore
	fetcher documentFetcher
	cfg     config.RelayRegistryNIP11Config
}

func NewScheduler(log *slog.Logger, store *relayregistry.Store, cfg config.RelayRegistryNIP11Config) *Scheduler {
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{
		log:     log,
		store:   store,
		fetcher: NewFetcher(cfg.Timeout),
		cfg:     cfg,
	}
}

// RunLoop fetches stale documents until ctx is cancelled.
func (s *Scheduler) RunLoop(ctx context.Context) {
	if !s.cfg.Enabled {
		s.log.Info("relay_nip11_disabled")
		return
	}
	if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
		s.log.Error("relay_nip11_initial_failed", "error", err)
	}
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.RunOnce(ctx); err != nil && ctx.Err() == nil {
				s.log.Error("relay_nip11_cycle_failed", "error", err)
			}
		}
	}
}

// RunOnce fetches one batch of relays whose stored document is missing or
// older than the freshness window.
func (s *Scheduler) RunOnce(ctx context.Context) error {
	if s.fetcher == nil {
		s.fetcher = NewFetcher(s.cfg.Timeout)
	}
	limit := s.cfg.BatchSize
	if limit <= 0 {
		limit = 25
	}
	staleBefore := time.Now().UTC().Add(-s.cfg.Freshness)
	relays, err := s.store.ListRelaysForNIP11(ctx, staleBefore, limit)
	if err != nil {
		return err
	}
	if len(relays) == 0 {
		return nil
	}

	parallel := s.cfg.MaxParallel
	if parallel <= 0 {
		parallel = 1
	}
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for _, relay := range relays {
		select {
		case <-ctx.Done():
			wg.Wait()
			return nil
		case sem <- struct{}{}:
		}
		wg.Add(1)
		go func(rec relayregistry.RelayRecord) {
			defer wg.Done()
			defer func() { <-sem }()
			s.fetchOne(ctx, rec)
		}(relay)
	}
	wg.Wait()
	s.log.Info("relay_nip11_cycle_completed", "considered", len(relays))
	return nil
}

func (s *Scheduler) fetchOne(ctx context.Context, rec relayregistry.RelayRecord) {
	info, err := s.fetcher.Fetch(ctx, rec.NormalizedURL)
	if ctx.Err() != nil {
		return
	}
	summary := StoredSummary{FetchedAt: time.Now().UTC()}
	if err != nil {
		code := fetchCode(err)
		summary.Error = code
		metrics.IncRelayNIP11Fetch(metricOutcome(code))
	} else {
		summary.OK = true
		summary.Info = &info
		metrics.IncRelayNIP11Fetch("ok")
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		s.log.Warn("relay_nip11_encode_failed", "relay", rec.NormalizedURL, "error", err)
		return
	}
	if err := s.store.UpdateCapabilitySummary(ctx, rec.URLKey, raw); err != nil {
		s.log.Warn("relay_nip11_persist_failed", "relay", rec.NormalizedURL, "error", err)
	}
}

func fetchCode(err error) string {
	var coded *FetchError
	if errors.As(err, &coded) && coded.Code != "" {
		return coded.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "connect_failed"
}

func metricOutcome(code string) string {
	switch code {
	case "ok", "timeout", "connect_failed", "private_address", "redirect_rejected", "too_large", "not_json":
		return code
	}
	if strings.HasPrefix(code, "http_") {
		return "http_error"
	}
	return "other"
}
