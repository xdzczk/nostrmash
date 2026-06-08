// Package hydration implements on-demand account hydration: a bounded fetch of
// an account's profile, contacts, recent notes, engagement, and thread parents
// from relays, persisted through the normal canonical ingest path. It is the
// "fill in what we don't have, when asked" half of the temporary-observation /
// durable-intelligence split.
package hydration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

// EventPersister persists a raw relay event payload through validate / dedupe /
// canonical insert. Satisfied by *ingestor/live.Processor constructed without a
// trust gate, so the explicitly-requested author is accepted by construction.
type EventPersister interface {
	Handle(ctx context.Context, relayURL string, payload []byte) error
}

// AccountStore is the account-state surface hydration needs.
type AccountStore interface {
	PromoteAccountToTracked(ctx context.Context, pubkey string, reason string) (string, error)
	GetAccountState(ctx context.Context, pubkey string) (store.AccountStateRow, error)
	GetStoragePressureState(ctx context.Context) (store.StoragePressureState, error)
	UpdateAccountCoverage(
		ctx context.Context,
		pubkey string,
		lastHydratedAt *time.Time,
		lastSuccessfulHydrationAt *time.Time,
		oldestKnownNoteAt *time.Time,
		newestKnownNoteAt *time.Time,
		engagementLastCheckedAt *time.Time,
		coverageWindowDays *int,
	) error
}

// RelaySource provides the relay set to query.
type RelaySource interface {
	Relays(ctx context.Context) ([]string, error)
}

// Result summarizes a hydration run.
type Result struct {
	Status      string // success | partial | skipped | failed
	EventsFound int
	Promoted    bool
}

// Service runs bounded account hydration.
type Service struct {
	log       *slog.Logger
	cfg       config.HydrationConfig
	store     AccountStore
	persister EventPersister
	fetcher   Fetcher
	relays    RelaySource
	sem       chan struct{}
}

// NewService constructs a hydration Service. concurrency is capped by
// cfg.MaxConcurrency.
func NewService(log *slog.Logger, cfg config.HydrationConfig, accountStore AccountStore, persister EventPersister, fetcher Fetcher, relays RelaySource) *Service {
	if log == nil {
		log = slog.Default()
	}
	concurrency := cfg.MaxConcurrency
	if concurrency <= 0 {
		concurrency = 1
	}
	return &Service{
		log:       log,
		cfg:       cfg,
		store:     accountStore,
		persister: persister,
		fetcher:   fetcher,
		relays:    relays,
		sem:       make(chan struct{}, concurrency),
	}
}

type minimalEvent struct {
	ID        string     `json:"id"`
	Kind      int        `json:"kind"`
	CreatedAt int64      `json:"created_at"`
	Tags      [][]string `json:"tags"`
}

// Hydrate performs one bounded hydration run for pubkey.
func (s *Service) Hydrate(ctx context.Context, pubkey string, reason string) (Result, error) {
	started := time.Now()
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if !isHexPubkey(pubkey) {
		return Result{Status: "failed"}, fmt.Errorf("invalid pubkey")
	}
	if !s.cfg.Enabled {
		metrics.ObserveHydrationRun("skipped", 0, 0)
		return Result{Status: "skipped"}, nil
	}

	// Respect storage pressure: refuse new runs when the governor has reached
	// the disable-hydration level.
	if st, err := s.store.GetStoragePressureState(ctx); err == nil {
		if st.Level >= int(config.PressureDisableHydration) {
			s.log.Info("hydration_skipped_storage_pressure", "pubkey", pubkey, "level", st.Level)
			metrics.ObserveHydrationRun("skipped", 0, 0)
			return Result{Status: "skipped"}, nil
		}
	}

	// Cooldown: skip if hydrated within the cooldown window.
	if row, err := s.store.GetAccountState(ctx, pubkey); err == nil {
		if row.LastHydratedAt != nil && s.cfg.Cooldown > 0 && time.Since(*row.LastHydratedAt) < s.cfg.Cooldown {
			metrics.ObserveHydrationRun("skipped", 0, 0)
			return Result{Status: "skipped"}, nil
		}
	}

	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	case <-ctx.Done():
		return Result{Status: "failed"}, ctx.Err()
	}

	if _, err := s.store.PromoteAccountToTracked(ctx, pubkey, reason); err != nil {
		metrics.ObserveHydrationRun("failed", time.Since(started).Seconds(), 0)
		return Result{Status: "failed"}, fmt.Errorf("promote to tracked: %w", err)
	}

	runCtx := ctx
	if s.cfg.MaxRuntime > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, s.cfg.MaxRuntime)
		defer cancel()
	}

	relays, err := s.relays.Relays(runCtx)
	if err != nil || len(relays) == 0 {
		metrics.ObserveHydrationRun("failed", time.Since(started).Seconds(), 0)
		return Result{Status: "failed", Promoted: true}, fmt.Errorf("resolve relays: %w", err)
	}
	if s.cfg.MaxRelays > 0 && len(relays) > s.cfg.MaxRelays {
		relays = relays[:s.cfg.MaxRelays]
	}

	now := time.Now().UTC()
	var since *int64
	if s.cfg.MaxLookbackDays > 0 {
		v := now.AddDate(0, 0, -s.cfg.MaxLookbackDays).Unix()
		since = &v
	}

	run := &hydrationRun{
		service:   s,
		relays:    relays,
		maxEvents: s.cfg.MaxEventsPerAccount,
	}

	// 1. Profile / contacts / relay list.
	run.fetchAndPersist(runCtx, FetchFilter{Kinds: []int{0, 3, 10002}, Authors: []string{pubkey}, Limit: 10})

	// 2. Recent authored notes.
	noteIDs := run.fetchAndPersist(runCtx, FetchFilter{Kinds: []int{1}, Authors: []string{pubkey}, Since: since, Limit: run.remaining()})

	// 3. Engagement referencing the fetched notes (bounded #e set).
	if len(noteIDs) > 0 && run.remaining() > 0 {
		run.fetchAndPersist(runCtx, FetchFilter{Kinds: []int{6, 7, 9735}, ETags: capStrings(noteIDs, 100), Limit: run.remaining()})
	}

	// 4. Thread-parent events referenced by the fetched notes.
	if len(run.parentIDs) > 0 && run.remaining() > 0 {
		run.fetchAndPersist(runCtx, FetchFilter{IDs: capStrings(run.parentIDs, 100), Limit: run.remaining()})
	}

	coverageDays := s.cfg.MaxLookbackDays
	status := "success"
	if run.eventsFound == 0 {
		status = "partial"
	}
	var lastSuccessful *time.Time
	if run.eventsFound > 0 {
		lastSuccessful = &now
	}
	if err := s.store.UpdateAccountCoverage(
		ctx,
		pubkey,
		&now,
		lastSuccessful,
		run.oldestNoteAt(),
		run.newestNoteAt(),
		&now,
		&coverageDays,
	); err != nil {
		s.log.Error("hydration_coverage_update_failed", "pubkey", pubkey, "error", err)
	}

	metrics.ObserveHydrationRun(status, time.Since(started).Seconds(), run.eventsFound)
	s.log.Info(
		"hydration_complete",
		"pubkey", pubkey,
		"status", status,
		"events_found", run.eventsFound,
		"relays", len(relays),
		"duration_s", time.Since(started).Seconds(),
	)
	return Result{Status: status, EventsFound: run.eventsFound, Promoted: true}, nil
}

// hydrationRun accumulates per-run state across the filter passes.
type hydrationRun struct {
	service     *Service
	relays      []string
	maxEvents   int
	eventsFound int
	parentIDs   []string

	oldestNote int64
	newestNote int64
}

func (r *hydrationRun) remaining() int {
	if r.maxEvents <= 0 {
		return 1000
	}
	rem := r.maxEvents - r.eventsFound
	if rem < 0 {
		return 0
	}
	return rem
}

// fetchAndPersist queries every relay for the filter, persists each event, and
// returns the distinct ids of any kind-1 notes seen (for engagement fetching).
func (r *hydrationRun) fetchAndPersist(ctx context.Context, filter FetchFilter) []string {
	seenNotes := make([]string, 0)
	for _, relay := range r.relays {
		if ctx.Err() != nil || r.remaining() <= 0 {
			break
		}
		f := filter
		if f.Limit > r.remaining() {
			f.Limit = r.remaining()
		}
		events, err := r.service.fetcher.Fetch(ctx, relay, f)
		if err != nil {
			r.service.log.Debug("hydration_fetch_failed", "relay", relay, "error", err)
			continue
		}
		for _, raw := range events {
			if r.remaining() <= 0 {
				break
			}
			if err := r.service.persister.Handle(ctx, relay, raw); err != nil {
				r.service.log.Debug("hydration_persist_failed", "relay", relay, "error", err)
				continue
			}
			r.eventsFound++
			r.inspect(raw, &seenNotes)
		}
	}
	return seenNotes
}

func (r *hydrationRun) inspect(raw []byte, seenNotes *[]string) {
	var ev minimalEvent
	if err := json.Unmarshal(raw, &ev); err != nil {
		return
	}
	if ev.Kind == 1 {
		if ev.ID != "" {
			*seenNotes = append(*seenNotes, ev.ID)
		}
		if ev.CreatedAt > 0 {
			if r.oldestNote == 0 || ev.CreatedAt < r.oldestNote {
				r.oldestNote = ev.CreatedAt
			}
			if ev.CreatedAt > r.newestNote {
				r.newestNote = ev.CreatedAt
			}
		}
		for _, tag := range ev.Tags {
			if len(tag) >= 2 && tag[0] == "e" && tag[1] != "" {
				r.parentIDs = append(r.parentIDs, tag[1])
			}
		}
	}
}

func (r *hydrationRun) oldestNoteAt() *time.Time {
	if r.oldestNote == 0 {
		return nil
	}
	t := time.Unix(r.oldestNote, 0).UTC()
	return &t
}

func (r *hydrationRun) newestNoteAt() *time.Time {
	if r.newestNote == 0 {
		return nil
	}
	t := time.Unix(r.newestNote, 0).UTC()
	return &t
}

func capStrings(in []string, max int) []string {
	if max <= 0 || len(in) <= max {
		return in
	}
	return in[:max]
}

func isHexPubkey(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
