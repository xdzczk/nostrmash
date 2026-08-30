package relayadmission

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"testing"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

type fakeRegistryStore struct {
	byKey map[string]relayregistry.RelayRecord
}

func newFakeRegistryStore(records ...relayregistry.RelayRecord) *fakeRegistryStore {
	s := &fakeRegistryStore{byKey: make(map[string]relayregistry.RelayRecord, len(records))}
	for _, rec := range records {
		s.byKey[rec.URLKey] = rec
	}
	return s
}

// ListRelays mirrors the real store's contract: filtered by admission state,
// ordered by score DESC (then URL for determinism), and truncated at
// filter.Limit. Honoring Limit here matters — the production cap-enforcement
// bug (active relays escaping demotion) was caused by exactly this
// truncation, and a fake that returns everything can never catch it.
func (s *fakeRegistryStore) ListRelays(_ context.Context, filter relayregistry.ListFilter) ([]relayregistry.RelayRecord, error) {
	allowed := make(map[relayregistry.AdmissionState]bool, len(filter.AdmissionStates))
	for _, st := range filter.AdmissionStates {
		allowed[st] = true
	}
	out := make([]relayregistry.RelayRecord, 0, len(s.byKey))
	for _, rec := range s.byKey {
		if len(allowed) > 0 && !allowed[rec.AdmissionState] {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].NormalizedURL < out[j].NormalizedURL
	})
	limit := filter.Limit
	if limit <= 0 {
		limit = 500
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *fakeRegistryStore) SetAdmissionState(
	_ context.Context,
	urlKey string,
	state relayregistry.AdmissionState,
	score float64,
	_ json.RawMessage,
) error {
	rec, ok := s.byKey[urlKey]
	if !ok {
		return fmt.Errorf("missing relay %q", urlKey)
	}
	rec.AdmissionState = state
	rec.Score = score
	s.byKey[urlKey] = rec
	return nil
}

func (s *fakeRegistryStore) countState(state relayregistry.AdmissionState) int {
	var n int
	for _, rec := range s.byKey {
		if rec.AdmissionState == state {
			n++
		}
	}
	return n
}

func TestEnforceCaps_ActiveSpillCountsTowardProbationCap(t *testing.T) {
	// Mirrors prod drift: many high-score actives get demoted into probation for
	// MaxTotalActive, while a smaller pre-existing probation set is all that the
	// old code trimmed. After the fix, final probation must be <= MaxProbation.
	records := []relayregistry.RelayRecord{
		{
			URLKey:         "pin",
			NormalizedURL:  "wss://pin",
			AdmissionState: relayregistry.AdmissionPinned,
			ManualPolicy:   relayregistry.ManualPolicyPinned,
			Score:          70,
		},
	}
	for i := 0; i < 15; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:         fmt.Sprintf("active-%02d", i),
			NormalizedURL:  fmt.Sprintf("wss://active-%02d", i),
			AdmissionState: relayregistry.AdmissionActive,
			Score:          70,
		})
	}
	for i := 0; i < 30; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:         fmt.Sprintf("prob-%02d", i),
			NormalizedURL:  fmt.Sprintf("wss://prob-%02d", i),
			AdmissionState: relayregistry.AdmissionProbation,
			Score:          10,
		})
	}

	store := newFakeRegistryStore(records...)
	cfg := newTestCfg() // MaxTotalActive=15, MaxProbation=20, 1 pinned + 15 active => 1 spill
	c := NewController(slog.Default(), store, cfg)

	c.enforceCaps(context.Background())

	if got := store.countState(relayregistry.AdmissionProbation); got > cfg.MaxProbation {
		t.Fatalf("probation count %d exceeds MaxProbation %d", got, cfg.MaxProbation)
	}
	if got := store.countState(relayregistry.AdmissionActive) + store.countState(relayregistry.AdmissionPinned); got > cfg.MaxTotalActive {
		t.Fatalf("active+pinned count %d exceeds MaxTotalActive %d", got, cfg.MaxTotalActive)
	}
}

func TestEnforceCaps_LargeOverflowIsNotTruncated(t *testing.T) {
	// Regression for the production incident where 337 relays were active
	// against MaxTotalActive=20: enforceCaps listed active+pinned+probation
	// with Limit 500, the mid-cycle set exceeded 500 rows, and the listing's
	// score-DESC truncation silently dropped the lowest-scored actives — the
	// exact rows that needed demoting. Build a set well past the old limit
	// where every active outscores every probation row, so under truncation
	// the probation tail (not the actives) would be cut and the probation
	// cap would leak instead; either leak fails the assertions below.
	var records []relayregistry.RelayRecord
	for i := 0; i < 340; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:         fmt.Sprintf("active-%03d", i),
			NormalizedURL:  fmt.Sprintf("wss://active-%03d", i),
			AdmissionState: relayregistry.AdmissionActive,
			Score:          float64(1000 + i),
		})
	}
	for i := 0; i < 480; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:         fmt.Sprintf("prob-%03d", i),
			NormalizedURL:  fmt.Sprintf("wss://prob-%03d", i),
			AdmissionState: relayregistry.AdmissionProbation,
			Score:          float64(i),
		})
	}

	store := newFakeRegistryStore(records...)
	cfg := newTestCfg()
	c := NewController(slog.Default(), store, cfg)

	c.enforceCaps(context.Background())

	if got := store.countState(relayregistry.AdmissionActive); got > cfg.MaxTotalActive {
		t.Fatalf("active count %d exceeds MaxTotalActive %d — cap listing truncated", got, cfg.MaxTotalActive)
	}
	if got := store.countState(relayregistry.AdmissionProbation); got > cfg.MaxProbation {
		t.Fatalf("probation count %d exceeds MaxProbation %d — cap listing truncated", got, cfg.MaxProbation)
	}
}

func TestRun_PromotionsAreCapacityGated(t *testing.T) {
	// Regression for the promote/demote washing machine: with probation and
	// active tiers already full, a cycle used to promote every eligible
	// candidate/inactive/probation relay anyway and rely on enforceCaps to
	// demote them right back (~20k state flips per day in production). With
	// capacity gating, relays that cannot fit simply keep their state.
	okStatus := relayregistry.ProbeStatusOK
	cfg := newTestCfg()

	var records []relayregistry.RelayRecord
	// Fill the active tier with clearly active-worthy relays
	// (probe OK 20 + popularity(1000 refs) ≈ 86 → score ≥ 30).
	for i := 0; i < cfg.MaxDynamicActive; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:               fmt.Sprintf("active-%02d", i),
			NormalizedURL:        fmt.Sprintf("wss://active-%02d", i),
			AdmissionState:       relayregistry.AdmissionActive,
			LastProbeStatus:      &okStatus,
			DistinctUserRefCount: 1000,
			Score:                100,
		})
	}
	// Fill the probation tier with active-eligible relays (score ≥ 30) that
	// cannot be promoted because the active tier is full.
	for i := 0; i < cfg.MaxProbation; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:               fmt.Sprintf("prob-%02d", i),
			NormalizedURL:        fmt.Sprintf("wss://prob-%02d", i),
			AdmissionState:       relayregistry.AdmissionProbation,
			LastProbeStatus:      &okStatus,
			DistinctUserRefCount: 100,
			Score:                50,
		})
	}
	// Probation-eligible inactive relays (probe OK → score 20) that cannot
	// be promoted because the probation tier is full.
	for i := 0; i < 50; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:          fmt.Sprintf("inactive-%02d", i),
			NormalizedURL:   fmt.Sprintf("wss://inactive-%02d", i),
			AdmissionState:  relayregistry.AdmissionInactive,
			LastProbeStatus: &okStatus,
			Score:           20,
		})
	}

	store := newFakeRegistryStore(records...)
	c := NewController(slog.Default(), store, cfg)

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run admission cycle: %v", err)
	}

	if got := store.countState(relayregistry.AdmissionActive); got != cfg.MaxDynamicActive {
		t.Fatalf("active count changed to %d, want %d (no promotions into a full tier)", got, cfg.MaxDynamicActive)
	}
	if got := store.countState(relayregistry.AdmissionProbation); got != cfg.MaxProbation {
		t.Fatalf("probation count changed to %d, want %d (no promotions into a full tier)", got, cfg.MaxProbation)
	}
	if got := store.countState(relayregistry.AdmissionInactive); got != 50 {
		t.Fatalf("inactive count changed to %d, want 50 (gated relays keep their state)", got)
	}
}

func TestRun_PromotionsFillOnlyAvailableCapacity(t *testing.T) {
	// With one open slot per tier, exactly one probation relay must be
	// promoted to active and (because that promotion frees a probation
	// slot) inactive relays may claim the freed capacity — the flood of
	// remaining contenders stays put.
	okStatus := relayregistry.ProbeStatusOK
	cfg := newTestCfg()

	var records []relayregistry.RelayRecord
	for i := 0; i < cfg.MaxDynamicActive-1; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:               fmt.Sprintf("active-%02d", i),
			NormalizedURL:        fmt.Sprintf("wss://active-%02d", i),
			AdmissionState:       relayregistry.AdmissionActive,
			LastProbeStatus:      &okStatus,
			DistinctUserRefCount: 1000,
			Score:                100,
		})
	}
	for i := 0; i < 5; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:               fmt.Sprintf("prob-%02d", i),
			NormalizedURL:        fmt.Sprintf("wss://prob-%02d", i),
			AdmissionState:       relayregistry.AdmissionProbation,
			LastProbeStatus:      &okStatus,
			DistinctUserRefCount: 100 + i,
			Score:                float64(50 + i),
		})
	}

	store := newFakeRegistryStore(records...)
	c := NewController(slog.Default(), store, cfg)

	if err := c.Run(context.Background()); err != nil {
		t.Fatalf("run admission cycle: %v", err)
	}

	if got := store.countState(relayregistry.AdmissionActive); got != cfg.MaxDynamicActive {
		t.Fatalf("active count %d, want %d (exactly one promotion into the open slot)", got, cfg.MaxDynamicActive)
	}
	// The highest-ref probation relay (prob-04) wins the slot because the
	// cycle iterates in score order.
	if got := store.byKey["prob-04"].AdmissionState; got != relayregistry.AdmissionActive {
		t.Fatalf("highest-scored contender state = %s, want active", got)
	}
}

func TestEnforceCaps_SourceSeedActiveIsNotPinProtected(t *testing.T) {
	// Seeds are bootstrap entries, not permanent pins: an active source_seed
	// relay must count toward the dynamic pool and be demotable under caps.
	records := []relayregistry.RelayRecord{
		{
			URLKey:         "seed-low",
			NormalizedURL:  "wss://seed-low",
			AdmissionState: relayregistry.AdmissionActive,
			SourceSeed:     true,
			Score:          1,
		},
	}
	for i := 0; i < 15; i++ {
		records = append(records, relayregistry.RelayRecord{
			URLKey:         fmt.Sprintf("active-%02d", i),
			NormalizedURL:  fmt.Sprintf("wss://active-%02d", i),
			AdmissionState: relayregistry.AdmissionActive,
			Score:          70,
		})
	}

	store := newFakeRegistryStore(records...)
	cfg := newTestCfg() // MaxTotalActive=15
	c := NewController(slog.Default(), store, cfg)

	c.enforceCaps(context.Background())

	seed := store.byKey["seed-low"]
	if seed.AdmissionState != relayregistry.AdmissionProbation {
		t.Fatalf("low-score source_seed active should be demoted under total cap, got %s", seed.AdmissionState)
	}
	if got := store.countState(relayregistry.AdmissionActive) + store.countState(relayregistry.AdmissionPinned); got > cfg.MaxTotalActive {
		t.Fatalf("active+pinned count %d exceeds MaxTotalActive %d", got, cfg.MaxTotalActive)
	}
}
