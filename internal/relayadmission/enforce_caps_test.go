package relayadmission

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
