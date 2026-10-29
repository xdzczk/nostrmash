package relayprobe

import (
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestCatalogProbeBudget_IsExtra(t *testing.T) {
	// MaxParallel 5 asks ListRelaysForProbing for 10 rows: 5 live and 5
	// discovery. The catalog budget must not change that split.
	baseLimit := 5 * 2
	live := (baseLimit + 1) / 2
	discovery := baseLimit - live
	if live != 5 || discovery != 5 {
		t.Fatalf("live/discovery split = %d/%d, want 5/5", live, discovery)
	}

	if got := CatalogProbeBudget(80, 5*time.Minute, time.Hour); got != 7 {
		t.Fatalf("catalog dials = %d, want 7", got)
	}
	if got := CatalogProbeBudget(1, 5*time.Minute, time.Hour); got != 1 {
		t.Fatalf("single-head dials = %d, want 1", got)
	}
	if got := CatalogProbeBudget(5, time.Hour, 30*time.Minute); got != 5 {
		t.Fatalf("window shorter than interval = %d, want the whole head", got)
	}
	if got := CatalogProbeBudget(0, 5*time.Minute, time.Hour); got != 0 {
		t.Fatalf("zero head = %d, want 0", got)
	}
	if got := CatalogProbeBudget(80, 0, time.Hour); got != 0 {
		t.Fatalf("zero interval = %d, want 0", got)
	}
	if got := CatalogProbeBudget(80, 5*time.Minute, 0); got != 0 {
		t.Fatalf("zero freshness = %d, want 0", got)
	}
}

func TestMergeProbeTargets_KeepsBaseAndAppendsNew(t *testing.T) {
	base := []relayregistry.RelayRecord{
		{URLKey: "live-stale"},
		{URLKey: "cand-never"},
	}
	extra := []relayregistry.RelayRecord{
		{URLKey: "live-stale"},
		{URLKey: "catalog-only"},
	}

	got := mergeProbeTargets(base, extra)
	if len(got) != 3 {
		t.Fatalf("merged length = %d, want 3 (base preserved, one added)", len(got))
	}
	if got[0].URLKey != "live-stale" || got[1].URLKey != "cand-never" || got[2].URLKey != "catalog-only" {
		t.Fatalf("merged order = [%s %s %s]", got[0].URLKey, got[1].URLKey, got[2].URLKey)
	}
	if len(base) != 2 {
		t.Fatalf("base length changed to %d", len(base))
	}

	onlyCatalog := mergeProbeTargets(nil, []relayregistry.RelayRecord{{URLKey: "catalog-only"}})
	if len(onlyCatalog) != 1 || onlyCatalog[0].URLKey != "catalog-only" {
		t.Fatalf("empty base should still probe catalog targets, got %#v", onlyCatalog)
	}
	if got := mergeProbeTargets(base, nil); len(got) != len(base) {
		t.Fatalf("empty extra changed base length to %d", len(got))
	}
}
