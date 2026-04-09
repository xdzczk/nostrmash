package api

import (
	"testing"
	"time"
)

func TestBuildFreshnessSignal_ComputesLagAndFreshness(t *testing.T) {
	now := time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)
	updatedAt := now.Add(-2 * time.Minute)
	rowCount := int64(42)

	signal := buildFreshnessSignal("profiles_latest", &updatedAt, 5*time.Minute, now, &rowCount)
	if signal.Status != "fresh" {
		t.Fatalf("unexpected status: got %s want fresh", signal.Status)
	}
	if signal.Stale {
		t.Fatal("expected fresh signal to be non-stale")
	}
	if signal.LagSeconds == nil || *signal.LagSeconds != 120 {
		t.Fatalf("unexpected lag: %+v", signal.LagSeconds)
	}
	if signal.RowCount == nil || *signal.RowCount != 42 {
		t.Fatalf("unexpected row count: %+v", signal.RowCount)
	}
}

func TestBuildFreshnessSignal_DetectsStaleAndMissing(t *testing.T) {
	now := time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)
	updatedAt := now.Add(-25 * time.Minute)

	stale := buildFreshnessSignal("note_discovery_stats", &updatedAt, 10*time.Minute, now, nil)
	if stale.Status != "stale" || !stale.Stale {
		t.Fatalf("expected stale signal, got %+v", stale)
	}

	missing := buildFreshnessSignal("trust_graph_snapshot", nil, 10*time.Minute, now, nil)
	if missing.Status != "missing" || !missing.Stale {
		t.Fatalf("expected missing signal to be stale, got %+v", missing)
	}
	if missing.LagSeconds != nil {
		t.Fatalf("missing lag should be nil, got %+v", missing.LagSeconds)
	}
}

func TestStaleSignalNames_ReturnsSortedStaleSubsystems(t *testing.T) {
	got := staleSignalNames([]adminFreshnessSignal{
		{Name: "profiles_latest", Stale: false},
		{Name: "note_discovery_stats", Stale: true},
		{Name: "ingest", Stale: true},
	})
	if len(got) != 2 || got[0] != "ingest" || got[1] != "note_discovery_stats" {
		t.Fatalf("unexpected stale list: %+v", got)
	}
}
