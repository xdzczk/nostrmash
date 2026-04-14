package relayadmission

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func TestComputeScore_EmptyRecord(t *testing.T) {
	rec := relayregistry.RelayRecord{}
	sc := ComputeScore(rec)
	if sc.TotalScore != 0 {
		t.Fatalf("expected 0 score for empty record, got %f", sc.TotalScore)
	}
}

func TestComputeScore_PopularRelay(t *testing.T) {
	rec := relayregistry.RelayRecord{
		DistinctUserRefCount: 20,
	}
	sc := ComputeScore(rec)
	if sc.PopularityScore <= 0 {
		t.Fatal("expected positive popularity score for popular relay")
	}
	if sc.PopularityScore > 40 {
		t.Fatalf("popularity should be capped at 40, got %f", sc.PopularityScore)
	}
}

func TestComputeScore_HealthyProbe(t *testing.T) {
	ok := relayregistry.ProbeStatusOK
	rec := relayregistry.RelayRecord{
		LastProbeStatus: &ok,
	}
	sc := ComputeScore(rec)
	if sc.ProbeHealthScore != 20 {
		t.Fatalf("expected 20 probe health for OK status, got %f", sc.ProbeHealthScore)
	}
}

func TestComputeScore_HighFailureRate(t *testing.T) {
	rec := relayregistry.RelayRecord{
		ProbeFailRate:        0.8,
		DistinctUserRefCount: 10,
	}
	sc := ComputeScore(rec)
	if sc.FailurePenalty >= 0 {
		t.Fatal("expected negative failure penalty for high fail rate")
	}
}

func TestComputeScore_HighDuplicateRatio(t *testing.T) {
	rec := relayregistry.RelayRecord{
		DuplicateRatio:       0.9,
		DistinctUserRefCount: 10,
	}
	sc := ComputeScore(rec)
	if sc.DuplicatePenalty >= 0 {
		t.Fatal("expected negative duplicate penalty for high duplicate ratio")
	}
}

func TestComputeScore_LowLatency(t *testing.T) {
	lat := 100.0
	rec := relayregistry.RelayRecord{
		AvgConnectLatency: &lat,
	}
	sc := ComputeScore(rec)
	if sc.LatencyScore != 10 {
		t.Fatalf("expected 10 latency score for fast relay, got %f", sc.LatencyScore)
	}
}

func TestComputeScore_NeverNegativeTotal(t *testing.T) {
	rec := relayregistry.RelayRecord{
		ProbeFailRate:  1.0,
		DuplicateRatio: 1.0,
	}
	sc := ComputeScore(rec)
	if sc.TotalScore < 0 {
		t.Fatalf("total score should never be negative, got %f", sc.TotalScore)
	}
}
