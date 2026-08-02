package relayadmission

import (
	"math"
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
}

func TestComputeScore_ThousandsOutrankSubThousand(t *testing.T) {
	small := ComputeScore(relayregistry.RelayRecord{DistinctUserRefCount: 500})
	borderline := ComputeScore(relayregistry.RelayRecord{DistinctUserRefCount: 999})
	thousand := ComputeScore(relayregistry.RelayRecord{DistinctUserRefCount: 1000})
	multiThousand := ComputeScore(relayregistry.RelayRecord{DistinctUserRefCount: 5000})
	huge := ComputeScore(relayregistry.RelayRecord{DistinctUserRefCount: 10000})

	if thousand.PopularityScore <= borderline.PopularityScore {
		t.Fatalf("1000-user relay should outrank 999-user relay: got 1000=%f 999=%f",
			thousand.PopularityScore, borderline.PopularityScore)
	}
	if multiThousand.PopularityScore <= small.PopularityScore {
		t.Fatalf("5000-user relay should outrank 500-user relay: got 5000=%f 500=%f",
			multiThousand.PopularityScore, small.PopularityScore)
	}
	if huge.PopularityScore <= multiThousand.PopularityScore {
		t.Fatalf("10000-user relay should outrank 5000-user relay: got 10000=%f 5000=%f",
			huge.PopularityScore, multiThousand.PopularityScore)
	}
	// Crossing the large-relay threshold should be a decisive jump, not noise.
	if thousand.PopularityScore-borderline.PopularityScore < 15 {
		t.Fatalf("expected >=15 point jump at 1000-user threshold, got delta %f",
			thousand.PopularityScore-borderline.PopularityScore)
	}
}

func TestComputeScore_LargeRelayBeatsHealthyNicheOnTotal(t *testing.T) {
	ok := relayregistry.ProbeStatusOK
	lat := 100.0
	niche := ComputeScore(relayregistry.RelayRecord{
		DistinctUserRefCount: 80,
		LastProbeStatus:      &ok,
		AvgConnectLatency:    &lat,
		YieldScore:           1,
	})
	large := ComputeScore(relayregistry.RelayRecord{
		DistinctUserRefCount: 3000,
	})
	if large.TotalScore <= niche.TotalScore {
		t.Fatalf("large relay without probes should still beat healthy niche: large=%f niche=%f",
			large.TotalScore, niche.TotalScore)
	}
}

func TestPopularityScore_SoftSafetyCap(t *testing.T) {
	got := popularityScore(math.MaxInt32)
	if got != maxPopularityScore {
		t.Fatalf("expected safety cap %d, got %f", maxPopularityScore, got)
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
