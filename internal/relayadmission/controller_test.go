package relayadmission

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

func newTestCfg() config.RelayRegistryAdmissionConfig {
	return config.RelayRegistryAdmissionConfig{
		MaxTotalActive:         15,
		MaxDynamicActive:       10,
		MaxProbation:           20,
		MinScoreForProbation:   10,
		MinScoreForActive:      30,
		DemoteFailureThreshold: 0.6,
	}
}

func TestTransition_BlockedPolicyAlwaysBlocked(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		ManualPolicy:   relayregistry.ManualPolicyBlocked,
		AdmissionState: relayregistry.AdmissionActive,
	}
	sc := ScoreComponents{TotalScore: 100}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionBlocked {
		t.Fatalf("blocked policy should always yield blocked state, got %s", got)
	}
}

func TestTransition_PinnedPolicyAlwaysPinned(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		ManualPolicy:   relayregistry.ManualPolicyPinned,
		AdmissionState: relayregistry.AdmissionCandidate,
	}
	sc := ScoreComponents{TotalScore: 0}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionPinned {
		t.Fatalf("pinned policy should always yield pinned state, got %s", got)
	}
}

func TestTransition_DrainedPolicyAlwaysDraining(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		ManualPolicy:   relayregistry.ManualPolicyDrained,
		AdmissionState: relayregistry.AdmissionActive,
	}
	sc := ScoreComponents{TotalScore: 100}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionDraining {
		t.Fatalf("drained policy should yield draining state, got %s", got)
	}
}

func TestTransition_CandidateToPromotion(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		AdmissionState: relayregistry.AdmissionCandidate,
		ManualPolicy:   relayregistry.ManualPolicyNone,
	}
	sc := ScoreComponents{TotalScore: 15}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionProbation {
		t.Fatalf("candidate with score >= min probation should promote, got %s", got)
	}
}

func TestTransition_CandidateStaysBelowThreshold(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		AdmissionState: relayregistry.AdmissionCandidate,
		ManualPolicy:   relayregistry.ManualPolicyNone,
	}
	sc := ScoreComponents{TotalScore: 5}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionCandidate {
		t.Fatalf("candidate below threshold should stay candidate, got %s", got)
	}
}

func TestTransition_ProbationToActive(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		AdmissionState: relayregistry.AdmissionProbation,
		ManualPolicy:   relayregistry.ManualPolicyNone,
	}
	sc := ScoreComponents{TotalScore: 35}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionActive {
		t.Fatalf("probation with sufficient score should promote to active, got %s", got)
	}
}

func TestTransition_ActiveDemotedOnHighFailure(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		AdmissionState: relayregistry.AdmissionActive,
		ManualPolicy:   relayregistry.ManualPolicyNone,
		ProbeFailRate:  0.8,
	}
	sc := ScoreComponents{TotalScore: 35}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionProbation {
		t.Fatalf("active relay with high failure rate should be demoted, got %s", got)
	}
}

func TestTransition_ActiveStaysWhenHealthy(t *testing.T) {
	c := NewController(nil, nil, newTestCfg())
	rec := relayregistry.RelayRecord{
		AdmissionState: relayregistry.AdmissionActive,
		ManualPolicy:   relayregistry.ManualPolicyNone,
		ProbeFailRate:  0.1,
	}
	sc := ScoreComponents{TotalScore: 50}
	got := c.computeTransition(rec, sc)
	if got != relayregistry.AdmissionActive {
		t.Fatalf("healthy active relay should remain active, got %s", got)
	}
}

func TestIsPromotion(t *testing.T) {
	if !isPromotion(relayregistry.AdmissionCandidate, relayregistry.AdmissionProbation) {
		t.Fatal("candidate -> probation should be promotion")
	}
	if !isPromotion(relayregistry.AdmissionProbation, relayregistry.AdmissionActive) {
		t.Fatal("probation -> active should be promotion")
	}
	if isPromotion(relayregistry.AdmissionActive, relayregistry.AdmissionProbation) {
		t.Fatal("active -> probation should not be promotion")
	}
}
