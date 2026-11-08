package relayregistry

import "testing"

func TestCoverageChangeKind(t *testing.T) {
	kind, ok := CoverageChangeKind(AdmissionCandidate, AdmissionActive)
	if !ok || kind != "added" {
		t.Fatalf("candidate to active = %q %v", kind, ok)
	}
	kind, ok = CoverageChangeKind(AdmissionActive, AdmissionInactive)
	if !ok || kind != "dropped" {
		t.Fatalf("active to inactive = %q %v", kind, ok)
	}
	if _, ok := CoverageChangeKind(AdmissionActive, AdmissionPinned); ok {
		t.Fatal("staying indexed is not a coverage change")
	}
	if _, ok := CoverageChangeKind(AdmissionProbation, AdmissionCandidate); ok {
		t.Fatal("staying outside the index is not a coverage change")
	}
}

func TestCoverageHostAndLabel(t *testing.T) {
	if got := CoverageHost("wss://relay.example.com/path"); got != "relay.example.com" {
		t.Fatalf("host = %q", got)
	}
	if got := CoverageLabel("added", "relay.example.com"); got != "Added relay.example.com" {
		t.Fatalf("label = %q", got)
	}
	if got := CoverageLabel("dropped", "relay.example.com"); got != "Dropped relay.example.com" {
		t.Fatalf("label = %q", got)
	}
}
