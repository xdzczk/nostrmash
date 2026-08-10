package query

import "testing"

func TestTrustSummaryFromState(t *testing.T) {
	t.Parallel()

	hop1 := 1
	rank := int64(5)
	seedState := TrustState{Pubkey: "seed", IsSeed: true, HopDistance: intPtr(0), Rank: &rank}
	got := TrustSummaryFromState(seedState, 100)
	if got.Tier != "seed" || got.HopDistance == nil || *got.HopDistance != 0 {
		t.Fatalf("unexpected seed summary: %#v", got)
	}
	if got.Percentile == nil || *got.Percentile != 5.0 {
		t.Fatalf("expected percentile 5.0, got %#v", got.Percentile)
	}

	inNetwork := TrustState{Pubkey: "alice", HopDistance: &hop1, Rank: &rank}
	got = TrustSummaryFromState(inNetwork, 100)
	if got.Tier != "in_network" {
		t.Fatalf("expected in_network tier, got %#v", got)
	}

	unranked := TrustState{Pubkey: "unknown"}
	got = TrustSummaryFromState(unranked, 100)
	if got.Tier != "unranked" || got.HopDistance != nil || got.Percentile != nil {
		t.Fatalf("unexpected unranked summary: %#v", got)
	}

	// No denominator => no percentile even when rank is present.
	got = TrustSummaryFromState(inNetwork, 0)
	if got.Percentile != nil {
		t.Fatalf("expected nil percentile with totalRanked=0, got %#v", got)
	}
}

func intPtr(v int) *int { return &v }
