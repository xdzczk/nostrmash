package relaydiscovery

import (
	"testing"
)

func TestSortCandidates(t *testing.T) {
	candidates := []relayCandidateAgg{
		{NormalizedURL: "wss://a.com", DistinctUsers: 5},
		{NormalizedURL: "wss://b.com", DistinctUsers: 20},
		{NormalizedURL: "wss://c.com", DistinctUsers: 10},
	}
	sortCandidates(candidates)
	if candidates[0].NormalizedURL != "wss://b.com" {
		t.Fatalf("expected most popular first, got %s", candidates[0].NormalizedURL)
	}
	if candidates[1].NormalizedURL != "wss://c.com" {
		t.Fatalf("expected second most popular second, got %s", candidates[1].NormalizedURL)
	}
	if candidates[2].NormalizedURL != "wss://a.com" {
		t.Fatalf("expected least popular last, got %s", candidates[2].NormalizedURL)
	}
}

func TestSortCandidates_Empty(t *testing.T) {
	sortCandidates(nil)
	sortCandidates([]relayCandidateAgg{})
}

func TestSortCandidates_Single(t *testing.T) {
	candidates := []relayCandidateAgg{
		{NormalizedURL: "wss://only.com", DistinctUsers: 1},
	}
	sortCandidates(candidates)
	if candidates[0].NormalizedURL != "wss://only.com" {
		t.Fatal("single element sort should preserve it")
	}
}

func TestPlanDiscoveryUpserts_RefreshesExistingBeyondNewInsertBudget(t *testing.T) {
	candidates := []relayCandidateAgg{
		{URLKey: "new-a", NormalizedURL: "wss://new-a", DistinctUsers: 50},
		{URLKey: "new-b", NormalizedURL: "wss://new-b", DistinctUsers: 40},
		{URLKey: "inactive-big", NormalizedURL: "wss://inactive-big", DistinctUsers: 30},
		{URLKey: "new-c", NormalizedURL: "wss://new-c", DistinctUsers: 20},
		{URLKey: "inactive-small", NormalizedURL: "wss://inactive-small", DistinctUsers: 2},
	}
	existing := map[string]struct{}{
		"inactive-big":   {},
		"inactive-small": {},
	}

	planned := planDiscoveryUpserts(candidates, existing, 3, 1)
	keys := make([]string, len(planned))
	for i, c := range planned {
		keys[i] = c.URLKey
	}

	// New-insert budget is 1 (new-a). Existing relays are still refreshed,
	// including inactive-small which is below the min-ref threshold.
	want := []string{"new-a", "inactive-big", "inactive-small"}
	if len(keys) != len(want) {
		t.Fatalf("planned keys=%v want=%v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("planned keys=%v want=%v", keys, want)
		}
	}
}

func TestPlanDiscoveryUpserts_SkipsUnknownBelowMinRefs(t *testing.T) {
	candidates := []relayCandidateAgg{
		{URLKey: "tiny-new", NormalizedURL: "wss://tiny-new", DistinctUsers: 1},
		{URLKey: "known", NormalizedURL: "wss://known", DistinctUsers: 1},
	}
	existing := map[string]struct{}{"known": {}}

	planned := planDiscoveryUpserts(candidates, existing, 3, 25)
	if len(planned) != 1 || planned[0].URLKey != "known" {
		t.Fatalf("expected only known refresh, got %+v", planned)
	}
}

func TestPlanDiscoveryUpserts_RespectsZeroNewInsertBudget(t *testing.T) {
	candidates := []relayCandidateAgg{
		{URLKey: "new", NormalizedURL: "wss://new", DistinctUsers: 100},
		{URLKey: "known", NormalizedURL: "wss://known", DistinctUsers: 5},
	}
	existing := map[string]struct{}{"known": {}}

	planned := planDiscoveryUpserts(candidates, existing, 3, 0)
	if len(planned) != 1 || planned[0].URLKey != "known" {
		t.Fatalf("expected only refreshes when new-insert budget is 0, got %+v", planned)
	}
}
