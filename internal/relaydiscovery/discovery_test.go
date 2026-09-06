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

	planned := planDiscoveryUpserts(candidates, existing, nil, 3, 1, 3)
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

	planned := planDiscoveryUpserts(candidates, existing, nil, 3, 25, 3)
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

	planned := planDiscoveryUpserts(candidates, existing, nil, 3, 0, 3)
	if len(planned) != 1 || planned[0].URLKey != "known" {
		t.Fatalf("expected only refreshes when new-insert budget is 0, got %+v", planned)
	}
}

// TestPlanDiscoveryUpserts_CapsVariantsPerHost locks in the junk-variant
// guard: once a hostname has maxVariantsPerHost registry entries (existing
// plus planned this run), further new URL variants of that host are refused
// while distinct hosts and refreshes of known variants still pass. Without
// this cap, user relay lists steadily fed path-variant junk
// (wss://host/random-words) into the candidate pool — production accumulated
// thousands of candidates that probe fine but are all the same few relays.
func TestPlanDiscoveryUpserts_CapsVariantsPerHost(t *testing.T) {
	candidates := []relayCandidateAgg{
		{URLKey: "spam-1", NormalizedURL: "wss://popular.example/one", DistinctUsers: 50},
		{URLKey: "spam-2", NormalizedURL: "wss://popular.example/two", DistinctUsers: 40},
		{URLKey: "fresh-host", NormalizedURL: "wss://fresh.example", DistinctUsers: 30},
		{URLKey: "known-variant", NormalizedURL: "wss://popular.example/known", DistinctUsers: 5},
	}
	existing := map[string]struct{}{"known-variant": {}}
	// Host already at the cap of 2 (the known variant plus the bare host).
	hostCounts := map[string]int{"popular.example": 2}

	planned := planDiscoveryUpserts(candidates, existing, hostCounts, 3, 25, 2)
	keys := make([]string, len(planned))
	for i, c := range planned {
		keys[i] = c.URLKey
	}
	want := []string{"fresh-host", "known-variant"}
	if len(keys) != len(want) {
		t.Fatalf("planned keys=%v want=%v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("planned keys=%v want=%v", keys, want)
		}
	}

	// Below the cap, variants of one host are admitted until the cap fills.
	hostCounts = map[string]int{}
	planned = planDiscoveryUpserts(candidates, existing, hostCounts, 3, 25, 2)
	keys = keys[:0]
	for _, c := range planned {
		keys = append(keys, c.URLKey)
	}
	// spam-1 and spam-2 fill popular.example's cap of 2; fresh-host has its
	// own host; known-variant refreshes regardless.
	want = []string{"spam-1", "spam-2", "fresh-host", "known-variant"}
	if len(keys) != len(want) {
		t.Fatalf("planned keys=%v want=%v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("planned keys=%v want=%v", keys, want)
		}
	}
}
