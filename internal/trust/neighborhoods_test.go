package trust

import (
	"testing"
)

func TestComputeSeedNeighborhoods_BFSWeightsAndCap(t *testing.T) {
	adjacency := map[string][]string{
		"seed": {"a", "b"},
		"a":    {"c"},
		"b":    {"d"},
		"c":    {"e"},
	}
	seeds := map[string]struct{}{"seed": {}}

	members := computeSeedNeighborhoods(adjacency, seeds, 2, 4)
	if len(members) != 4 {
		t.Fatalf("expected 4 members under cap, got %d (%+v)", len(members), members)
	}
	byMember := make(map[string]neighborhoodMember, len(members))
	for _, member := range members {
		byMember[member.MemberPubkey] = member
	}
	if byMember["seed"].Hops != 0 || byMember["seed"].Weight != 1.0 {
		t.Fatalf("unexpected seed member: %+v", byMember["seed"])
	}
	if byMember["a"].Hops != 1 || byMember["a"].Weight != 0.5 {
		t.Fatalf("unexpected a member: %+v", byMember["a"])
	}
	if byMember["b"].Hops != 1 {
		t.Fatalf("unexpected b member: %+v", byMember["b"])
	}
	// Deterministic BFS explores sorted neighbors; with cap 4 we keep seed,a,b
	// and one hop-2 node (c before d).
	if _, ok := byMember["c"]; !ok {
		t.Fatalf("expected hop-2 member c under deterministic BFS, got %+v", members)
	}
	if _, ok := byMember["d"]; ok {
		t.Fatalf("did not expect d under member cap, got %+v", members)
	}
	if _, ok := byMember["e"]; ok {
		t.Fatalf("did not expect hop-3 e with maxHops=2, got %+v", members)
	}
}

func TestComputeSeedNeighborhoods_MultipleSeedsIndependent(t *testing.T) {
	adjacency := map[string][]string{
		"s1": {"a"},
		"s2": {"b"},
	}
	seeds := map[string]struct{}{"s1": {}, "s2": {}}
	members := computeSeedNeighborhoods(adjacency, seeds, 1, 100)
	if len(members) != 4 {
		t.Fatalf("expected 4 members across two seeds, got %d", len(members))
	}
	counts := map[string]int{}
	for _, member := range members {
		counts[member.SeedPubkey]++
	}
	if counts["s1"] != 2 || counts["s2"] != 2 {
		t.Fatalf("unexpected per-seed counts: %+v", counts)
	}
}
