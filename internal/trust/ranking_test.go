package trust

import "testing"

func TestComputeIterativeGlobalRank_EmptyGraph(t *testing.T) {
	ranked := computeIterativeGlobalRank(map[string][]string{}, map[string]struct{}{})
	if len(ranked) != 0 {
		t.Fatalf("expected empty rank output, got %d entries", len(ranked))
	}
}

func TestComputeIterativeGlobalRank_DeterministicOrderAndDangling(t *testing.T) {
	adj := map[string][]string{
		"a": {"b"},
		"b": {"a"},
		"c": {},
	}
	nodes := map[string]struct{}{
		"a": {},
		"b": {},
		"c": {},
	}

	ranked := computeIterativeGlobalRank(adj, nodes)
	if len(ranked) != 3 {
		t.Fatalf("expected 3 ranked nodes, got %d", len(ranked))
	}
	if ranked[0].Pubkey != "a" || ranked[1].Pubkey != "b" || ranked[2].Pubkey != "c" {
		t.Fatalf("unexpected deterministic rank order: %#v", ranked)
	}
	if ranked[0].Score < ranked[2].Score || ranked[1].Score < ranked[2].Score {
		t.Fatalf("expected strongly connected pair to outrank dangling node: %#v", ranked)
	}
}
