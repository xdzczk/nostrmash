package trust

import (
	"math"
	"testing"
)

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

func TestComputePersonalizedRank_UniformTeleportMatchesGlobal(t *testing.T) {
	adj := map[string][]string{
		"a": {"b", "c"},
		"b": {"a"},
		"c": {"a", "b"},
	}
	nodes := map[string]struct{}{
		"a": {},
		"b": {},
		"c": {},
	}
	global := computeIterativeGlobalRank(adj, nodes)
	teleport := map[string]float64{"a": 1.0 / 3.0, "b": 1.0 / 3.0, "c": 1.0 / 3.0}
	personalized := ComputePersonalizedRank(adj, nodes, teleport, rankDamping)
	if len(global) != len(personalized) {
		t.Fatalf("length mismatch: global=%d personalized=%d", len(global), len(personalized))
	}
	for i := range global {
		if global[i].Pubkey != personalized[i].Pubkey {
			t.Fatalf("order mismatch at %d: global=%#v personalized=%#v", i, global, personalized)
		}
		if math.Abs(global[i].Score-personalized[i].Score) > 1e-12 {
			t.Fatalf("score mismatch at %d: global=%v personalized=%v", i, global[i].Score, personalized[i].Score)
		}
	}
}

func TestComputePersonalizedRank_SeedTeleportBoostsSeedNeighborhood(t *testing.T) {
	adj := map[string][]string{
		"seed":   {"friend"},
		"friend": {"seed"},
		"other":  {"lonely"},
		"lonely": {},
	}
	nodes := map[string]struct{}{
		"seed":   {},
		"friend": {},
		"other":  {},
		"lonely": {},
	}
	ranked := ComputePersonalizedRank(adj, nodes, map[string]float64{"seed": 1.0}, rankDamping)
	if len(ranked) < 2 {
		t.Fatalf("expected ranked output, got %#v", ranked)
	}
	top := map[string]struct{}{ranked[0].Pubkey: {}, ranked[1].Pubkey: {}}
	if _, ok := top["seed"]; !ok {
		t.Fatalf("expected seed in top-2 personalized ranks, got %#v", ranked)
	}
	if _, ok := top["friend"]; !ok {
		t.Fatalf("expected friend in top-2 personalized ranks, got %#v", ranked)
	}
}
