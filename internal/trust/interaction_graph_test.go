package trust

import "testing"

func TestSpearmanRankCorrelation_Identical(t *testing.T) {
	ranked := []rankNode{{Pubkey: "a", Score: 3}, {Pubkey: "b", Score: 2}, {Pubkey: "c", Score: 1}}
	got := spearmanRankCorrelation(ranked, ranked)
	if got != 1 {
		t.Fatalf("expected perfect correlation, got %v", got)
	}
}

func TestSetOverlap(t *testing.T) {
	if got := setOverlap([]string{"a", "b", "c"}, []string{"b", "d"}); got != 1 {
		t.Fatalf("unexpected overlap: %d", got)
	}
}

func TestMergeWeightedAdjacency(t *testing.T) {
	nodes := map[string]struct{}{"a": {}, "b": {}}
	base := map[string]map[string]float64{"a": {"b": 1}}
	extra := map[string]map[string]float64{"a": {"b": 2}, "b": {"c": 1.5}}
	merged := mergeWeightedAdjacency(base, extra, nodes)
	if merged["a"]["b"] != 3 {
		t.Fatalf("expected summed weight 3, got %v", merged["a"]["b"])
	}
	if _, ok := nodes["c"]; !ok {
		t.Fatalf("expected interaction destination to join node set")
	}
}
