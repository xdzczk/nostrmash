package trust

import (
	"fmt"
	"testing"
)

func BenchmarkComputeIterativeGlobalRankSmall(b *testing.B) {
	benchmarkComputeIterativeGlobalRank(b, 32)
}

func BenchmarkComputeIterativeGlobalRankMedium(b *testing.B) {
	benchmarkComputeIterativeGlobalRank(b, 128)
}

func BenchmarkComputeIterativeGlobalRankLarge(b *testing.B) {
	benchmarkComputeIterativeGlobalRank(b, 512)
}

func benchmarkComputeIterativeGlobalRank(b *testing.B, nodeCount int) {
	adjacency, nodeSet := buildBenchmarkGraph(nodeCount)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ranked := computeIterativeGlobalRank(adjacency, nodeSet)
		if len(ranked) != nodeCount {
			b.Fatalf("expected %d ranked nodes, got %d", nodeCount, len(ranked))
		}
	}
}

func buildBenchmarkGraph(nodeCount int) (map[string][]string, map[string]struct{}) {
	adjacency := make(map[string][]string, nodeCount)
	nodeSet := make(map[string]struct{}, nodeCount)

	for i := 0; i < nodeCount; i++ {
		pubkey := fmt.Sprintf("pk_%04d", i)
		nodeSet[pubkey] = struct{}{}

		neighbors := make([]string, 0, 3)
		for step := 1; step <= 3; step++ {
			target := fmt.Sprintf("pk_%04d", (i+step)%nodeCount)
			neighbors = append(neighbors, target)
		}
		adjacency[pubkey] = neighbors
	}

	return adjacency, nodeSet
}
