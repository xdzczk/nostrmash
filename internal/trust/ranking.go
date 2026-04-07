package trust

import (
	"math"
	"sort"
)

func computeIterativeGlobalRank(adjacency map[string][]string, nodeSet map[string]struct{}) []rankNode {
	if len(nodeSet) == 0 {
		return nil
	}
	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	n := float64(len(nodes))
	scores := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		scores[node] = 1.0 / n
	}

	teleport := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		teleport[node] = 1.0 / n
	}

	const (
		damping    = 0.85
		maxIters   = 30
		tolerance  = 1e-9
		minOutSize = 1
	)
	for i := 0; i < maxIters; i++ {
		nextScores := make(map[string]float64, len(nodes))
		for _, node := range nodes {
			nextScores[node] = (1.0 - damping) * teleport[node]
		}

		danglingMass := 0.0
		for _, src := range nodes {
			out := adjacency[src]
			if len(out) < minOutSize {
				danglingMass += scores[src]
				continue
			}
			share := damping * scores[src] / float64(len(out))
			for _, dst := range out {
				if _, ok := nextScores[dst]; !ok {
					nextScores[dst] = 0
				}
				nextScores[dst] += share
			}
		}
		if danglingMass > 0 {
			distribute := damping * danglingMass
			for _, node := range nodes {
				nextScores[node] += distribute * teleport[node]
			}
		}

		delta := 0.0
		for _, node := range nodes {
			delta += math.Abs(nextScores[node] - scores[node])
		}
		scores = nextScores
		if delta < tolerance {
			break
		}
	}

	out := make([]rankNode, 0, len(nodes))
	for _, node := range nodes {
		out = append(out, rankNode{
			Pubkey: node,
			Score:  scores[node],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].Pubkey < out[j].Pubkey
		}
		return out[i].Score > out[j].Score
	})
	return out
}
