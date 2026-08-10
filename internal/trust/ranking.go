package trust

import (
	"math"
	"sort"
	"strings"
)

const (
	rankDamping   = 0.85
	rankMaxIters  = 30
	rankTolerance = 1e-9
)

func computeIterativeGlobalRank(adjacency map[string][]string, nodeSet map[string]struct{}) []rankNode {
	if len(nodeSet) == 0 {
		return nil
	}
	return ComputePersonalizedRank(adjacency, nodeSet, uniformTeleport(nodeSet), rankDamping)
}

// ComputePersonalizedRank runs iterative PageRank-style ranking with a caller
// supplied teleport vector over an unweighted adjacency list. Teleport mass is
// renormalized over nodes present in nodeSet; missing/zero mass falls back to a
// uniform teleport vector.
func ComputePersonalizedRank(
	adjacency map[string][]string,
	nodeSet map[string]struct{},
	teleport map[string]float64,
	damping float64,
) []rankNode {
	return ComputePersonalizedRankWeighted(adjacencyToWeighted(adjacency), nodeSet, teleport, damping)
}

// ComputePersonalizedRankWeighted is the weighted-adjacency ranking core used by
// optional interaction-graph merges. Follow-only ranking passes unit weights.
func ComputePersonalizedRankWeighted(
	adjacency map[string]map[string]float64,
	nodeSet map[string]struct{},
	teleport map[string]float64,
	damping float64,
) []rankNode {
	if len(nodeSet) == 0 {
		return nil
	}
	if damping <= 0 || damping >= 1 {
		damping = rankDamping
	}

	nodes := make([]string, 0, len(nodeSet))
	for node := range nodeSet {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	normalizedTeleport := normalizeTeleport(nodes, teleport)
	scores := make(map[string]float64, len(nodes))
	for _, node := range nodes {
		scores[node] = normalizedTeleport[node]
	}

	outWeights := make(map[string]float64, len(nodes))
	for _, src := range nodes {
		sum := 0.0
		for dst, weight := range adjacency[src] {
			dst = strings.TrimSpace(dst)
			if dst == "" || weight <= 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
				continue
			}
			if _, ok := nodeSet[dst]; !ok {
				continue
			}
			sum += weight
		}
		outWeights[src] = sum
	}

	for i := 0; i < rankMaxIters; i++ {
		nextScores := make(map[string]float64, len(nodes))
		for _, node := range nodes {
			nextScores[node] = (1.0 - damping) * normalizedTeleport[node]
		}

		danglingMass := 0.0
		for _, src := range nodes {
			outWeight := outWeights[src]
			if outWeight <= 0 {
				danglingMass += scores[src]
				continue
			}
			for dst, weight := range adjacency[src] {
				dst = strings.TrimSpace(dst)
				if dst == "" || weight <= 0 {
					continue
				}
				if _, ok := nextScores[dst]; !ok {
					continue
				}
				nextScores[dst] += damping * scores[src] * (weight / outWeight)
			}
		}
		if danglingMass > 0 {
			distribute := damping * danglingMass
			for _, node := range nodes {
				nextScores[node] += distribute * normalizedTeleport[node]
			}
		}

		delta := 0.0
		for _, node := range nodes {
			delta += math.Abs(nextScores[node] - scores[node])
		}
		scores = nextScores
		if delta < rankTolerance {
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

func normalizeTeleport(nodes []string, teleport map[string]float64) map[string]float64 {
	n := float64(len(nodes))
	out := make(map[string]float64, len(nodes))
	mass := 0.0
	for _, node := range nodes {
		weight := 0.0
		if teleport != nil {
			weight = teleport[node]
		}
		if weight < 0 || math.IsNaN(weight) || math.IsInf(weight, 0) {
			weight = 0
		}
		out[node] = weight
		mass += weight
	}
	if mass <= 0 {
		for _, node := range nodes {
			out[node] = 1.0 / n
		}
		return out
	}
	for _, node := range nodes {
		out[node] /= mass
	}
	return out
}
