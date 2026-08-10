package trust

import (
	"sort"
	"strings"
)

// computeSeedNeighborhoods runs a bounded BFS from each seed over follow
// adjacency (follower → followed). The seed is always included at hop 0.
// Per-seed membership is capped at maxMembers (including the seed).
func computeSeedNeighborhoods(
	adjacency map[string][]string,
	seeds map[string]struct{},
	maxHops int,
	maxMembers int,
) []neighborhoodMember {
	if maxHops < 0 {
		maxHops = 0
	}
	if maxMembers <= 0 {
		maxMembers = 1
	}
	seedList := make([]string, 0, len(seeds))
	for seed := range seeds {
		seed = strings.TrimSpace(seed)
		if seed == "" {
			continue
		}
		seedList = append(seedList, seed)
	}
	sort.Strings(seedList)

	out := make([]neighborhoodMember, 0)
	for _, seed := range seedList {
		members := bfsNeighborhood(adjacency, seed, maxHops, maxMembers)
		out = append(out, members...)
	}
	return out
}

func bfsNeighborhood(
	adjacency map[string][]string,
	seed string,
	maxHops int,
	maxMembers int,
) []neighborhoodMember {
	type queueItem struct {
		pubkey string
		hops   int
	}
	visited := map[string]int{seed: 0}
	queue := []queueItem{{pubkey: seed, hops: 0}}
	for len(queue) > 0 && len(visited) < maxMembers {
		item := queue[0]
		queue = queue[1:]
		if item.hops >= maxHops {
			continue
		}
		neighbors := append([]string(nil), adjacency[item.pubkey]...)
		sort.Strings(neighbors)
		for _, next := range neighbors {
			next = strings.TrimSpace(next)
			if next == "" {
				continue
			}
			if _, seen := visited[next]; seen {
				continue
			}
			visited[next] = item.hops + 1
			if len(visited) >= maxMembers {
				break
			}
			queue = append(queue, queueItem{pubkey: next, hops: item.hops + 1})
		}
	}

	members := make([]neighborhoodMember, 0, len(visited))
	for pubkey, hops := range visited {
		members = append(members, neighborhoodMember{
			SeedPubkey:   seed,
			MemberPubkey: pubkey,
			Hops:         hops,
			Weight:       neighborhoodWeight(hops),
		})
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].Hops != members[j].Hops {
			return members[i].Hops < members[j].Hops
		}
		return members[i].MemberPubkey < members[j].MemberPubkey
	})
	return members
}

func neighborhoodWeight(hops int) float64 {
	if hops < 0 {
		hops = 0
	}
	return 1.0 / float64(hops+1)
}
