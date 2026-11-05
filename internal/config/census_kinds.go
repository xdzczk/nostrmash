package config

import "slices"

// censusFilterGroupName is the built-in count-only subscription. Its events
// are tallied and then discarded; they are not part of default_v1.
const censusFilterGroupName = "census_v1"

// dvmRequestKinds are the NIP-90 registry request kinds. Result kinds are
// the same numbers plus 1000. Kind 7000 is job feedback.
var dvmRequestKinds = []int{
	5000, 5001, 5002, 5050, 5100, 5200, 5201, 5202, 5250,
	5300, 5301, 5302, 5303, 5400, 5500, 5900, 5901, 5905, 5970,
}

// CensusKinds is the count-only kind set: git, wiki, publications, calendar,
// classifieds, voice, NIP-66 monitors, the NIP-90 registry, and NIP-29
// moderation. The list is explicit because relays cap filter size.
func CensusKinds() []int {
	kinds := make([]int, 0, len(censusKindSet))
	for kind := range censusKindSet {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	return kinds
}

// IsCensusKind reports whether a kind is counted and then discarded.
func IsCensusKind(kind int) bool {
	_, ok := censusKindSet[kind]
	return ok
}

var censusKindSet = func() map[int]struct{} {
	kinds := []int{
		1222, 1244, 1617, 1621, 10166, 30166, 30402, 30617, 30818,
		30040, 30041, 31922, 31923, 31925, 7000,
	}
	kinds = append(kinds, dvmRequestKinds...)
	for _, request := range dvmRequestKinds {
		kinds = append(kinds, request+1000)
	}
	for kind := 9000; kind <= 9022; kind++ {
		kinds = append(kinds, kind)
	}
	out := make(map[int]struct{}, len(kinds))
	for _, kind := range kinds {
		out[kind] = struct{}{}
	}
	return out
}()
