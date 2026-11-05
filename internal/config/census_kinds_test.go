package config

import "testing"

func TestCensusKindsStayOutOfTheStoredFilter(t *testing.T) {
	stored := make(map[int]struct{}, len(defaultFilterGroupKinds))
	for _, kind := range defaultFilterGroupKinds {
		stored[kind] = struct{}{}
	}
	seen := make(map[int]struct{}, len(CensusKinds()))
	for _, kind := range CensusKinds() {
		if _, ok := stored[kind]; ok {
			t.Fatalf("census kind %d is also stored", kind)
		}
		if _, ok := seen[kind]; ok {
			t.Fatalf("duplicate census kind %d", kind)
		}
		seen[kind] = struct{}{}
		if !IsCensusKind(kind) {
			t.Fatalf("kind %d missing from the census set", kind)
		}
	}
	if IsCensusKind(1) || !IsCensusKind(7000) || !IsCensusKind(6000) || !IsCensusKind(9000) {
		t.Fatal("census membership")
	}
}
