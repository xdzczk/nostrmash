package store

import "testing"

func TestNormalizeRelayURLs_DeduplicatesAndNormalizes(t *testing.T) {
	normalized, order := NormalizeRelayURLs([]string{
		" wss://A ",
		"wss://b",
		"wss://a",
		"",
		" WSS://B ",
		"wss://c",
	})
	if len(normalized) != 3 {
		t.Fatalf("unexpected normalized length: %#v", normalized)
	}
	if normalized[0] != "wss://a" || normalized[1] != "wss://b" || normalized[2] != "wss://c" {
		t.Fatalf("unexpected normalized values: %#v", normalized)
	}
	if order["wss://a"] != 0 || order["wss://b"] != 1 || order["wss://c"] != 5 {
		t.Fatalf("unexpected base order map: %#v", order)
	}
}

func TestSortRelaysByWeights_PrefersHigherWeightThenBaseOrder(t *testing.T) {
	sorted := SortRelaysByWeights(
		[]string{"wss://a", "wss://b", "wss://c"},
		map[string]int{"wss://a": 0, "wss://b": 1, "wss://c": 2},
		map[string]float64{"wss://c": 10, "wss://a": 5},
	)
	if len(sorted) != 3 {
		t.Fatalf("unexpected sorted length: %#v", sorted)
	}
	if sorted[0] != "wss://c" || sorted[1] != "wss://a" || sorted[2] != "wss://b" {
		t.Fatalf("unexpected sorted order: %#v", sorted)
	}
}
