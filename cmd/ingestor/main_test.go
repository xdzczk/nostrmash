package main

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/config"
)

func TestResolveLiveKinds(t *testing.T) {
	t.Run("returns active default group kinds", func(t *testing.T) {
		kinds, err := resolveLiveKinds(config.RelayConfig{
			ActiveFilterGroup: "default_v1",
			FilterGroups: map[string]config.FilterGroup{
				"default_v1": {Kinds: []int{1, 3, 7}},
			},
		})
		if err != nil {
			t.Fatalf("resolve kinds: %v", err)
		}
		if len(kinds) != 3 || kinds[0] != 1 || kinds[1] != 3 || kinds[2] != 7 {
			t.Fatalf("unexpected kinds: %#v", kinds)
		}
	})

	t.Run("fails when active group missing", func(t *testing.T) {
		_, err := resolveLiveKinds(config.RelayConfig{
			ActiveFilterGroup: "default_v1",
			FilterGroups:      map[string]config.FilterGroup{},
		})
		if err == nil {
			t.Fatal("expected error for missing active filter group")
		}
	})

	t.Run("fails for unimplemented group", func(t *testing.T) {
		_, err := resolveLiveKinds(config.RelayConfig{
			ActiveFilterGroup: "next_v2",
			FilterGroups: map[string]config.FilterGroup{
				"next_v2": {Kinds: []int{1}},
			},
		})
		if err == nil {
			t.Fatal("expected error for unimplemented filter group")
		}
	})
}

func TestResolveBuildVersion(t *testing.T) {
	orig := buildVersion
	t.Cleanup(func() {
		buildVersion = orig
	})

	buildVersion = "binary-v1.2.3"
	if got := resolveBuildVersion("env-v9.9.9"); got != "binary-v1.2.3" {
		t.Fatalf("expected build version override, got %q", got)
	}

	buildVersion = ""
	if got := resolveBuildVersion(" env-v9.9.9 "); got != "env-v9.9.9" {
		t.Fatalf("expected fallback to app version, got %q", got)
	}
}

func TestSortRelaysByWeights(t *testing.T) {
	normalized := []string{"wss://a", "wss://b", "wss://c"}
	baseOrder := map[string]int{
		"wss://a": 0,
		"wss://b": 1,
		"wss://c": 2,
	}
	weights := map[string]float64{
		"wss://c": 3.0,
		"wss://a": 1.0,
	}
	sorted := sortRelaysByWeights(normalized, baseOrder, weights)
	if len(sorted) != 3 {
		t.Fatalf("unexpected sorted length: %d", len(sorted))
	}
	if sorted[0] != "wss://c" || sorted[1] != "wss://a" || sorted[2] != "wss://b" {
		t.Fatalf("unexpected sorted order: %#v", sorted)
	}
}
