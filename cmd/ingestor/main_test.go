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
