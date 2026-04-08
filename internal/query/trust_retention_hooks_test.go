package query

import (
	"testing"
	"time"
)

func TestTrustRetentionHooks_Select_UsesTrustedVsUntrustedHorizon(t *testing.T) {
	hooks := DefaultTrustRetentionHooks(trustModePreferTrusted)

	trustedSelection, err := hooks.Select(TrustRetentionScopeDiscoveryCandidateProjection, true)
	if err != nil {
		t.Fatalf("select trusted retention horizon: %v", err)
	}
	if trustedSelection.Horizon != hooks.DiscoveryCandidateProjection.TrustedHorizon {
		t.Fatalf("expected trusted horizon %s, got %s", hooks.DiscoveryCandidateProjection.TrustedHorizon, trustedSelection.Horizon)
	}
	if !trustedSelection.TrustAware {
		t.Fatalf("expected trust-aware selection in prefer_trusted mode")
	}

	untrustedSelection, err := hooks.Select(TrustRetentionScopeDiscoveryCandidateProjection, false)
	if err != nil {
		t.Fatalf("select untrusted retention horizon: %v", err)
	}
	if untrustedSelection.Horizon != hooks.DiscoveryCandidateProjection.UntrustedHorizon {
		t.Fatalf("expected untrusted horizon %s, got %s", hooks.DiscoveryCandidateProjection.UntrustedHorizon, untrustedSelection.Horizon)
	}
	if !untrustedSelection.TrustAware {
		t.Fatalf("expected trust-aware selection in prefer_trusted mode")
	}
}

func TestTrustRetentionHooks_Select_OpenModeUsesTrustedHorizonForAll(t *testing.T) {
	hooks := DefaultTrustRetentionHooks(trustModeOpen)

	selection, err := hooks.Select(TrustRetentionScopeDiscoveryCache, false)
	if err != nil {
		t.Fatalf("select open-mode retention horizon: %v", err)
	}
	if selection.Horizon != hooks.DiscoveryCache.TrustedHorizon {
		t.Fatalf("expected trusted horizon fallback %s, got %s", hooks.DiscoveryCache.TrustedHorizon, selection.Horizon)
	}
	if selection.TrustAware {
		t.Fatalf("did not expect trust-aware selection in open mode")
	}
}

func TestTrustRetentionHooks_Select_RejectsCanonicalDurableScope(t *testing.T) {
	hooks := DefaultTrustRetentionHooks(trustModePreferTrusted)
	if _, err := hooks.Select(TrustRetentionScopeCanonicalDurableEvents, true); err == nil {
		t.Fatal("expected canonical durable scope to be rejected")
	}
}

func TestTrustRetentionHooks_ValidateRejectsUnsafeHookConfig(t *testing.T) {
	hooks := DefaultTrustRetentionHooks(trustModePreferTrusted)
	hooks.DiscoveryCache.UntrustedHorizon = 15 * time.Minute
	if err := hooks.Validate(); err == nil {
		t.Fatal("expected validation error when untrusted horizon exceeds trusted horizon")
	}
}
