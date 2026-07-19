package query

import (
	"fmt"
	"strings"
	"time"
)

// TrustRetentionScope enumerates retention surfaces where trust-aware policy is
// explicitly allowed. Canonical durable event storage is intentionally excluded.
type TrustRetentionScope string

const (
	TrustRetentionScopeDiscoveryCache               TrustRetentionScope = "discovery_cache"
	TrustRetentionScopeDiscoveryCandidateProjection TrustRetentionScope = "discovery_candidate_projection"
	TrustRetentionScopeLowValueEnrichmentState      TrustRetentionScope = "low_value_enrichment_state"
	TrustRetentionScopeFallbackTransientMetadata    TrustRetentionScope = "fallback_transient_metadata"
	TrustRetentionScopeCanonicalDurableEvents       TrustRetentionScope = "canonical_durable_events"
)

type TrustRetentionHook struct {
	Owner            string
	Enabled          bool
	CanonicalDurable bool
	TrustedHorizon   time.Duration
	UntrustedHorizon time.Duration
}

type TrustRetentionHooks struct {
	Mode                         string
	DiscoveryCache               TrustRetentionHook
	DiscoveryCandidateProjection TrustRetentionHook
	LowValueEnrichmentState      TrustRetentionHook
	FallbackTransientMetadata    TrustRetentionHook
}

type TrustRetentionSelection struct {
	Owner      string
	Horizon    time.Duration
	TrustAware bool
}

func DefaultTrustRetentionHooks(mode string) TrustRetentionHooks {
	mode = strings.TrimSpace(strings.ToLower(mode))
	if mode == "" {
		mode = trustModeOpen
	}
	return TrustRetentionHooks{
		Mode: mode,
		DiscoveryCache: TrustRetentionHook{
			Owner:            "query.discovery_cache",
			Enabled:          true,
			CanonicalDurable: false,
			TrustedHorizon:   10 * time.Minute,
			UntrustedHorizon: 2 * time.Minute,
		},
		DiscoveryCandidateProjection: TrustRetentionHook{
			Owner:            "query.discovery_candidate_projection",
			Enabled:          true,
			CanonicalDurable: false,
			TrustedHorizon:   24 * time.Hour,
			UntrustedHorizon: 6 * time.Hour,
		},
		LowValueEnrichmentState: TrustRetentionHook{
			Owner:            "query.low_value_enrichment_state",
			Enabled:          false,
			CanonicalDurable: false,
			TrustedHorizon:   12 * time.Hour,
			UntrustedHorizon: 3 * time.Hour,
		},
		FallbackTransientMetadata: TrustRetentionHook{
			Owner:            "query.fallback_transient_metadata",
			Enabled:          false,
			CanonicalDurable: false,
			TrustedHorizon:   2 * time.Hour,
			UntrustedHorizon: 30 * time.Minute,
		},
	}
}

func (h TrustRetentionHooks) Validate() error {
	mode := strings.TrimSpace(strings.ToLower(h.Mode))
	if mode == "" {
		mode = trustModeOpen
	}
	switch mode {
	case trustModeOpen, trustModePreferTrusted, trustModeTrustedOnly:
	default:
		return fmt.Errorf("invalid retention trust mode %q", h.Mode)
	}
	if err := validateTrustRetentionHook("discovery_cache", h.DiscoveryCache); err != nil {
		return err
	}
	if err := validateTrustRetentionHook("discovery_candidate_projection", h.DiscoveryCandidateProjection); err != nil {
		return err
	}
	if err := validateTrustRetentionHook("low_value_enrichment_state", h.LowValueEnrichmentState); err != nil {
		return err
	}
	return validateTrustRetentionHook("fallback_transient_metadata", h.FallbackTransientMetadata)
}

func (h TrustRetentionHooks) isZero() bool {
	return strings.TrimSpace(h.Mode) == "" &&
		h.DiscoveryCache == (TrustRetentionHook{}) &&
		h.DiscoveryCandidateProjection == (TrustRetentionHook{}) &&
		h.LowValueEnrichmentState == (TrustRetentionHook{}) &&
		h.FallbackTransientMetadata == (TrustRetentionHook{})
}

func validateTrustRetentionHook(scope string, hook TrustRetentionHook) error {
	if strings.TrimSpace(hook.Owner) == "" {
		return fmt.Errorf("retention hook %s owner is required", scope)
	}
	if hook.CanonicalDurable {
		return fmt.Errorf("retention hook %s must not target canonical durable data", scope)
	}
	if !hook.Enabled {
		return nil
	}
	if hook.TrustedHorizon <= 0 {
		return fmt.Errorf("retention hook %s trusted horizon must be > 0", scope)
	}
	if hook.UntrustedHorizon <= 0 {
		return fmt.Errorf("retention hook %s untrusted horizon must be > 0", scope)
	}
	if hook.UntrustedHorizon > hook.TrustedHorizon {
		return fmt.Errorf("retention hook %s untrusted horizon must be <= trusted horizon", scope)
	}
	return nil
}

func (h TrustRetentionHooks) Select(scope TrustRetentionScope, trusted bool) (TrustRetentionSelection, error) {
	hook, ok, err := h.hookForScope(scope)
	if err != nil {
		return TrustRetentionSelection{}, err
	}
	if !ok || !hook.Enabled {
		return TrustRetentionSelection{}, nil
	}
	mode := strings.TrimSpace(strings.ToLower(h.Mode))
	if mode == "" {
		mode = trustModeOpen
	}
	if mode == trustModeOpen || trusted {
		return TrustRetentionSelection{
			Owner:      hook.Owner,
			Horizon:    hook.TrustedHorizon,
			TrustAware: mode != trustModeOpen,
		}, nil
	}
	return TrustRetentionSelection{
		Owner:      hook.Owner,
		Horizon:    hook.UntrustedHorizon,
		TrustAware: true,
	}, nil
}

func (h TrustRetentionHooks) hookForScope(scope TrustRetentionScope) (TrustRetentionHook, bool, error) {
	switch scope {
	case TrustRetentionScopeDiscoveryCache:
		return h.DiscoveryCache, true, nil
	case TrustRetentionScopeDiscoveryCandidateProjection:
		return h.DiscoveryCandidateProjection, true, nil
	case TrustRetentionScopeLowValueEnrichmentState:
		return h.LowValueEnrichmentState, true, nil
	case TrustRetentionScopeFallbackTransientMetadata:
		return h.FallbackTransientMetadata, true, nil
	case TrustRetentionScopeCanonicalDurableEvents:
		return TrustRetentionHook{}, false, fmt.Errorf("canonical durable retention is out of scope for trust-aware hooks")
	default:
		return TrustRetentionHook{}, false, nil
	}
}
