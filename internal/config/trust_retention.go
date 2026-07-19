package config

import (
	"fmt"
	"time"
)

// TrustRetentionHooksConfig defines trust-aware retention horizons for
// derived/transient data classes only. Canonical event durability is out of scope.
type TrustRetentionHooksConfig struct {
	DiscoveryCache                TrustRetentionHookConfig
	DiscoveryProjectionCandidates TrustRetentionHookConfig
	LowValueEnrichmentState       TrustRetentionHookConfig
	FallbackTransientMetadata     TrustRetentionHookConfig
}

type TrustRetentionHookConfig struct {
	Enabled          bool
	TrustedHorizon   time.Duration
	UntrustedHorizon time.Duration
}

func loadTrustRetentionHooksConfig() (TrustRetentionHooksConfig, error) {
	discoveryCacheTrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_DISCOVERY_CACHE_TRUSTED_TTL", 10*time.Minute)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	discoveryCacheUntrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_DISCOVERY_CACHE_UNTRUSTED_TTL", 2*time.Minute)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	discoveryProjectionTrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_DISCOVERY_CANDIDATE_TRUSTED_MAX_AGE", 24*time.Hour)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	discoveryProjectionUntrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_DISCOVERY_CANDIDATE_UNTRUSTED_MAX_AGE", 6*time.Hour)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	enrichmentTrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_ENRICHMENT_STATE_TRUSTED_MAX_AGE", 12*time.Hour)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	enrichmentUntrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_ENRICHMENT_STATE_UNTRUSTED_MAX_AGE", 3*time.Hour)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	fallbackTrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_FALLBACK_METADATA_TRUSTED_MAX_AGE", 2*time.Hour)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	fallbackUntrusted, err := getEnvPositiveDurationStrict("TRUST_RETENTION_FALLBACK_METADATA_UNTRUSTED_MAX_AGE", 30*time.Minute)
	if err != nil {
		return TrustRetentionHooksConfig{}, err
	}

	cfg := TrustRetentionHooksConfig{
		DiscoveryCache: TrustRetentionHookConfig{
			Enabled:          getEnvBool("TRUST_RETENTION_DISCOVERY_CACHE_ENABLED", true),
			TrustedHorizon:   discoveryCacheTrusted,
			UntrustedHorizon: discoveryCacheUntrusted,
		},
		DiscoveryProjectionCandidates: TrustRetentionHookConfig{
			Enabled:          getEnvBool("TRUST_RETENTION_DISCOVERY_CANDIDATE_ENABLED", true),
			TrustedHorizon:   discoveryProjectionTrusted,
			UntrustedHorizon: discoveryProjectionUntrusted,
		},
		LowValueEnrichmentState: TrustRetentionHookConfig{
			Enabled:          getEnvBool("TRUST_RETENTION_ENRICHMENT_STATE_ENABLED", false),
			TrustedHorizon:   enrichmentTrusted,
			UntrustedHorizon: enrichmentUntrusted,
		},
		FallbackTransientMetadata: TrustRetentionHookConfig{
			Enabled:          getEnvBool("TRUST_RETENTION_FALLBACK_METADATA_ENABLED", false),
			TrustedHorizon:   fallbackTrusted,
			UntrustedHorizon: fallbackUntrusted,
		},
	}
	if err := validateTrustRetentionHooksConfig(cfg); err != nil {
		return TrustRetentionHooksConfig{}, err
	}
	return cfg, nil
}

func validateTrustRetentionHooksConfig(cfg TrustRetentionHooksConfig) error {
	if err := validateTrustRetentionHookConfig("TRUST_RETENTION_DISCOVERY_CACHE", cfg.DiscoveryCache); err != nil {
		return err
	}
	if err := validateTrustRetentionHookConfig("TRUST_RETENTION_DISCOVERY_CANDIDATE", cfg.DiscoveryProjectionCandidates); err != nil {
		return err
	}
	if err := validateTrustRetentionHookConfig("TRUST_RETENTION_ENRICHMENT_STATE", cfg.LowValueEnrichmentState); err != nil {
		return err
	}
	return validateTrustRetentionHookConfig("TRUST_RETENTION_FALLBACK_METADATA", cfg.FallbackTransientMetadata)
}

func validateTrustRetentionHookConfig(prefix string, cfg TrustRetentionHookConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.TrustedHorizon <= 0 {
		return fmt.Errorf("%s_TRUSTED horizon must be > 0", prefix)
	}
	if cfg.UntrustedHorizon <= 0 {
		return fmt.Errorf("%s_UNTRUSTED horizon must be > 0", prefix)
	}
	if cfg.UntrustedHorizon > cfg.TrustedHorizon {
		return fmt.Errorf("%s_UNTRUSTED horizon must be <= %s_TRUSTED horizon", prefix, prefix)
	}
	return nil
}
