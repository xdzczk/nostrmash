package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	TrustModeOpen          = "open"
	TrustModePreferTrusted = "prefer_trusted"
	TrustModeTrustedOnly   = "trusted_only"
)

var allowedTrustModes = map[string]struct{}{
	TrustModeOpen:          {},
	TrustModePreferTrusted: {},
	TrustModeTrustedOnly:   {},
}

// TrustPolicyConfig defines explicit trust behavior knobs per runtime surface.
//
// This chunk only introduces config surfaces and validation guardrails.
// Runtime behavior remains unchanged until a follow-up chunk wires these values.
type TrustPolicyConfig struct {
	CanonicalIngestMode              string
	DiscoveryCandidateMode           string
	SearchRankingMode                string
	FallbackFetchMode                string
	FallbackFetchMaxAttempts         int
	FallbackFetchMaxTimeBudget       time.Duration
	FallbackFetchMaxRelaysPerAttempt int
	FallbackFetchAllowDirectLookup   bool
	RetentionPolicyMode              string
	RetentionHooks                   TrustRetentionHooksConfig
	MinimumScore                     float64
	SeedPubkeys                      []string
	MaxHops                          int
	RefreshInterval                  time.Duration
}

func loadTrustPolicyConfig() (TrustPolicyConfig, error) {
	minimumScore, err := getEnvNonNegativeFloat64Strict("TRUST_MINIMUM_SCORE", 0)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	refreshInterval, err := getEnvPositiveDurationStrict("TRUST_REFRESH_INTERVAL", 10*time.Minute)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	maxHops, err := getEnvPositiveIntStrict("TRUST_MAX_HOPS", 3)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	fallbackMaxAttempts, err := getEnvPositiveIntStrict("TRUST_FALLBACK_FETCH_MAX_ATTEMPTS", 1)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	fallbackMaxTimeBudget, err := getEnvPositiveDurationStrict("TRUST_FALLBACK_FETCH_MAX_TIME_BUDGET", 2*time.Second)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	fallbackMaxRelaysPerAttempt, err := getEnvPositiveIntStrict("TRUST_FALLBACK_FETCH_MAX_RELAYS_PER_ATTEMPT", 3)
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	retentionHooks, err := loadTrustRetentionHooksConfig()
	if err != nil {
		return TrustPolicyConfig{}, err
	}
	cfg := TrustPolicyConfig{
		CanonicalIngestMode:              strings.ToLower(strings.TrimSpace(getEnv("TRUST_CANONICAL_INGEST_MODE", TrustModeOpen))),
		DiscoveryCandidateMode:           strings.ToLower(strings.TrimSpace(getEnv("TRUST_DISCOVERY_CANDIDATE_MODE", TrustModeOpen))),
		SearchRankingMode:                strings.ToLower(strings.TrimSpace(getEnv("TRUST_SEARCH_RANKING_MODE", TrustModePreferTrusted))),
		FallbackFetchMode:                strings.ToLower(strings.TrimSpace(getEnv("TRUST_FALLBACK_FETCH_MODE", TrustModeOpen))),
		FallbackFetchMaxAttempts:         fallbackMaxAttempts,
		FallbackFetchMaxTimeBudget:       fallbackMaxTimeBudget,
		FallbackFetchMaxRelaysPerAttempt: fallbackMaxRelaysPerAttempt,
		FallbackFetchAllowDirectLookup:   getEnvBool("TRUST_FALLBACK_FETCH_ALLOW_DIRECT_LOOKUP", true),
		RetentionPolicyMode:              strings.ToLower(strings.TrimSpace(getEnv("TRUST_RETENTION_POLICY_MODE", TrustModeOpen))),
		RetentionHooks:                   retentionHooks,
		MinimumScore:                     minimumScore,
		SeedPubkeys:                      parseCSVEnv("TRUST_SEED_PUBKEYS"),
		MaxHops:                          maxHops,
		RefreshInterval:                  refreshInterval,
	}
	if err := validateTrustPolicyConfig(cfg); err != nil {
		return TrustPolicyConfig{}, err
	}
	return cfg, nil
}

func validateTrustPolicyConfig(cfg TrustPolicyConfig) error {
	modeFields := map[string]string{
		"TRUST_CANONICAL_INGEST_MODE":    cfg.CanonicalIngestMode,
		"TRUST_DISCOVERY_CANDIDATE_MODE": cfg.DiscoveryCandidateMode,
		"TRUST_SEARCH_RANKING_MODE":      cfg.SearchRankingMode,
		"TRUST_FALLBACK_FETCH_MODE":      cfg.FallbackFetchMode,
		"TRUST_RETENTION_POLICY_MODE":    cfg.RetentionPolicyMode,
	}
	requiresSeeds := false
	for envName, mode := range modeFields {
		if _, ok := allowedTrustModes[mode]; !ok {
			return fmt.Errorf("%s %q must be one of: open, prefer_trusted, trusted_only", envName, mode)
		}
		if mode == TrustModeTrustedOnly {
			requiresSeeds = true
		}
	}
	if cfg.MinimumScore < 0 {
		return fmt.Errorf("TRUST_MINIMUM_SCORE must be >= 0")
	}
	if cfg.MaxHops <= 0 {
		return fmt.Errorf("TRUST_MAX_HOPS must be > 0")
	}
	if cfg.RefreshInterval <= 0 {
		return fmt.Errorf("TRUST_REFRESH_INTERVAL must be > 0")
	}
	if cfg.FallbackFetchMaxAttempts <= 0 {
		return fmt.Errorf("TRUST_FALLBACK_FETCH_MAX_ATTEMPTS must be > 0")
	}
	if cfg.FallbackFetchMaxTimeBudget <= 0 {
		return fmt.Errorf("TRUST_FALLBACK_FETCH_MAX_TIME_BUDGET must be > 0")
	}
	if cfg.FallbackFetchMaxRelaysPerAttempt <= 0 {
		return fmt.Errorf("TRUST_FALLBACK_FETCH_MAX_RELAYS_PER_ATTEMPT must be > 0")
	}
	if err := validateTrustRetentionHooksConfig(cfg.RetentionHooks); err != nil {
		return err
	}
	if requiresSeeds && len(cfg.SeedPubkeys) == 0 {
		return fmt.Errorf("TRUST_SEED_PUBKEYS is required when any trust mode is trusted_only")
	}
	return nil
}

func getEnvNonNegativeFloat64Strict(key string, def float64) (float64, error) {
	raw := strings.TrimSpace(getEnv(key, ""))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative number", key)
	}
	return v, nil
}
