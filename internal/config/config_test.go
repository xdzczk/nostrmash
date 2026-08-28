package config

import (
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestValidateRelayConfig_RejectsNonTLSInTLSMode(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"ws://localhost:8080"}
	cfg.Allowlist = []string{"ws://localhost:8080"}
	cfg.RequireTLS = true
	if err := validateRelayConfig(cfg); err == nil {
		t.Fatal("expected error for ws relay when TLS is required")
	}
}

func TestValidateRelayConfig_EnforcesAllowlist(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"wss://relay.example.com"}
	cfg.Allowlist = []string{"wss://different.example.com"}
	if err := validateRelayConfig(cfg); err == nil {
		t.Fatal("expected allowlist enforcement error")
	}
}

func TestValidateBackfillConfig_RequiresValidRange(t *testing.T) {
	since := int64(20)
	until := int64(10)
	err := validateBackfillConfig(BackfillConfig{
		Enabled:        true,
		Mode:           "backfill",
		Since:          &since,
		Until:          &until,
		PageLimit:      100,
		IdleTimeout:    2 * time.Second,
		EmptyPageMax:   1,
		ConnectTimeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("expected invalid range validation error")
	}
}

func TestLoad_NonIngestorRejected(t *testing.T) {
	if _, err := Load("api"); err == nil {
		t.Fatal("expected non-ingestor Load to be rejected")
	}
}

func TestLoad_IngestorCompatibilityWrapper(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	cfg, err := Load("ingestor")
	if err != nil {
		t.Fatalf("expected Load ingestor compatibility to work: %v", err)
	}
	if cfg.DatabaseURL != "postgres://example" || cfg.Mode != "live" {
		t.Fatalf("unexpected legacy wrapper values: %#v", cfg)
	}
}

func TestResolveDatabaseMaxConns(t *testing.T) {
	cases := []struct {
		name    string
		service string
		env     int32
		url     string
		want    int32
	}{
		{name: "env wins over everything", service: "api", env: 50, url: "postgres://h/db?pool_max_conns=10", want: 50},
		{name: "dsn honored when env unset", service: "api", env: 0, url: "postgres://h/db?pool_max_conns=10", want: 0},
		{name: "api service default", service: "api", env: 0, url: "postgres://h/db", want: 32},
		{name: "worker service default", service: "worker", env: 0, url: "postgres://h/db", want: 16},
		{name: "ingestor service default", service: "ingestor", env: 0, url: "postgres://h/db", want: 8},
		{name: "trust_worker service default", service: "trust_worker", env: 0, url: "postgres://h/db", want: 8},
		{name: "unknown service fallback", service: "mystery", env: 0, url: "postgres://h/db", want: databaseMaxConnsFallbackDefault},
		{name: "keyword dsn form honored", service: "worker", env: 0, url: "host=h dbname=db pool_max_conns=5", want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveDatabaseMaxConns(tc.service, tc.env, tc.url); got != tc.want {
				t.Fatalf("resolveDatabaseMaxConns(%q, %d, %q) = %d, want %d", tc.service, tc.env, tc.url, got, tc.want)
			}
		})
	}
}

func TestLoadSharedConfig_DatabaseMaxConnsPrecedence(t *testing.T) {
	t.Run("api default applied when unset", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		cfg, err := loadSharedConfig("api")
		if err != nil {
			t.Fatalf("load shared config: %v", err)
		}
		if cfg.Database.MaxConns != 32 {
			t.Fatalf("expected api default 32, got %d", cfg.Database.MaxConns)
		}
	})
	t.Run("env overrides default", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("DATABASE_MAX_CONNS", "7")
		cfg, err := loadSharedConfig("api")
		if err != nil {
			t.Fatalf("load shared config: %v", err)
		}
		if cfg.Database.MaxConns != 7 {
			t.Fatalf("expected env override 7, got %d", cfg.Database.MaxConns)
		}
	})
	t.Run("dsn pool_max_conns defers override", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example?pool_max_conns=9")
		cfg, err := loadSharedConfig("api")
		if err != nil {
			t.Fatalf("load shared config: %v", err)
		}
		if cfg.Database.MaxConns != 0 {
			t.Fatalf("expected 0 (defer to DSN), got %d", cfg.Database.MaxConns)
		}
	})
}

func TestLoadSharedConfig_TrustPolicyDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	cfg, err := loadSharedConfig("api")
	if err != nil {
		t.Fatalf("load shared config defaults: %v", err)
	}
	if cfg.TrustPolicy.CanonicalIngestMode != TrustModeOpen {
		t.Fatalf("expected default canonical ingest mode open, got %q", cfg.TrustPolicy.CanonicalIngestMode)
	}
	if cfg.TrustPolicy.DiscoveryCandidateMode != TrustModeOpen ||
		cfg.TrustPolicy.SearchRankingMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.FallbackFetchMode != TrustModeOpen ||
		cfg.TrustPolicy.RetentionPolicyMode != TrustModeOpen {
		t.Fatalf("expected default trust policy modes with prefer_trusted search ranking, got %#v", cfg.TrustPolicy)
	}
	if cfg.TrustPolicy.MinimumScore != 0 ||
		cfg.TrustPolicy.DiscoveryScoreBoostWeight != 0 ||
		cfg.TrustPolicy.MaxHops != 3 ||
		cfg.TrustPolicy.RefreshInterval != 10*time.Minute {
		t.Fatalf("unexpected trust policy defaults: %#v", cfg.TrustPolicy)
	}
	if cfg.TrustPolicy.FallbackFetchMaxAttempts != 1 ||
		cfg.TrustPolicy.FallbackFetchMaxRelaysPerAttempt != 3 ||
		cfg.TrustPolicy.FallbackFetchMaxTimeBudget != 2*time.Second ||
		!cfg.TrustPolicy.FallbackFetchAllowDirectLookup {
		t.Fatalf("unexpected fallback trust policy defaults: %#v", cfg.TrustPolicy)
	}
	if !cfg.TrustPolicy.RetentionHooks.DiscoveryCache.Enabled ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryCache.TrustedHorizon != 10*time.Minute ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryCache.UntrustedHorizon != 2*time.Minute {
		t.Fatalf("unexpected discovery-cache retention hook defaults: %#v", cfg.TrustPolicy.RetentionHooks.DiscoveryCache)
	}
	if !cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.Enabled ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.TrustedHorizon != 24*time.Hour ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.UntrustedHorizon != 6*time.Hour {
		t.Fatalf("unexpected discovery-candidate retention hook defaults: %#v", cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates)
	}
	if cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.Enabled ||
		cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.TrustedHorizon != 12*time.Hour ||
		cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.UntrustedHorizon != 3*time.Hour {
		t.Fatalf("unexpected enrichment retention hook defaults: %#v", cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState)
	}
	if cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.Enabled ||
		cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.TrustedHorizon != 2*time.Hour ||
		cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.UntrustedHorizon != 30*time.Minute {
		t.Fatalf("unexpected fallback metadata retention hook defaults: %#v", cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata)
	}
	if len(cfg.TrustPolicy.SeedPubkeys) != 0 {
		t.Fatalf("expected empty trust seed pubkeys by default, got %#v", cfg.TrustPolicy.SeedPubkeys)
	}
}

func TestLoadSharedConfig_TrustPolicyOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_CANONICAL_INGEST_MODE", "prefer_trusted")
	t.Setenv("TRUST_DISCOVERY_CANDIDATE_MODE", "trusted_only")
	t.Setenv("TRUST_SEARCH_RANKING_MODE", "prefer_trusted")
	t.Setenv("TRUST_FALLBACK_FETCH_MODE", "trusted_only")
	t.Setenv("TRUST_RETENTION_POLICY_MODE", "prefer_trusted")
	t.Setenv("TRUST_MINIMUM_SCORE", "0.42")
	t.Setenv("TRUST_SEED_PUBKEYS", "pk1,pk2")
	t.Setenv("TRUST_MAX_HOPS", "5")
	t.Setenv("TRUST_REFRESH_INTERVAL", "30s")
	t.Setenv("TRUST_FALLBACK_FETCH_MAX_ATTEMPTS", "2")
	t.Setenv("TRUST_FALLBACK_FETCH_MAX_RELAYS_PER_ATTEMPT", "5")
	t.Setenv("TRUST_FALLBACK_FETCH_MAX_TIME_BUDGET", "3s")
	t.Setenv("TRUST_FALLBACK_FETCH_ALLOW_DIRECT_LOOKUP", "false")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CACHE_ENABLED", "false")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CACHE_TRUSTED_TTL", "8m")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CACHE_UNTRUSTED_TTL", "2m")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CANDIDATE_TRUSTED_MAX_AGE", "16h")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CANDIDATE_UNTRUSTED_MAX_AGE", "4h")
	t.Setenv("TRUST_RETENTION_ENRICHMENT_STATE_ENABLED", "true")
	t.Setenv("TRUST_RETENTION_ENRICHMENT_STATE_TRUSTED_MAX_AGE", "9h")
	t.Setenv("TRUST_RETENTION_ENRICHMENT_STATE_UNTRUSTED_MAX_AGE", "1h")
	t.Setenv("TRUST_RETENTION_FALLBACK_METADATA_ENABLED", "true")
	t.Setenv("TRUST_RETENTION_FALLBACK_METADATA_TRUSTED_MAX_AGE", "90m")
	t.Setenv("TRUST_RETENTION_FALLBACK_METADATA_UNTRUSTED_MAX_AGE", "15m")

	cfg, err := loadSharedConfig("api")
	if err != nil {
		t.Fatalf("load shared config trust overrides: %v", err)
	}
	if cfg.TrustPolicy.CanonicalIngestMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.DiscoveryCandidateMode != TrustModeTrustedOnly ||
		cfg.TrustPolicy.SearchRankingMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.FallbackFetchMode != TrustModeTrustedOnly ||
		cfg.TrustPolicy.RetentionPolicyMode != TrustModePreferTrusted {
		t.Fatalf("unexpected trust mode overrides: %#v", cfg.TrustPolicy)
	}
	if cfg.TrustPolicy.MinimumScore != 0.42 ||
		cfg.TrustPolicy.MaxHops != 5 ||
		cfg.TrustPolicy.RefreshInterval != 30*time.Second {
		t.Fatalf("unexpected trust numeric overrides: %#v", cfg.TrustPolicy)
	}
	if cfg.TrustPolicy.FallbackFetchMaxAttempts != 2 ||
		cfg.TrustPolicy.FallbackFetchMaxRelaysPerAttempt != 5 ||
		cfg.TrustPolicy.FallbackFetchMaxTimeBudget != 3*time.Second ||
		cfg.TrustPolicy.FallbackFetchAllowDirectLookup {
		t.Fatalf("unexpected fallback trust overrides: %#v", cfg.TrustPolicy)
	}
	if cfg.TrustPolicy.RetentionHooks.DiscoveryCache.Enabled ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryCache.TrustedHorizon != 8*time.Minute ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryCache.UntrustedHorizon != 2*time.Minute {
		t.Fatalf("unexpected discovery cache retention hook overrides: %#v", cfg.TrustPolicy.RetentionHooks.DiscoveryCache)
	}
	if !cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.Enabled ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.TrustedHorizon != 16*time.Hour ||
		cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates.UntrustedHorizon != 4*time.Hour {
		t.Fatalf("unexpected discovery candidate retention hook overrides: %#v", cfg.TrustPolicy.RetentionHooks.DiscoveryProjectionCandidates)
	}
	if !cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.Enabled ||
		cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.TrustedHorizon != 9*time.Hour ||
		cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState.UntrustedHorizon != time.Hour {
		t.Fatalf("unexpected enrichment retention hook overrides: %#v", cfg.TrustPolicy.RetentionHooks.LowValueEnrichmentState)
	}
	if !cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.Enabled ||
		cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.TrustedHorizon != 90*time.Minute ||
		cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata.UntrustedHorizon != 15*time.Minute {
		t.Fatalf("unexpected fallback retention hook overrides: %#v", cfg.TrustPolicy.RetentionHooks.FallbackTransientMetadata)
	}
	if len(cfg.TrustPolicy.SeedPubkeys) != 2 || cfg.TrustPolicy.SeedPubkeys[0] != "pk1" || cfg.TrustPolicy.SeedPubkeys[1] != "pk2" {
		t.Fatalf("unexpected trust seed pubkeys: %#v", cfg.TrustPolicy.SeedPubkeys)
	}
}

func TestLoadSharedConfig_TrustPolicyPresetBalanced(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_SURFACE_POLICY_PRESET", "balanced")

	cfg, err := loadSharedConfig("api")
	if err != nil {
		t.Fatalf("load shared config trust preset balanced: %v", err)
	}
	if cfg.TrustPolicy.CanonicalIngestMode != TrustModeOpen {
		t.Fatalf("expected canonical ingest mode to remain open, got %q", cfg.TrustPolicy.CanonicalIngestMode)
	}
	if cfg.TrustPolicy.DiscoveryCandidateMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.SearchRankingMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.FallbackFetchMode != TrustModePreferTrusted {
		t.Fatalf("unexpected balanced preset surface modes: %#v", cfg.TrustPolicy)
	}
}

func TestLoadSharedConfig_TrustPolicyPresetAllowsSurfaceOverride(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_SURFACE_POLICY_PRESET", "strict")
	t.Setenv("TRUST_DISCOVERY_CANDIDATE_MODE", "open")
	t.Setenv("TRUST_SEARCH_RANKING_MODE", "prefer_trusted")
	t.Setenv("TRUST_FALLBACK_FETCH_MODE", "open")

	cfg, err := loadSharedConfig("api")
	if err != nil {
		t.Fatalf("load shared config trust preset overrides: %v", err)
	}
	if cfg.TrustPolicy.DiscoveryCandidateMode != TrustModeOpen ||
		cfg.TrustPolicy.SearchRankingMode != TrustModePreferTrusted ||
		cfg.TrustPolicy.FallbackFetchMode != TrustModeOpen {
		t.Fatalf("expected explicit surface overrides to win over preset, got %#v", cfg.TrustPolicy)
	}
}

func TestLoadSharedConfig_TrustPolicyRejectsInvalidPreset(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_SURFACE_POLICY_PRESET", "paranoid")
	_, err := loadSharedConfig("api")
	if err == nil || !strings.Contains(err.Error(), "TRUST_SURFACE_POLICY_PRESET") {
		t.Fatalf("expected actionable TRUST_SURFACE_POLICY_PRESET validation error, got %v", err)
	}
}

func TestLoadSharedConfig_TrustPolicyRejectsInvalidMode(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_SEARCH_RANKING_MODE", "friends_only")
	_, err := loadSharedConfig("api")
	if err == nil || !strings.Contains(err.Error(), "TRUST_SEARCH_RANKING_MODE") {
		t.Fatalf("expected actionable TRUST_SEARCH_RANKING_MODE validation error, got %v", err)
	}
}

func TestLoadSharedConfig_TrustPolicyRejectsInvalidCombination(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_CANONICAL_INGEST_MODE", "trusted_only")
	t.Setenv("TRUST_SEED_PUBKEYS", "")
	_, err := loadSharedConfig("api")
	if err == nil || !strings.Contains(err.Error(), "TRUST_SEED_PUBKEYS") {
		t.Fatalf("expected actionable TRUST_SEED_PUBKEYS validation error, got %v", err)
	}
}

func TestLoadSharedConfig_TrustPolicyRejectsInvalidRetentionHookHorizon(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CACHE_TRUSTED_TTL", "1m")
	t.Setenv("TRUST_RETENTION_DISCOVERY_CACHE_UNTRUSTED_TTL", "10m")
	_, err := loadSharedConfig("api")
	if err == nil || !strings.Contains(err.Error(), "TRUST_RETENTION_DISCOVERY_CACHE") {
		t.Fatalf("expected actionable TRUST_RETENTION_DISCOVERY_CACHE validation error, got %v", err)
	}
}

func TestLoadAPI_DefaultsAndEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("METRICS_ADDR", "127.0.0.1:19090")
	t.Setenv("DEBUG_ADDR", "")
	t.Setenv("API_MAX_BATCH_SIZE", "")
	t.Setenv("HTTP_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_RATE_LIMIT_BURST", "")
	t.Setenv("HTTP_SEARCH_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_BATCH_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_DISCOVERY_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_SUGGEST_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_PUBLIC_STATS_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_PUBLIC_MAX_RESULT_LIMIT", "")
	t.Setenv("HTTP_PUBLIC_MAX_PAGE_SIZE", "")
	t.Setenv("HTTP_PUBLIC_MAX_PAGE_OFFSET", "")
	t.Setenv("HTTP_PUBLIC_MAX_SEARCH_WINDOW_HOURS", "")
	t.Setenv("HTTP_PUBLIC_MAX_DISCOVERY_WINDOW_HOURS", "")
	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "")
	t.Setenv("API_DISCOVERY_CACHE_ENABLED", "")
	t.Setenv("API_DISCOVERY_CACHE_MAX_ENTRIES", "")
	t.Setenv("API_DISCOVERY_CACHE_BUNDLE_TTL", "")
	t.Setenv("API_DISCOVERY_CACHE_DISCOVERY_TTL", "")
	t.Setenv("API_DISCOVERY_CACHE_SUGGESTION_TTL", "")
	t.Setenv("API_DISCOVERY_CACHE_STATS_TTL", "")
	t.Setenv("API_DISCOVERY_CACHE_TRENDING_TTL", "")
	t.Setenv("API_DISCOVERY_CACHE_PUBLIC_STATS_TTL", "")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("load api defaults: %v", err)
	}
	if cfg.HTTP.MaxBatchSize != 200 || cfg.HTTP.RateLimitRPM != 240 || cfg.HTTP.RateLimitBurst != 60 {
		t.Fatalf("unexpected api defaults: %#v", cfg.HTTP)
	}
	if cfg.HTTP.DiscoveryRateLimitRPM != 90 ||
		cfg.HTTP.SuggestRateLimitRPM != 120 ||
		cfg.HTTP.PublicStatsRateLimitRPM != 120 ||
		cfg.HTTP.PublicMaxResultLimit != 100 ||
		cfg.HTTP.PublicMaxPageSize != 100 ||
		cfg.HTTP.PublicMaxPageOffset != 5000 ||
		cfg.HTTP.PublicMaxSearchWindowHrs != 7*24 ||
		cfg.HTTP.PublicMaxDiscoveryWindowHrs != 30*24 {
		t.Fatalf("unexpected api public guard defaults: %#v", cfg.HTTP)
	}
	if cfg.PrimalWS.MaxSubscriptions != 200 || cfg.PrimalWS.MaxReqPerMinute != 240 {
		t.Fatalf("unexpected ws defaults: %#v", cfg.PrimalWS)
	}
	if cfg.Shared.Observability.DebugAddr != "" {
		t.Fatalf("expected empty debug addr by default, got %q", cfg.Shared.Observability.DebugAddr)
	}
	if cfg.Shared.Observability.MetricsAddr != "" {
		t.Fatalf("expected API metrics addr to be ignored, got %q", cfg.Shared.Observability.MetricsAddr)
	}
	if !cfg.DiscoveryCache.Enabled || cfg.DiscoveryCache.MaxEntries != 256 ||
		cfg.DiscoveryCache.BundleTTL != 60*time.Second ||
		cfg.DiscoveryCache.DiscoveryTTL != 60*time.Second ||
		cfg.DiscoveryCache.SuggestionTTL != 60*time.Second ||
		cfg.DiscoveryCache.StatsTTL != 10*time.Minute ||
		cfg.DiscoveryCache.TrendingTTL != 60*time.Second ||
		cfg.DiscoveryCache.PublicStatsTTL != 10*time.Minute {
		t.Fatalf("unexpected discovery cache defaults: %#v", cfg.DiscoveryCache)
	}

	t.Setenv("API_MAX_BATCH_SIZE", "75")
	t.Setenv("HTTP_DISCOVERY_RATE_LIMIT_RPM", "88")
	t.Setenv("HTTP_SUGGEST_RATE_LIMIT_RPM", "77")
	t.Setenv("HTTP_PUBLIC_STATS_RATE_LIMIT_RPM", "66")
	t.Setenv("HTTP_PUBLIC_MAX_RESULT_LIMIT", "55")
	t.Setenv("HTTP_PUBLIC_MAX_PAGE_SIZE", "44")
	t.Setenv("HTTP_PUBLIC_MAX_PAGE_OFFSET", "3333")
	t.Setenv("HTTP_PUBLIC_MAX_SEARCH_WINDOW_HOURS", "72")
	t.Setenv("HTTP_PUBLIC_MAX_DISCOVERY_WINDOW_HOURS", "240")
	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "50")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "2s")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "2048")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "33")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "11")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "https://app.primal.net,https://nostrmash.local")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "true")
	t.Setenv("DEBUG_ADDR", "127.0.0.1:6060")
	t.Setenv("API_DISCOVERY_CACHE_ENABLED", "false")
	t.Setenv("API_DISCOVERY_CACHE_MAX_ENTRIES", "120")
	t.Setenv("API_DISCOVERY_CACHE_BUNDLE_TTL", "20s")
	t.Setenv("API_DISCOVERY_CACHE_DISCOVERY_TTL", "30s")
	t.Setenv("API_DISCOVERY_CACHE_SUGGESTION_TTL", "15s")
	t.Setenv("API_DISCOVERY_CACHE_STATS_TTL", "5m")
	t.Setenv("API_DISCOVERY_CACHE_TRENDING_TTL", "45s")
	t.Setenv("API_DISCOVERY_CACHE_PUBLIC_STATS_TTL", "7m")

	cfg, err = LoadAPI()
	if err != nil {
		t.Fatalf("load api env: %v", err)
	}
	if cfg.HTTP.MaxBatchSize != 75 {
		t.Fatalf("unexpected max batch size: %d", cfg.HTTP.MaxBatchSize)
	}
	if cfg.HTTP.DiscoveryRateLimitRPM != 88 ||
		cfg.HTTP.SuggestRateLimitRPM != 77 ||
		cfg.HTTP.PublicStatsRateLimitRPM != 66 ||
		cfg.HTTP.PublicMaxResultLimit != 55 ||
		cfg.HTTP.PublicMaxPageSize != 44 ||
		cfg.HTTP.PublicMaxPageOffset != 3333 ||
		cfg.HTTP.PublicMaxSearchWindowHrs != 72 ||
		cfg.HTTP.PublicMaxDiscoveryWindowHrs != 240 {
		t.Fatalf("unexpected api public guard env config: %#v", cfg.HTTP)
	}
	if cfg.PrimalWS.MaxSubscriptions != 50 || cfg.PrimalWS.MaxMessageBytes != 2048 || cfg.PrimalWS.MaxReqPerMinute != 33 {
		t.Fatalf("unexpected ws env config: %#v", cfg.PrimalWS)
	}
	if cfg.Shared.Observability.DebugAddr != "127.0.0.1:6060" {
		t.Fatalf("unexpected debug addr: %q", cfg.Shared.Observability.DebugAddr)
	}
	if cfg.DiscoveryCache.Enabled ||
		cfg.DiscoveryCache.MaxEntries != 120 ||
		cfg.DiscoveryCache.BundleTTL != 20*time.Second ||
		cfg.DiscoveryCache.DiscoveryTTL != 30*time.Second ||
		cfg.DiscoveryCache.SuggestionTTL != 15*time.Second ||
		cfg.DiscoveryCache.StatsTTL != 5*time.Minute ||
		cfg.DiscoveryCache.TrendingTTL != 45*time.Second ||
		cfg.DiscoveryCache.PublicStatsTTL != 7*time.Minute {
		t.Fatalf("unexpected discovery cache env config: %#v", cfg.DiscoveryCache)
	}
}

func TestLoadAPI_InvalidEnvIsActionable(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_MAX_BATCH_SIZE", "nope")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "API_MAX_BATCH_SIZE") {
		t.Fatalf("expected actionable API_MAX_BATCH_SIZE error, got %v", err)
	}
}

func TestLoadAPI_InvalidAllowedOriginFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "ws://bad-origin")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "PRIMAL_WS_ALLOWED_ORIGINS") {
		t.Fatalf("expected PRIMAL_WS_ALLOWED_ORIGINS validation error, got %v", err)
	}
}

func TestLoadAPI_InvalidDurationFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "bad")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "PRIMAL_WS_REQUEST_TIMEOUT") {
		t.Fatalf("expected PRIMAL_WS_REQUEST_TIMEOUT validation error, got %v", err)
	}
}

func TestLoadAPI_FallbackRelayURLsNormalizeAndDeduplicate(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_RELAY_FALLBACK_ENABLED", "true")
	t.Setenv("API_RELAY_FALLBACK_URLS", "WSS://Relay.Example.com/,wss://relay.example.com")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("load api fallback relay urls: %v", err)
	}
	if len(cfg.RelayFallback.URLs) != 1 || cfg.RelayFallback.URLs[0] != "wss://relay.example.com" {
		t.Fatalf("unexpected normalized fallback urls: %#v", cfg.RelayFallback.URLs)
	}
	if len(cfg.RelayFallback.ProfileURLs) != 2 ||
		cfg.RelayFallback.ProfileURLs[0] != "wss://purplepag.es" ||
		cfg.RelayFallback.ProfileURLs[1] != "wss://user.kindpag.es" {
		t.Fatalf("expected default profile fallback urls, got %#v", cfg.RelayFallback.ProfileURLs)
	}
	if !cfg.RelayFallback.UseRegistry {
		t.Fatalf("expected registry-backed event fallback to default on")
	}
	if cfg.RelayFallback.RefreshInterval != 5*time.Minute {
		t.Fatalf("expected default refresh interval 5m, got %s", cfg.RelayFallback.RefreshInterval)
	}
}

func TestLoadAPI_FallbackProfileURLsOverrideDefault(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_RELAY_FALLBACK_ENABLED", "true")
	t.Setenv("API_RELAY_FALLBACK_URLS", "wss://nos.lol")
	t.Setenv("API_RELAY_FALLBACK_PROFILE_URLS", "wss://directory.example,wss://purplepag.es")
	t.Setenv("API_RELAY_FALLBACK_USE_REGISTRY", "false")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("load api profile fallback urls: %v", err)
	}
	if cfg.RelayFallback.UseRegistry {
		t.Fatal("expected registry-backed event fallback to honor explicit false")
	}
	want := []string{"wss://directory.example", "wss://purplepag.es"}
	if !reflect.DeepEqual(cfg.RelayFallback.ProfileURLs, want) {
		t.Fatalf("profile urls: got %#v want %#v", cfg.RelayFallback.ProfileURLs, want)
	}
}

func TestLoadAPI_FallbackRelayInvalidURLFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_RELAY_FALLBACK_ENABLED", "true")
	t.Setenv("API_RELAY_FALLBACK_URLS", "https://relay.example.com")

	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "invalid fallback relay URL") {
		t.Fatalf("expected invalid fallback relay URL validation error, got %v", err)
	}
}

func TestLoadWorker_DefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT", "")
	t.Setenv("DEBUG_ADDR", "")
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatalf("load worker defaults: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Fatalf("unexpected worker default concurrency: %d", cfg.Concurrency)
	}
	if cfg.JobRecovery.RunningTimeout != 15*time.Minute {
		t.Fatalf("unexpected worker stale running timeout: %s", cfg.JobRecovery.RunningTimeout)
	}
	if cfg.JobRecovery.StaleRecoveryInterval != 30*time.Second {
		t.Fatalf("unexpected worker stale recovery interval: %s", cfg.JobRecovery.StaleRecoveryInterval)
	}
	if cfg.JobRecovery.StaleRecoveryBatchLimit != 100 {
		t.Fatalf("unexpected worker stale recovery batch limit: %d", cfg.JobRecovery.StaleRecoveryBatchLimit)
	}
	if !cfg.JobRetention.Enabled {
		t.Fatalf("expected job retention enabled by default")
	}
	if cfg.JobRetention.SucceededMaxAge != 6*time.Hour {
		t.Fatalf("unexpected default retention succeeded max age: %s", cfg.JobRetention.SucceededMaxAge)
	}
	if cfg.JobRetention.DeadMaxAge != 7*24*time.Hour {
		t.Fatalf("unexpected default retention dead max age: %s", cfg.JobRetention.DeadMaxAge)
	}
	if cfg.JobRetention.RunInterval != 15*time.Minute {
		t.Fatalf("unexpected default retention run interval: %s", cfg.JobRetention.RunInterval)
	}
	if cfg.JobRetention.DeleteBatchLimit != 2000 {
		t.Fatalf("unexpected default retention batch limit: %d", cfg.JobRetention.DeleteBatchLimit)
	}
	if !cfg.InvalidEventRetention.Enabled {
		t.Fatalf("expected invalid event retention enabled by default")
	}
	if cfg.InvalidEventRetention.MaxAge != 30*24*time.Hour {
		t.Fatalf("unexpected invalid event retention max age: %s", cfg.InvalidEventRetention.MaxAge)
	}
	if !cfg.InvalidEventRetention.PayloadTrim.Enabled {
		t.Fatalf("expected invalid event payload trim enabled by default")
	}
	if cfg.InvalidEventRetention.PayloadTrim.MaxAge != 7*24*time.Hour {
		t.Fatalf("unexpected invalid event payload trim max age: %s", cfg.InvalidEventRetention.PayloadTrim.MaxAge)
	}

	t.Setenv("WORKER_CONCURRENCY", "8")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker env: %v", err)
	}
	if cfg.Concurrency != 8 {
		t.Fatalf("unexpected worker env concurrency: %d", cfg.Concurrency)
	}
	t.Setenv("WORKER_JOB_RETENTION_ENABLED", "false")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker retention disabled: %v", err)
	}
	if cfg.JobRetention.Enabled {
		t.Fatalf("expected retention to be disabled by env")
	}
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "false")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker invalid retention disabled: %v", err)
	}
	if cfg.InvalidEventRetention.Enabled {
		t.Fatalf("expected invalid event retention to be disabled by env")
	}

	t.Setenv("WORKER_CONCURRENCY", "0")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_CONCURRENCY") {
		t.Fatalf("expected actionable WORKER_CONCURRENCY error, got %v", err)
	}

	t.Setenv("WORKER_CONCURRENCY", "4")
	t.Setenv("WORKER_JOB_RUNNING_TIMEOUT", "0s")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_JOB_RUNNING_TIMEOUT") {
		t.Fatalf("expected actionable WORKER_JOB_RUNNING_TIMEOUT error, got %v", err)
	}
}

func TestLoadWorker_InvalidInvalidEventsRetentionConfigFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "0s")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_INVALID_EVENTS_RETENTION_MAX_AGE") {
		t.Fatalf("expected actionable WORKER_INVALID_EVENTS_RETENTION_MAX_AGE error, got %v", err)
	}
}

func TestLoadWorker_InvalidInvalidEventsPayloadTrimConfigFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE", "720h")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "720h")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE") {
		t.Fatalf("expected actionable WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE error, got %v", err)
	}
}

func TestLoadTrustWorker_DefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("TRUST_WORKER_CONCURRENCY", "")
	t.Setenv("TRUST_WORKER_CLAIM_BATCH_SIZE", "")
	t.Setenv("TRUST_WORKER_POLL_INTERVAL", "")
	t.Setenv("TRUST_WORKER_RETRY_DELAY", "")
	t.Setenv("TRUST_ENABLE_REDIS_SYNC", "")
	t.Setenv("TRUST_ENABLE_SCORE_COMPUTE", "")

	cfg, err := LoadTrustWorker()
	if err != nil {
		t.Fatalf("load trust worker defaults: %v", err)
	}
	if cfg.Concurrency != 2 || cfg.ClaimBatchSize != 5 {
		t.Fatalf("unexpected trust worker defaults: %#v", cfg)
	}
	if cfg.JobRecovery.RunningTimeout != 15*time.Minute {
		t.Fatalf("unexpected trust worker stale running timeout: %s", cfg.JobRecovery.RunningTimeout)
	}
	if cfg.JobRecovery.StaleRecoveryInterval != 30*time.Second {
		t.Fatalf("unexpected trust worker stale recovery interval: %s", cfg.JobRecovery.StaleRecoveryInterval)
	}
	if cfg.JobRecovery.StaleRecoveryBatchLimit != 100 {
		t.Fatalf("unexpected trust worker stale recovery batch limit: %d", cfg.JobRecovery.StaleRecoveryBatchLimit)
	}
	if !cfg.JobRetention.Enabled {
		t.Fatalf("expected trust worker job retention enabled by default")
	}
	if cfg.JobRetention.SucceededMaxAge != 6*time.Hour {
		t.Fatalf("unexpected trust worker retention succeeded max age: %s", cfg.JobRetention.SucceededMaxAge)
	}
	if cfg.JobRetention.DeadMaxAge != 7*24*time.Hour {
		t.Fatalf("unexpected trust worker retention dead max age: %s", cfg.JobRetention.DeadMaxAge)
	}
	if cfg.Redis.URL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected redis url: %q", cfg.Redis.URL)
	}
	if cfg.Redis.KeyPrefix != "nostrmash" {
		t.Fatalf("unexpected redis key prefix: %q", cfg.Redis.KeyPrefix)
	}
	if cfg.EnableNeighborhoods {
		t.Fatalf("expected TRUST_ENABLE_NEIGHBORHOODS default false")
	}
	if cfg.NeighborhoodMaxMembers != 5000 {
		t.Fatalf("unexpected neighborhood max members default: %d", cfg.NeighborhoodMaxMembers)
	}

	t.Setenv("TRUST_WORKER_CONCURRENCY", "0")
	if _, err := LoadTrustWorker(); err == nil || !strings.Contains(err.Error(), "TRUST_WORKER_CONCURRENCY") {
		t.Fatalf("expected actionable TRUST_WORKER_CONCURRENCY error, got %v", err)
	}

	t.Setenv("TRUST_WORKER_CONCURRENCY", "2")
	t.Setenv("WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT", "0")
	if _, err := LoadTrustWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT") {
		t.Fatalf("expected actionable WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT error, got %v", err)
	}
}

func TestLoadTrustWorker_InvalidSharedObservabilityAddrsFail(t *testing.T) {
	t.Run("invalid metrics addr", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
		t.Setenv("METRICS_ADDR", "not-a-valid-addr")
		_, err := LoadTrustWorker()
		if err == nil || !strings.Contains(err.Error(), "METRICS_ADDR") {
			t.Fatalf("expected actionable METRICS_ADDR validation error, got %v", err)
		}
	})

	t.Run("invalid debug addr", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
		t.Setenv("DEBUG_ADDR", "not-a-valid-addr")
		_, err := LoadTrustWorker()
		if err == nil || !strings.Contains(err.Error(), "DEBUG_ADDR") {
			t.Fatalf("expected actionable DEBUG_ADDR validation error, got %v", err)
		}
	})
}

func TestLoadTrustWorker_TrustSpecificGuardrailsFail(t *testing.T) {
	t.Run("requires at least one trust phase", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
		t.Setenv("TRUST_ENABLE_REDIS_SYNC", "false")
		t.Setenv("TRUST_ENABLE_SCORE_COMPUTE", "false")
		_, err := LoadTrustWorker()
		if err == nil || !strings.Contains(err.Error(), "at least one of TRUST_ENABLE_REDIS_SYNC or TRUST_ENABLE_SCORE_COMPUTE must be true") {
			t.Fatalf("expected trust phase validation error, got %v", err)
		}
	})

	t.Run("compute only without redis url is allowed", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", " ")
		t.Setenv("TRUST_ENABLE_REDIS_SYNC", "false")
		t.Setenv("TRUST_ENABLE_SCORE_COMPUTE", "true")
		if _, err := LoadTrustWorker(); err != nil {
			t.Fatalf("expected compute-only mode without redis url to pass, got %v", err)
		}
	})

	t.Run("redis sync mode requires redis url", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_ENABLE_REDIS_SYNC", "true")
		t.Setenv("TRUST_ENABLE_SCORE_COMPUTE", "true")
		t.Setenv("TRUST_REDIS_URL", " ")
		_, err := LoadTrustWorker()
		if err == nil || !strings.Contains(err.Error(), "TRUST_REDIS_URL") {
			t.Fatalf("expected actionable TRUST_REDIS_URL validation error, got %v", err)
		}
	})

	t.Run("blank redis key prefix falls back to default", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
		t.Setenv("TRUST_REDIS_KEY_PREFIX", " ")
		cfg, err := LoadTrustWorker()
		if err != nil {
			t.Fatalf("expected blank key prefix to use default, got %v", err)
		}
		if cfg.Redis.KeyPrefix != "nostrmash" {
			t.Fatalf("expected blank key prefix to fall back to default, got %q", cfg.Redis.KeyPrefix)
		}
	})

	t.Run("invalid poll interval", func(t *testing.T) {
		t.Setenv("DATABASE_URL", "postgres://example")
		t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
		t.Setenv("TRUST_WORKER_POLL_INTERVAL", "bad")
		_, err := LoadTrustWorker()
		if err == nil || !strings.Contains(err.Error(), "TRUST_WORKER_POLL_INTERVAL") {
			t.Fatalf("expected actionable TRUST_WORKER_POLL_INTERVAL validation error, got %v", err)
		}
	})
}

func TestLoadAPI_InvalidDebugAddrFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("DEBUG_ADDR", "not-a-valid-addr")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "DEBUG_ADDR") {
		t.Fatalf("expected actionable DEBUG_ADDR validation error, got %v", err)
	}
}

func TestValidateIngestorMode_ReplayRequiresFixturePath(t *testing.T) {
	if err := validateIngestorMode("ingestor", "replay", ReplayConfig{}, testRelayConfig(), false); err == nil {
		t.Fatal("expected replay mode to require fixture path")
	}
}

func TestLoad_LiveResumeDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS", "")
	t.Setenv("INGESTOR_LIVE_RESUME_OVERLAP_SECONDS", "")

	cfg, err := LoadIngestor()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Shared.Database.URL != "postgres://example" {
		t.Fatalf("expected shared database url, got %q", cfg.Shared.Database.URL)
	}
	if cfg.Runtime.Mode != "live" {
		t.Fatalf("expected runtime mode live, got %q", cfg.Runtime.Mode)
	}
	if cfg.Relay.LiveBootstrapLookbackSeconds != 300 || cfg.Relay.LiveResumeOverlapSeconds != 60 {
		t.Fatalf("unexpected live resume defaults: %#v", cfg.Relay)
	}
	if !cfg.TrustPrioritization.Enabled || cfg.TrustPrioritization.TopPubkeys != 2000 {
		t.Fatalf("unexpected ingest trust prioritization defaults: %#v", cfg.TrustPrioritization)
	}
	if cfg.TrustFetch.Enabled {
		t.Fatalf("expected trust fetch disabled by default, got %#v", cfg.TrustFetch)
	}
}

func TestLoad_RejectsInvalidLiveResumeConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS", "abc")
	if _, err := LoadIngestor(); err == nil {
		t.Fatal("expected invalid bootstrap lookback to fail")
	}
}

func TestLoadIngestor_TrustPrioritizationOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_TRUST_PRIORITIZATION_ENABLED", "true")
	t.Setenv("INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS", "123")
	t.Setenv("INGESTOR_TRUST_FETCH_ENABLED", "true")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS", "1111")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE", "17")
	t.Setenv("INGESTOR_TRUST_FETCH_REFRESH_INTERVAL", "45s")
	t.Setenv("INGESTOR_TRUST_FETCH_COOLDOWN", "20m")
	t.Setenv("INGESTOR_TRUST_FETCH_STABLE_WINDOW", "8m")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE", "12")
	t.Setenv("INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS", "7200")
	t.Setenv("INGESTOR_TRUST_FETCH_PAGE_LIMIT_PER_RELAY", "77")
	t.Setenv("INGESTOR_TRUST_FETCH_RETRY_DELAY", "3m")

	cfg, err := LoadIngestor()
	if err != nil {
		t.Fatalf("load ingestor trust prioritization overrides: %v", err)
	}
	if !cfg.TrustPrioritization.Enabled || cfg.TrustPrioritization.TopPubkeys != 123 {
		t.Fatalf("unexpected trust prioritization config: %#v", cfg.TrustPrioritization)
	}
	if !cfg.TrustFetch.Enabled ||
		cfg.TrustFetch.MaxTrackedPubkeys != 1111 ||
		cfg.TrustFetch.MaxSelectedPerCycle != 17 ||
		cfg.TrustFetch.RefreshInterval != 45*time.Second ||
		cfg.TrustFetch.FetchCooldown != 20*time.Minute ||
		cfg.TrustFetch.StableWindow != 8*time.Minute ||
		cfg.TrustFetch.MaxPromotionsPerCycle != 12 ||
		cfg.TrustFetch.RecentLookbackSeconds != 7200 ||
		cfg.TrustFetch.PageLimitPerRelay != 77 ||
		cfg.TrustFetch.RetryDelay != 3*time.Minute {
		t.Fatalf("unexpected trust fetch config: %#v", cfg.TrustFetch)
	}
}

func TestConfigEnvDocs_BasicCoverageAndUniqueness(t *testing.T) {
	docs := ConfigEnvDocs()
	if len(docs) == 0 {
		t.Fatal("expected config env docs to be non-empty")
	}

	seen := make(map[string]struct{}, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.Name) == "" {
			t.Fatal("env doc name must not be empty")
		}
		if _, ok := seen[doc.Name]; ok {
			t.Fatalf("duplicate env doc name: %s", doc.Name)
		}
		seen[doc.Name] = struct{}{}
		if len(doc.Runtimes) == 0 {
			t.Fatalf("env doc %s must list at least one runtime", doc.Name)
		}
		if strings.TrimSpace(doc.Description) == "" {
			t.Fatalf("env doc %s must include a description", doc.Name)
		}
	}

	for _, want := range []string{
		"DATABASE_URL",
		"INGESTOR_MODE",
		"HTTP_ADDR",
		"WORKER_CONCURRENCY",
	} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("expected env doc entry for %s", want)
		}
	}
}

func TestGenerateConfigurationMarkdown_ContainsExpectedTableRows(t *testing.T) {
	content := GenerateConfigurationMarkdown()
	if !strings.Contains(content, "# Configuration") {
		t.Fatal("expected configuration heading")
	}
	if !strings.Contains(content, "| Env var | Runtime(s) | Required | Default | Description |") {
		t.Fatal("expected markdown table header")
	}
	if !strings.Contains(content, "`DATABASE_URL`") {
		t.Fatal("expected DATABASE_URL row")
	}
	if !strings.Contains(content, "`WORKER_CONCURRENCY`") {
		t.Fatal("expected WORKER_CONCURRENCY row")
	}
}

func TestGenerateConfigurationMarkdown_MatchesCommittedDocumentation(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	docPath := filepath.Join(root, "docs", "configuration.md")

	committed, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	generated := []byte(GenerateConfigurationMarkdown())
	if string(committed) != string(generated) {
		t.Fatalf("configuration docs are out of date; run: go run ./cmd/configdoc -out %s", docPath)
	}
}

func testRelayConfig() RelayConfig {
	return RelayConfig{
		FilterGroups: map[string]FilterGroup{
			"default_v1": {Kinds: []int{0, 1, 3, 4, 5, 6, 7, 9735, 9802, 10000, 10002, 10003, 30023}},
		},
		ActiveFilterGroup:            "default_v1",
		LiveBootstrapLookbackSeconds: 300,
		LiveResumeOverlapSeconds:     60,
	}
}
