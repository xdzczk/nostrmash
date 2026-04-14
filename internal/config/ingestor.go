package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// IngestorRuntimeConfig owns ingestor-only runtime switches.
type IngestorRuntimeConfig struct {
	Mode string
}

// IngestorConfig owns ingestor runtime configuration plus shared settings.
type IngestorConfig struct {
	Shared                  SharedConfig
	Runtime                 IngestorRuntimeConfig
	Relay                   RelayConfig
	Backfill                BackfillConfig
	Replay                  ReplayConfig
	TrustPrioritization     IngestorTrustPrioritizationConfig
	TrustFetch              IngestorTrustFetchConfig
	AuthorMetadataDiscovery IngestorAuthorMetadataDiscoveryConfig
	RelayRegistry           RelayRegistryConfig
}

// IngestorAuthorMetadataDiscoveryConfig controls the background loop that
// fetches kind-0 metadata from relays for note authors whose metadata was
// never ingested.
type IngestorAuthorMetadataDiscoveryConfig struct {
	Enabled           bool
	BatchSize         int
	Interval          time.Duration
	PageLimitPerRelay int
}

type IngestorTrustPrioritizationConfig struct {
	Enabled    bool
	TopPubkeys int
}

type IngestorTrustFetchConfig struct {
	Enabled               bool
	MaxTrackedPubkeys     int
	MaxSelectedPerCycle   int
	RefreshInterval       time.Duration
	FetchCooldown         time.Duration
	StableWindow          time.Duration
	MaxPromotionsPerCycle int
	RecentLookbackSeconds int64
	PageLimitPerRelay     int
	RetryDelay            time.Duration
}

// LoadIngestor reads ingestor configuration from environment variables.
func LoadIngestor() (IngestorConfig, error) {
	shared, err := loadSharedConfig("ingestor")
	if err != nil {
		return IngestorConfig{}, err
	}

	localDev := isLocalDevEnvironment(shared.Environment)
	requireTLS := getEnvBool("INGESTOR_RELAY_REQUIRE_TLS", !localDev)
	liveBootstrapLookbackSeconds, err := getEnvNonNegativeInt64(
		"INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS",
		300,
	)
	if err != nil {
		return IngestorConfig{}, err
	}
	liveResumeOverlapSeconds, err := getEnvNonNegativeInt64(
		"INGESTOR_LIVE_RESUME_OVERLAP_SECONDS",
		60,
	)
	if err != nil {
		return IngestorConfig{}, err
	}

	cfg := IngestorConfig{
		Shared: shared,
		Runtime: IngestorRuntimeConfig{
			Mode: strings.ToLower(strings.TrimSpace(getEnv("INGESTOR_MODE", "live"))),
		},
		Relay: RelayConfig{
			URLs:           parseCSVEnv("INGESTOR_RELAY_URLS"),
			Allowlist:      parseCSVEnv("INGESTOR_RELAY_ALLOWLIST"),
			Disabled:       parseCSVEnv("INGESTOR_RELAY_DISABLED"),
			RequireTLS:     requireTLS,
			ConnectTimeout: getEnvDuration("INGESTOR_RELAY_CONNECT_TIMEOUT", 10*time.Second),
			InitialBackoff: getEnvDuration("INGESTOR_RELAY_BACKOFF_INITIAL", 1*time.Second),
			MaxBackoff:     getEnvDuration("INGESTOR_RELAY_BACKOFF_MAX", 30*time.Second),
			LagThreshold:   getEnvDuration("INGESTOR_RELAY_LAG_THRESHOLD", 45*time.Second),
			FilterGroups:   defaultFilterGroups(),
			ActiveFilterGroup: strings.TrimSpace(
				getEnv("INGESTOR_FILTER_GROUP", defaultFilterGroupName),
			),
			LiveBootstrapLookbackSeconds: liveBootstrapLookbackSeconds,
			LiveResumeOverlapSeconds:     liveResumeOverlapSeconds,
		},
		Backfill: BackfillConfig{
			Enabled:        getEnvBool("INGESTOR_BACKFILL_ENABLED", false),
			Mode:           strings.TrimSpace(getEnv("INGESTOR_BACKFILL_MODE", "backfill")),
			Since:          getEnvOptionalUnix("INGESTOR_BACKFILL_SINCE"),
			Until:          getEnvOptionalUnix("INGESTOR_BACKFILL_UNTIL"),
			PageLimit:      getEnvInt("INGESTOR_BACKFILL_PAGE_LIMIT", 500),
			IdleTimeout:    getEnvDuration("INGESTOR_BACKFILL_IDLE_TIMEOUT", 3*time.Second),
			EmptyPageMax:   getEnvInt("INGESTOR_BACKFILL_EMPTY_PAGE_MAX", 2),
			ConnectTimeout: getEnvDuration("INGESTOR_BACKFILL_CONNECT_TIMEOUT", 10*time.Second),
		},
		Replay: ReplayConfig{
			FixturePath: strings.TrimSpace(os.Getenv("INGESTOR_REPLAY_FIXTURE_PATH")),
		},
		TrustPrioritization: IngestorTrustPrioritizationConfig{
			Enabled:    getEnvBool("INGESTOR_TRUST_PRIORITIZATION_ENABLED", true),
			TopPubkeys: getEnvInt("INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS", 2000),
		},
		TrustFetch: IngestorTrustFetchConfig{
			Enabled:               getEnvBool("INGESTOR_TRUST_FETCH_ENABLED", false),
			MaxTrackedPubkeys:     getEnvInt("INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS", 5000),
			MaxSelectedPerCycle:   getEnvInt("INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE", 100),
			RefreshInterval:       getEnvDuration("INGESTOR_TRUST_FETCH_REFRESH_INTERVAL", 2*time.Minute),
			FetchCooldown:         getEnvDuration("INGESTOR_TRUST_FETCH_COOLDOWN", 30*time.Minute),
			StableWindow:          getEnvDuration("INGESTOR_TRUST_FETCH_STABLE_WINDOW", 10*time.Minute),
			MaxPromotionsPerCycle: getEnvInt("INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE", 50),
			RecentLookbackSeconds: getEnvInt64("INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS", 86400),
			PageLimitPerRelay:     getEnvInt("INGESTOR_TRUST_FETCH_PAGE_LIMIT_PER_RELAY", 200),
			RetryDelay:            getEnvDuration("INGESTOR_TRUST_FETCH_RETRY_DELAY", 10*time.Minute),
		},
		AuthorMetadataDiscovery: IngestorAuthorMetadataDiscoveryConfig{
			Enabled:           getEnvBool("INGESTOR_AUTHOR_METADATA_DISCOVERY_ENABLED", true),
			BatchSize:         getEnvInt("INGESTOR_AUTHOR_METADATA_DISCOVERY_BATCH_SIZE", 50),
			Interval:          getEnvDuration("INGESTOR_AUTHOR_METADATA_DISCOVERY_INTERVAL", 3*time.Minute),
			PageLimitPerRelay: getEnvInt("INGESTOR_AUTHOR_METADATA_DISCOVERY_PAGE_LIMIT", 10),
		},
	}

	relayRegistryCfg, err := LoadRelayRegistryConfig()
	if err != nil {
		return IngestorConfig{}, err
	}
	cfg.RelayRegistry = relayRegistryCfg

	if err := applyConfiguredFilterGroups(&cfg.Relay); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateRelayConfig(cfg.Relay); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateBackfillConfig(cfg.Backfill); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateIngestorMode(cfg.Shared.ServiceName, cfg.Runtime.Mode, cfg.Replay, cfg.Relay, cfg.RelayRegistry.Enabled); err != nil {
		return IngestorConfig{}, err
	}
	if strings.TrimSpace(cfg.Shared.Database.URL) == "" {
		return IngestorConfig{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.TrustPrioritization.Enabled && cfg.TrustPrioritization.TopPubkeys <= 0 {
		return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS must be > 0 when trust prioritization is enabled")
	}
	if cfg.TrustFetch.Enabled {
		if cfg.TrustFetch.MaxTrackedPubkeys <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS must be > 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.MaxSelectedPerCycle <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE must be > 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.RefreshInterval <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_REFRESH_INTERVAL must be > 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.FetchCooldown < 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_COOLDOWN must be >= 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.StableWindow < 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_STABLE_WINDOW must be >= 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.MaxPromotionsPerCycle <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE must be > 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.RecentLookbackSeconds < 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS must be >= 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.PageLimitPerRelay <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_PAGE_LIMIT_PER_RELAY must be > 0 when trust fetch is enabled")
		}
		if cfg.TrustFetch.RetryDelay < 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_TRUST_FETCH_RETRY_DELAY must be >= 0 when trust fetch is enabled")
		}
	}
	if cfg.AuthorMetadataDiscovery.Enabled {
		if cfg.AuthorMetadataDiscovery.BatchSize <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_AUTHOR_METADATA_DISCOVERY_BATCH_SIZE must be > 0 when author metadata discovery is enabled")
		}
		if cfg.AuthorMetadataDiscovery.Interval <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_AUTHOR_METADATA_DISCOVERY_INTERVAL must be > 0 when author metadata discovery is enabled")
		}
		if cfg.AuthorMetadataDiscovery.PageLimitPerRelay <= 0 {
			return IngestorConfig{}, fmt.Errorf("INGESTOR_AUTHOR_METADATA_DISCOVERY_PAGE_LIMIT must be > 0 when author metadata discovery is enabled")
		}
	}
	return cfg, nil
}
