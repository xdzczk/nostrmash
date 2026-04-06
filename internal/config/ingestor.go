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
	Shared   SharedConfig
	Runtime  IngestorRuntimeConfig
	Relay    RelayConfig
	Backfill BackfillConfig
	Replay   ReplayConfig
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
	}

	if err := applyConfiguredFilterGroups(&cfg.Relay); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateRelayConfig(cfg.Relay); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateBackfillConfig(cfg.Backfill); err != nil {
		return IngestorConfig{}, err
	}
	if err := validateIngestorMode(cfg.Shared.ServiceName, cfg.Runtime.Mode, cfg.Replay, cfg.Relay); err != nil {
		return IngestorConfig{}, err
	}
	if strings.TrimSpace(cfg.Shared.Database.URL) == "" {
		return IngestorConfig{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}
