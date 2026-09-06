package config

import (
	"fmt"
	"time"
)

// RelayRegistryConfig controls the relay registry subsystem.
type RelayRegistryConfig struct {
	Enabled             bool
	SeedRelays          []string
	AllowPrivateNetwork bool

	Discovery RelayRegistryDiscoveryConfig
	Probing   RelayRegistryProbingConfig
	Admission RelayRegistryAdmissionConfig
	Retention RelayRegistryRetentionConfig
	Reconcile RelayRegistryReconcileConfig

	RefreshInterval time.Duration
}

// RelayRegistryReconcileConfig controls ingestor desired-set polling and reconciliation.
type RelayRegistryReconcileConfig struct {
	PollInterval time.Duration
	DrainTimeout time.Duration
}

// RelayRegistryDiscoveryConfig controls relay candidate discovery from user relay lists.
type RelayRegistryDiscoveryConfig struct {
	Enabled             bool
	MinDistinctUserRefs int
	// MaxNewCandidatesPerRun limits brand-new registry inserts only.
	// Existing relays are always refreshed with current user-ref counts.
	MaxNewCandidatesPerRun int
	// MaxVariantsPerHost caps how many registry entries may share one
	// hostname before discovery stops admitting new URL variants of that
	// host. User relay lists are full of junk path variants
	// (wss://host/random-words) that all resolve to the same relay, probe
	// successfully, and pollute the candidate pool; distinct real relays
	// virtually always live on distinct hostnames.
	MaxVariantsPerHost int
}

// RelayRegistryProbingConfig controls local health probing of relay candidates.
type RelayRegistryProbingConfig struct {
	Enabled        bool
	Interval       time.Duration
	TimeoutConnect time.Duration
	TimeoutEOSE    time.Duration
	MaxParallel    int
}

// RelayRegistryAdmissionConfig controls scoring thresholds and dynamic set caps.
type RelayRegistryAdmissionConfig struct {
	MaxTotalActive         int
	MaxDynamicActive       int
	MaxProbation           int
	MinScoreForProbation   float64
	MinScoreForActive      float64
	DemoteFailureThreshold float64
}

// RelayRegistryRetentionConfig controls probe observation retention.
type RelayRegistryRetentionConfig struct {
	RawProbeDays     int
	PurgeBatchLimit  int
	PurgeRunInterval time.Duration
}

// LoadRelayRegistryConfig reads relay registry config from environment variables.
func LoadRelayRegistryConfig() (RelayRegistryConfig, error) {
	refreshInterval, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_REFRESH_INTERVAL", 5*time.Minute)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	probeInterval, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_PROBING_INTERVAL", 5*time.Minute)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	probeTimeoutConnect, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_PROBING_TIMEOUT_CONNECT", 10*time.Second)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	probeTimeoutEOSE, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_PROBING_TIMEOUT_EOSE", 15*time.Second)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	purgeRunInterval, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_RETENTION_PURGE_INTERVAL", 1*time.Hour)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	reconcilePoll, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_RECONCILE_POLL_INTERVAL", 30*time.Second)
	if err != nil {
		return RelayRegistryConfig{}, err
	}
	reconcileDrain, err := getEnvPositiveDurationStrict("RELAY_REGISTRY_RECONCILE_DRAIN_TIMEOUT", 10*time.Second)
	if err != nil {
		return RelayRegistryConfig{}, err
	}

	cfg := RelayRegistryConfig{
		Enabled:             getEnvBool("RELAY_REGISTRY_ENABLED", false),
		SeedRelays:          parseCSVEnv("RELAY_REGISTRY_SEED_RELAYS"),
		AllowPrivateNetwork: getEnvBool("RELAY_REGISTRY_ALLOW_PRIVATE_NETWORK", false),
		Discovery: RelayRegistryDiscoveryConfig{
			Enabled:                getEnvBool("RELAY_REGISTRY_DISCOVERY_ENABLED", false),
			MinDistinctUserRefs:    getEnvInt("RELAY_REGISTRY_DISCOVERY_MIN_DISTINCT_USER_REFS", 3),
			MaxNewCandidatesPerRun: getEnvInt("RELAY_REGISTRY_DISCOVERY_MAX_NEW_CANDIDATES", 25),
			MaxVariantsPerHost:     getEnvInt("RELAY_REGISTRY_DISCOVERY_MAX_VARIANTS_PER_HOST", 3),
		},
		Probing: RelayRegistryProbingConfig{
			Enabled:        getEnvBool("RELAY_REGISTRY_PROBING_ENABLED", false),
			Interval:       probeInterval,
			TimeoutConnect: probeTimeoutConnect,
			TimeoutEOSE:    probeTimeoutEOSE,
			MaxParallel:    getEnvInt("RELAY_REGISTRY_PROBING_MAX_PARALLEL", 5),
		},
		Admission: RelayRegistryAdmissionConfig{
			MaxTotalActive:         getEnvInt("RELAY_REGISTRY_ADMISSION_MAX_TOTAL_ACTIVE", 20),
			MaxDynamicActive:       getEnvInt("RELAY_REGISTRY_ADMISSION_MAX_DYNAMIC_ACTIVE", 15),
			MaxProbation:           getEnvInt("RELAY_REGISTRY_ADMISSION_MAX_PROBATION", 20),
			MinScoreForProbation:   float64(getEnvInt("RELAY_REGISTRY_ADMISSION_MIN_SCORE_PROBATION", 10)),
			MinScoreForActive:      float64(getEnvInt("RELAY_REGISTRY_ADMISSION_MIN_SCORE_ACTIVE", 30)),
			DemoteFailureThreshold: float64(getEnvInt("RELAY_REGISTRY_ADMISSION_DEMOTE_FAILURE_THRESHOLD", 60)) / 100.0,
		},
		Retention: RelayRegistryRetentionConfig{
			RawProbeDays:     getEnvInt("RELAY_REGISTRY_RETENTION_RAW_PROBE_DAYS", 14),
			PurgeBatchLimit:  getEnvInt("RELAY_REGISTRY_RETENTION_PURGE_BATCH_LIMIT", 500),
			PurgeRunInterval: purgeRunInterval,
		},
		Reconcile: RelayRegistryReconcileConfig{
			PollInterval: reconcilePoll,
			DrainTimeout: reconcileDrain,
		},
		RefreshInterval: refreshInterval,
	}

	if err := validateRelayRegistryConfig(cfg); err != nil {
		return RelayRegistryConfig{}, err
	}
	return cfg, nil
}

func validateRelayRegistryConfig(cfg RelayRegistryConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.RefreshInterval <= 0 {
		return fmt.Errorf("RELAY_REGISTRY_REFRESH_INTERVAL must be > 0")
	}
	if cfg.Admission.MaxTotalActive <= 0 {
		return fmt.Errorf("RELAY_REGISTRY_ADMISSION_MAX_TOTAL_ACTIVE must be > 0")
	}
	if cfg.Admission.MaxDynamicActive < 0 {
		return fmt.Errorf("RELAY_REGISTRY_ADMISSION_MAX_DYNAMIC_ACTIVE must be >= 0")
	}
	if cfg.Retention.RawProbeDays <= 0 {
		return fmt.Errorf("RELAY_REGISTRY_RETENTION_RAW_PROBE_DAYS must be > 0")
	}
	if cfg.Probing.Enabled {
		if cfg.Probing.Interval <= 0 {
			return fmt.Errorf("RELAY_REGISTRY_PROBING_INTERVAL must be > 0")
		}
		if cfg.Probing.MaxParallel <= 0 {
			return fmt.Errorf("RELAY_REGISTRY_PROBING_MAX_PARALLEL must be > 0")
		}
	}
	return nil
}
