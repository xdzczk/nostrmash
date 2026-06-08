package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// SharedConfig holds runtime settings shared across binaries.
type SharedConfig struct {
	ServiceName     string
	Environment     string
	Database        DatabaseConfig
	Observability   ObservabilityConfig
	TrustPolicy     TrustPolicyConfig
	StoragePressure StoragePressureConfig
}

// DatabaseConfig owns database connectivity settings.
//
// MaxConns, when > 0, overrides the pool_max_conns value parsed from
// the DATABASE_URL DSN (and the pgx default of 4 when the DSN omits
// it). It is sourced from DATABASE_MAX_CONNS. The default of 4 is
// dangerously low for the worker process, which runs the bundle pool
// plus several background sweeper goroutines (author_analytics,
// profile_stats, meilisearch); see store.OpenPool for the failure mode
// that an undersized pool produces.
type DatabaseConfig struct {
	URL      string
	MaxConns int32
}

// ObservabilityConfig owns process-level observability settings.
type ObservabilityConfig struct {
	MetricsAddr string
	DebugAddr   string
}

func loadSharedConfig(serviceName string) (SharedConfig, error) {
	trustPolicy, err := loadTrustPolicyConfig()
	if err != nil {
		return SharedConfig{}, err
	}
	storagePressure, err := loadStoragePressureConfig()
	if err != nil {
		return SharedConfig{}, err
	}
	maxConns, err := getEnvNonNegativeIntStrict("DATABASE_MAX_CONNS", 0)
	if err != nil {
		return SharedConfig{}, err
	}
	cfg := SharedConfig{
		ServiceName: strings.TrimSpace(serviceName),
		Environment: getEnv("ENVIRONMENT", "development"),
		Database: DatabaseConfig{
			URL:      strings.TrimSpace(os.Getenv("DATABASE_URL")),
			MaxConns: int32(maxConns),
		},
		Observability: ObservabilityConfig{
			MetricsAddr: strings.TrimSpace(getEnv("METRICS_ADDR", ":9090")),
			DebugAddr:   strings.TrimSpace(getEnv("DEBUG_ADDR", "")),
		},
		TrustPolicy:     trustPolicy,
		StoragePressure: storagePressure,
	}
	if cfg.ServiceName == "" {
		return SharedConfig{}, fmt.Errorf("service name is required")
	}
	if err := validateSharedConfig(cfg); err != nil {
		return SharedConfig{}, err
	}
	return cfg, nil
}

func validateSharedConfig(cfg SharedConfig) error {
	if strings.TrimSpace(cfg.Database.URL) == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if addr := strings.TrimSpace(cfg.Observability.MetricsAddr); addr != "" {
		if _, _, err := net.SplitHostPort(addr); err != nil && !strings.HasPrefix(addr, ":") {
			return fmt.Errorf("METRICS_ADDR must be host:port or :port (got %q)", addr)
		}
	}
	if addr := strings.TrimSpace(cfg.Observability.DebugAddr); addr != "" {
		if _, _, err := net.SplitHostPort(addr); err != nil && !strings.HasPrefix(addr, ":") {
			return fmt.Errorf("DEBUG_ADDR must be host:port or :port (got %q)", addr)
		}
	}
	return nil
}

func getEnvPositiveIntStrict(key string, def int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return v, nil
}

func getEnvNonNegativeIntStrict(key string, def int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return v, nil
}

func getEnvPositiveInt64Strict(key string, def int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return v, nil
}

// getEnvIntListStrict parses a comma-separated list of positive
// integers from the named env var, returning def when unset.
// Whitespace around values is ignored. Empty entries are skipped.
// Any parse failure or non-positive value yields an error so misconfig
// surfaces at startup rather than producing silently-wrong behavior.
func getEnvIntListStrict(key string, def []int) ([]int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.Atoi(p)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("%s entries must be positive integers (got %q)", key, p)
		}
		out = append(out, v)
	}
	if len(out) == 0 {
		return def, nil
	}
	return out, nil
}

func getEnvPositiveDurationStrict(key string, def time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration (e.g. 10s)", key)
	}
	return v, nil
}
