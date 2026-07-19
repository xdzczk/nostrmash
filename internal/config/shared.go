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
// MaxConns is resolved by resolveDatabaseMaxConns with precedence
// DATABASE_MAX_CONNS (env) > DSN pool_max_conns > per-service default
// (see databaseMaxConnsDefaults). A resolved value of 0 means "do not
// override" and lets store.OpenPool honor the DSN/pgx value. The bare
// pgx default of 4 is dangerously low: the API deadlocks under mixed
// WS+API load, and worker sweepers (author_analytics, profile_stats,
// meilisearch) starve bundle workers; the per-service defaults exist to
// prevent that whenever the operator expresses no explicit preference.
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
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	cfg := SharedConfig{
		ServiceName: strings.TrimSpace(serviceName),
		Environment: getEnv("ENVIRONMENT", "development"),
		Database: DatabaseConfig{
			URL:      databaseURL,
			MaxConns: resolveDatabaseMaxConns(serviceName, int32(maxConns), databaseURL),
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

// databaseMaxConnsDefaults holds the safe per-service pool sizes applied when
// neither DATABASE_MAX_CONNS nor the DSN's pool_max_conns is set. The pgx
// default of 4 is dangerously low: the API deadlocks under mixed WS+API load
// and worker sweepers starve bundle workers. These defaults exceed the
// concurrency each binary actually drives.
var databaseMaxConnsDefaults = map[string]int32{
	"api":          32,
	"worker":       16,
	"ingestor":     8,
	"trust_worker": 8,
}

// databaseMaxConnsFallbackDefault is used for any service name not in the map
// (kept above the pgx default of 4 so no binary silently runs undersized).
const databaseMaxConnsFallbackDefault int32 = 16

// resolveDatabaseMaxConns applies precedence env > DSN > per-service default.
// A value of 0 means "do not override" and is passed through to store.OpenPool,
// which then honors whatever pool_max_conns the DSN carries (or the pgx
// default). We only fall back to a service default when the operator has
// expressed no preference at all (env unset/zero AND DSN silent).
func resolveDatabaseMaxConns(serviceName string, envMaxConns int32, databaseURL string) int32 {
	if envMaxConns > 0 {
		return envMaxConns
	}
	if dsnSpecifiesPoolMaxConns(databaseURL) {
		return 0
	}
	if def, ok := databaseMaxConnsDefaults[strings.TrimSpace(serviceName)]; ok {
		return def
	}
	return databaseMaxConnsFallbackDefault
}

// dsnSpecifiesPoolMaxConns reports whether the DSN carries an explicit
// pool_max_conns setting (URL query form or keyword/value form). pgx sets the
// same default of 4 whether the key is present or absent, so the parsed config
// cannot distinguish the two; a textual check is the reliable signal.
func dsnSpecifiesPoolMaxConns(databaseURL string) bool {
	return strings.Contains(databaseURL, "pool_max_conns")
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
