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
	ServiceName   string
	Environment   string
	Database      DatabaseConfig
	Observability ObservabilityConfig
}

// DatabaseConfig owns database connectivity settings.
type DatabaseConfig struct {
	URL string
}

// ObservabilityConfig owns process-level observability settings.
type ObservabilityConfig struct {
	MetricsAddr string
	DebugAddr   string
}

func loadSharedConfig(serviceName string) (SharedConfig, error) {
	cfg := SharedConfig{
		ServiceName: strings.TrimSpace(serviceName),
		Environment: getEnv("ENVIRONMENT", "development"),
		Database: DatabaseConfig{
			URL: strings.TrimSpace(os.Getenv("DATABASE_URL")),
		},
		Observability: ObservabilityConfig{
			MetricsAddr: strings.TrimSpace(getEnv("METRICS_ADDR", ":9090")),
			DebugAddr:   strings.TrimSpace(getEnv("DEBUG_ADDR", "")),
		},
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
