package config

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Config holds env-driven settings shared by binaries.
type Config struct {
	ServiceName string
	Mode        string

	DatabaseURL string
	Environment string

	// HTTPAddr is the listen address for the API (e.g. ":8080").
	HTTPAddr string
	// APIMaxBatchSize caps number of event IDs in /api/v1/events/batch.
	APIMaxBatchSize int
	// MetricsAddr is optional HTTP address for Prometheus metrics exposition.
	MetricsAddr string
	// AdminBearerToken protects /admin endpoints with Bearer auth.
	AdminBearerToken string

	Relay    RelayConfig
	Backfill BackfillConfig
	Replay   ReplayConfig
}

// RelayConfig holds ingestor relay lifecycle settings.
type RelayConfig struct {
	URLs      []string
	Allowlist []string
	Disabled  []string

	RequireTLS bool

	ConnectTimeout time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	LagThreshold   time.Duration

	FilterGroups      map[string]FilterGroup
	ActiveFilterGroup string
}

// BackfillConfig controls bootstrap/backfill ingest behavior.
type BackfillConfig struct {
	Enabled bool

	Mode string

	Since *int64
	Until *int64

	PageLimit int

	IdleTimeout    time.Duration
	EmptyPageMax   int
	ConnectTimeout time.Duration
}

// ReplayConfig controls deterministic replay mode for the ingestor.
type ReplayConfig struct {
	FixturePath string
}

// FilterGroup names a reusable subscription filter set.
type FilterGroup struct {
	Kinds []int `json:"kinds"`
}

const defaultFilterGroupName = "default_v1"

// Load reads configuration from environment variables.
func Load(serviceName string) (Config, error) {
	environment := getEnv("ENVIRONMENT", "development")
	localDev := isLocalDevEnvironment(environment)
	requireTLS := getEnvBool("INGESTOR_RELAY_REQUIRE_TLS", !localDev)

	c := Config{
		ServiceName: serviceName,
		Mode:        strings.ToLower(strings.TrimSpace(getEnv("INGESTOR_MODE", "live"))),
		DatabaseURL: strings.TrimSpace(os.Getenv("DATABASE_URL")),
		Environment: environment,
		HTTPAddr:    getEnv("HTTP_ADDR", ":8080"),
		MetricsAddr: strings.TrimSpace(getEnv("METRICS_ADDR", ":9090")),
		APIMaxBatchSize: getEnvInt(
			"API_MAX_BATCH_SIZE",
			200,
		),
		AdminBearerToken: strings.TrimSpace(os.Getenv("ADMIN_BEARER_TOKEN")),
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
	if err := applyConfiguredFilterGroups(&c.Relay); err != nil {
		return c, err
	}

	if c.DatabaseURL == "" {
		return c, fmt.Errorf("DATABASE_URL is required")
	}
	if err := validateRelayConfig(c.Relay); err != nil {
		return c, err
	}
	if err := validateBackfillConfig(c.Backfill); err != nil {
		return c, err
	}
	if err := validateIngestorMode(c.Mode, c.Replay); err != nil {
		return c, err
	}

	return c, nil
}

func getEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getEnvBool(key string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		return def
	}
	return v
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return def
	}
	return d
}

func getEnvInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return def
	}
	return v
}

func getEnvOptionalUnix(key string) *int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return nil
	}
	return &v
}

func parseCSVEnv(key string) []string {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func isLocalDevEnvironment(environment string) bool {
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "dev", "development", "local", "test":
		return true
	default:
		return false
	}
}

func validateRelayConfig(cfg RelayConfig) error {
	if cfg.InitialBackoff > cfg.MaxBackoff {
		return fmt.Errorf("INGESTOR_RELAY_BACKOFF_INITIAL must be <= INGESTOR_RELAY_BACKOFF_MAX")
	}
	if len(cfg.URLs) == 0 {
		if err := validateFilterGroups(cfg); err != nil {
			return err
		}
		return nil
	}
	if len(cfg.Allowlist) == 0 {
		return fmt.Errorf("INGESTOR_RELAY_ALLOWLIST is required when INGESTOR_RELAY_URLS is set")
	}

	allow := make(map[string]struct{}, len(cfg.Allowlist))
	for _, relayURL := range cfg.Allowlist {
		normalized, err := normalizeRelayURL(relayURL, cfg.RequireTLS)
		if err != nil {
			return fmt.Errorf("invalid allowlisted relay %q: %w", relayURL, err)
		}
		allow[normalized] = struct{}{}
	}

	for _, relayURL := range cfg.URLs {
		normalized, err := normalizeRelayURL(relayURL, cfg.RequireTLS)
		if err != nil {
			return fmt.Errorf("invalid relay url %q: %w", relayURL, err)
		}
		if _, ok := allow[normalized]; !ok {
			return fmt.Errorf("relay url %q is not allowlisted", relayURL)
		}
	}

	for _, relayURL := range cfg.Disabled {
		normalized, err := normalizeRelayURL(relayURL, cfg.RequireTLS)
		if err != nil {
			return fmt.Errorf("invalid disabled relay %q: %w", relayURL, err)
		}
		if _, ok := allow[normalized]; !ok {
			return fmt.Errorf("disabled relay %q is not allowlisted", relayURL)
		}
	}

	if err := validateFilterGroups(cfg); err != nil {
		return err
	}

	return nil
}

func validateBackfillConfig(cfg BackfillConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.Mode != "backfill" {
		return fmt.Errorf("INGESTOR_BACKFILL_MODE %q is not implemented", cfg.Mode)
	}
	if cfg.PageLimit <= 0 {
		return fmt.Errorf("INGESTOR_BACKFILL_PAGE_LIMIT must be > 0")
	}
	if cfg.IdleTimeout <= 0 {
		return fmt.Errorf("INGESTOR_BACKFILL_IDLE_TIMEOUT must be > 0")
	}
	if cfg.EmptyPageMax <= 0 {
		return fmt.Errorf("INGESTOR_BACKFILL_EMPTY_PAGE_MAX must be > 0")
	}
	if cfg.ConnectTimeout <= 0 {
		return fmt.Errorf("INGESTOR_BACKFILL_CONNECT_TIMEOUT must be > 0")
	}
	if cfg.Since != nil && cfg.Until != nil && *cfg.Since > *cfg.Until {
		return fmt.Errorf("INGESTOR_BACKFILL_SINCE must be <= INGESTOR_BACKFILL_UNTIL")
	}
	return nil
}

func validateIngestorMode(mode string, replay ReplayConfig) error {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "live":
		return nil
	case "replay":
		if strings.TrimSpace(replay.FixturePath) == "" {
			return fmt.Errorf("INGESTOR_REPLAY_FIXTURE_PATH is required when INGESTOR_MODE=replay")
		}
		return nil
	default:
		return fmt.Errorf("INGESTOR_MODE %q is not implemented", mode)
	}
}

func applyConfiguredFilterGroups(cfg *RelayConfig) error {
	if cfg == nil {
		return fmt.Errorf("relay config is required")
	}
	raw := strings.TrimSpace(os.Getenv("INGESTOR_FILTER_GROUPS_JSON"))
	if raw == "" {
		return nil
	}
	parsed := make(map[string]FilterGroup)
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return fmt.Errorf("invalid INGESTOR_FILTER_GROUPS_JSON: %w", err)
	}
	for name, group := range parsed {
		trimmedName := strings.TrimSpace(name)
		if trimmedName == "" {
			return fmt.Errorf("INGESTOR_FILTER_GROUPS_JSON contains an empty group name")
		}
		cfg.FilterGroups[trimmedName] = FilterGroup{
			Kinds: append([]int(nil), group.Kinds...),
		}
	}
	return nil
}

func defaultFilterGroups() map[string]FilterGroup {
	return map[string]FilterGroup{
		defaultFilterGroupName: {
			Kinds: []int{0, 1, 3, 5, 6, 7, 10002},
		},
	}
}

func validateFilterGroups(cfg RelayConfig) error {
	if cfg.FilterGroups == nil {
		return fmt.Errorf("relay filter groups are required")
	}
	defaultGroup, ok := cfg.FilterGroups[defaultFilterGroupName]
	if !ok {
		return fmt.Errorf("relay filter group %q is required", defaultFilterGroupName)
	}
	wantKinds := []int{0, 1, 3, 5, 6, 7, 10002}
	gotKinds := append([]int(nil), defaultGroup.Kinds...)
	slices.Sort(gotKinds)
	slices.Sort(wantKinds)
	if !slices.Equal(gotKinds, wantKinds) {
		return fmt.Errorf(
			"relay filter group %q must use kinds 0,1,3,5,6,7,10002 in this chunk",
			defaultFilterGroupName,
		)
	}
	active := strings.TrimSpace(cfg.ActiveFilterGroup)
	if active == "" {
		return fmt.Errorf("INGESTOR_FILTER_GROUP is required")
	}
	if _, ok := cfg.FilterGroups[active]; !ok {
		return fmt.Errorf("INGESTOR_FILTER_GROUP %q is not defined", active)
	}
	return nil
}

func normalizeRelayURL(raw string, requireTLS bool) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", fmt.Errorf("parse: %w", err)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("host is required")
	}
	if parsed.Fragment != "" {
		return "", fmt.Errorf("fragments are not allowed")
	}
	if parsed.RawQuery != "" {
		return "", fmt.Errorf("query parameters are not allowed")
	}
	if parsed.User != nil {
		return "", fmt.Errorf("userinfo is not allowed")
	}

	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "wss":
	case "ws":
		if requireTLS {
			return "", fmt.Errorf("ws scheme is disallowed when TLS is required")
		}
	default:
		return "", fmt.Errorf("scheme must be ws or wss")
	}

	host := strings.ToLower(parsed.Host)
	path := strings.TrimSpace(parsed.EscapedPath())
	if path == "/" {
		path = ""
	}
	return fmt.Sprintf("%s://%s%s", scheme, host, path), nil
}
