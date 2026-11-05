package config

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/relayurl"
)

// Config holds env-driven settings shared by binaries.
type Config struct {
	ServiceName string
	Mode        string

	DatabaseURL string
	Environment string

	// MetricsAddr is optional HTTP address for Prometheus metrics exposition.
	MetricsAddr string

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

	LiveBootstrapLookbackSeconds int64
	LiveResumeOverlapSeconds     int64
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

// Load reads ingestor configuration from environment variables.
// API and worker binaries should use LoadAPI and LoadWorker.
func Load(serviceName string) (Config, error) {
	if strings.TrimSpace(serviceName) != "ingestor" {
		return Config{}, fmt.Errorf("Load(%q) is unsupported; use LoadAPI or LoadWorker for non-ingestor binaries", serviceName)
	}
	ingestorCfg, err := LoadIngestor()
	if err != nil {
		return Config{}, err
	}
	return Config{
		ServiceName: ingestorCfg.Shared.ServiceName,
		Mode:        ingestorCfg.Runtime.Mode,
		DatabaseURL: ingestorCfg.Shared.Database.URL,
		Environment: ingestorCfg.Shared.Environment,
		MetricsAddr: ingestorCfg.Shared.Observability.MetricsAddr,
		Relay:       ingestorCfg.Relay,
		Backfill:    ingestorCfg.Backfill,
		Replay:      ingestorCfg.Replay,
	}, nil
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

func getEnvInt64(key string, def int64) int64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 64)
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

func getEnvNonNegativeInt64(key string, def int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	if v < 0 {
		return 0, fmt.Errorf("%s must be >= 0", key)
	}
	return v, nil
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
		return validateFilterGroups(cfg)
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

	return validateFilterGroups(cfg)
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

func validateIngestorMode(serviceName, mode string, replay ReplayConfig, relay RelayConfig, registryEnabled bool) error {
	if strings.TrimSpace(serviceName) != "ingestor" {
		return nil
	}
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "live":
		if len(relay.URLs) == 0 && !registryEnabled {
			return fmt.Errorf("live ingest requires at least one relay URL; set INGESTOR_RELAY_URLS or enable RELAY_REGISTRY_ENABLED")
		}
		if relay.LiveBootstrapLookbackSeconds <= 0 {
			return fmt.Errorf("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS must be > 0")
		}
		if relay.LiveResumeOverlapSeconds < 0 {
			return fmt.Errorf("INGESTOR_LIVE_RESUME_OVERLAP_SECONDS must be >= 0")
		}
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

// defaultFilterGroupKinds is the canonical live-ingest kind set for
// default_v1. Ordering is kept ascending so it matches the sorted form used by
// validateFilterGroups. Authored kinds (1, 4, 5, 20, 21, 22, 1984, 9802,
// 10000, 10001, 10003, 10019, 10050, 30000, 30023, 30311, 34235, 34236) are
// author-trust-gated in internal/ingestor/live/gate.go. Kind 1111 is accepted
// when its NIP-22 root is stored, otherwise author-gated. Kind 1059 is
// accepted when a p-tag recipient is trusted. Engagement kinds (6, 7, 16,
// 9321, 9735) are target-gated; 9321 also accepts a trusted p-tag recipient.
// The rest (0, 3, 10002) stay open so the trust graph and profiles can bootstrap.
var defaultFilterGroupKinds = []int{
	0, 1, 3, 4, 5, 6, 7, 8, 9, 11, 16, 20, 21, 22, 1018, 1059, 1063, 1068, 1111, 1311, 1984, 1985, 9041, 9321,
	9735, 9802, 10000, 10001, 10002, 10003, 10006, 10007, 10009, 10012, 10015, 10019, 10030, 10050, 10063,
	30000, 30002, 30004, 30005, 30008, 30009, 30015, 30023, 30311, 30315, 34235, 34236,
	39000, 39001, 39002, 39003,
}

func formatFilterKinds(kinds []int) string {
	parts := make([]string, len(kinds))
	for i, kind := range kinds {
		parts[i] = strconv.Itoa(kind)
	}
	return strings.Join(parts, ",")
}

func defaultFilterGroups() map[string]FilterGroup {
	return map[string]FilterGroup{
		defaultFilterGroupName: {
			Kinds: append([]int(nil), defaultFilterGroupKinds...),
		},
		censusFilterGroupName: {
			Kinds: CensusKinds(),
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
	wantKinds := append([]int(nil), defaultFilterGroupKinds...)
	gotKinds := append([]int(nil), defaultGroup.Kinds...)
	slices.Sort(gotKinds)
	slices.Sort(wantKinds)
	if !slices.Equal(gotKinds, wantKinds) {
		return fmt.Errorf(
			"relay filter group %q must use kinds %s in this chunk",
			defaultFilterGroupName,
			formatFilterKinds(defaultFilterGroupKinds),
		)
	}
	active := strings.TrimSpace(cfg.ActiveFilterGroup)
	if active == "" {
		return fmt.Errorf("INGESTOR_FILTER_GROUP is required")
	}
	if _, ok := cfg.FilterGroups[active]; !ok {
		return fmt.Errorf("INGESTOR_FILTER_GROUP %q is not defined", active)
	}
	if census, ok := cfg.FilterGroups[censusFilterGroupName]; ok {
		want := CensusKinds()
		got := append([]int(nil), census.Kinds...)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			return fmt.Errorf("relay filter group %q must use the built-in census kind list", censusFilterGroupName)
		}
	}
	return nil
}

func normalizeRelayURL(raw string, requireTLS bool) (string, error) {
	return relayurl.Normalize(raw, relayurl.NormalizeOptions{RequireTLS: requireTLS})
}
