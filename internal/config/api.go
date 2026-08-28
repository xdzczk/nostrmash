package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type APIConfig struct {
	Shared         SharedConfig
	HTTP           APIHTTPConfig
	PrimalWS       APIPrimalWSConfig
	Relay          APIRelayConfig
	RelayFallback  APIRelayFallbackConfig
	DiscoveryCache APIDiscoveryCacheConfig
	Meilisearch    MeilisearchConfig
	Hydration      HydrationConfig
}

type APIHTTPConfig struct {
	Addr                        string
	MaxBatchSize                int
	RateLimitRPM                int
	RateLimitBurst              int
	SearchRateLimitRPM          int
	BatchRateLimitRPM           int
	DiscoveryRateLimitRPM       int
	SuggestRateLimitRPM         int
	PublicStatsRateLimitRPM     int
	PublicMaxResultLimit        int
	PublicMaxPageSize           int
	PublicMaxPageOffset         int
	PublicMaxSearchWindowHrs    int
	PublicMaxDiscoveryWindowHrs int
	AdminBearerToken            string
}

type APIPrimalWSConfig struct {
	MaxSubscriptions     int
	RequestTimeout       time.Duration
	MaxMessageBytes      int64
	MaxReqPerMinute      int
	DMCompatRateLimitRPM int
	AllowedOrigins       []string
	AllowAnyOrigin       bool
}

// APIRelayConfig is read by API for admin diagnostics only.
type APIRelayConfig struct {
	URLs     []string
	Disabled []string
}

type APIRelayFallbackConfig struct {
	Enabled         bool
	URLs            []string
	ProfileURLs     []string
	Timeout         time.Duration
	MaxFanout       int
	UseRegistry     bool
	RefreshInterval time.Duration
}

type APIDiscoveryCacheConfig struct {
	Enabled        bool
	MaxEntries     int
	BundleTTL      time.Duration
	DiscoveryTTL   time.Duration
	SuggestionTTL  time.Duration
	TrendingTTL    time.Duration
	StatsTTL       time.Duration
	PublicStatsTTL time.Duration
}

type MeilisearchConfig struct {
	Enabled      bool
	URL          string
	MasterKey    string
	SearchAPIKey string
}

func LoadAPI() (APIConfig, error) {
	shared, err := loadSharedConfig("api")
	if err != nil {
		return APIConfig{}, err
	}
	// API serves /metrics on the main HTTP listener; dedicated METRICS_ADDR is worker/ingestor-only.
	shared.Observability.MetricsAddr = ""
	maxBatchSize, err := getEnvPositiveIntStrict("API_MAX_BATCH_SIZE", 200)
	if err != nil {
		return APIConfig{}, err
	}
	httpRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_RATE_LIMIT_RPM", 240)
	if err != nil {
		return APIConfig{}, err
	}
	httpRateLimitBurst, err := getEnvPositiveIntStrict("HTTP_RATE_LIMIT_BURST", 60)
	if err != nil {
		return APIConfig{}, err
	}
	httpSearchRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_SEARCH_RATE_LIMIT_RPM", 60)
	if err != nil {
		return APIConfig{}, err
	}
	httpBatchRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_BATCH_RATE_LIMIT_RPM", 40)
	if err != nil {
		return APIConfig{}, err
	}
	httpDiscoveryRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_DISCOVERY_RATE_LIMIT_RPM", 90)
	if err != nil {
		return APIConfig{}, err
	}
	httpSuggestRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_SUGGEST_RATE_LIMIT_RPM", 120)
	if err != nil {
		return APIConfig{}, err
	}
	httpPublicStatsRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_PUBLIC_STATS_RATE_LIMIT_RPM", 120)
	if err != nil {
		return APIConfig{}, err
	}
	publicMaxResultLimit, err := getEnvPositiveIntStrict("HTTP_PUBLIC_MAX_RESULT_LIMIT", 100)
	if err != nil {
		return APIConfig{}, err
	}
	publicMaxPageSize, err := getEnvPositiveIntStrict("HTTP_PUBLIC_MAX_PAGE_SIZE", 100)
	if err != nil {
		return APIConfig{}, err
	}
	publicMaxPageOffset, err := getEnvPositiveIntStrict("HTTP_PUBLIC_MAX_PAGE_OFFSET", 5000)
	if err != nil {
		return APIConfig{}, err
	}
	publicMaxSearchWindowHours, err := getEnvPositiveIntStrict("HTTP_PUBLIC_MAX_SEARCH_WINDOW_HOURS", 7*24)
	if err != nil {
		return APIConfig{}, err
	}
	publicMaxDiscoveryWindowHours, err := getEnvPositiveIntStrict("HTTP_PUBLIC_MAX_DISCOVERY_WINDOW_HOURS", 30*24)
	if err != nil {
		return APIConfig{}, err
	}
	primalMaxSubscriptions, err := getEnvPositiveIntStrict("PRIMAL_WS_MAX_SUBSCRIPTIONS", 200)
	if err != nil {
		return APIConfig{}, err
	}
	primalMaxReqPerMinute, err := getEnvPositiveIntStrict("PRIMAL_WS_MAX_REQ_PER_MINUTE", 240)
	if err != nil {
		return APIConfig{}, err
	}
	dmCompatRateLimitRPM, err := getEnvPositiveIntStrict("HTTP_DM_COMPAT_RATE_LIMIT_RPM", 30)
	if err != nil {
		return APIConfig{}, err
	}
	primalMaxMessageBytes, err := getEnvPositiveInt64Strict("PRIMAL_WS_MAX_MESSAGE_BYTES", 1<<20)
	if err != nil {
		return APIConfig{}, err
	}
	primalRequestTimeout, err := getEnvPositiveDurationStrict("PRIMAL_WS_REQUEST_TIMEOUT", 10*time.Second)
	if err != nil {
		return APIConfig{}, err
	}
	relayFallbackTimeout, err := getEnvPositiveDurationStrict("API_RELAY_FALLBACK_TIMEOUT", 2*time.Second)
	if err != nil {
		return APIConfig{}, err
	}
	relayFallbackMaxFanout, err := getEnvPositiveIntStrict("API_RELAY_FALLBACK_MAX_FANOUT", 3)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryCacheMaxEntries, err := getEnvPositiveIntStrict("API_DISCOVERY_CACHE_MAX_ENTRIES", 256)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryTrendingTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_TRENDING_TTL", 60*time.Second)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryPublicStatsTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_PUBLIC_STATS_TTL", 10*time.Minute)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryBundleTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_BUNDLE_TTL", discoveryTrendingTTL)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryDiscoveryTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_DISCOVERY_TTL", discoveryTrendingTTL)
	if err != nil {
		return APIConfig{}, err
	}
	discoverySuggestionTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_SUGGESTION_TTL", discoveryTrendingTTL)
	if err != nil {
		return APIConfig{}, err
	}
	discoveryStatsTTL, err := getEnvPositiveDurationStrict("API_DISCOVERY_CACHE_STATS_TTL", discoveryPublicStatsTTL)
	if err != nil {
		return APIConfig{}, err
	}
	fallbackURLs := parseCSVEnv("API_RELAY_FALLBACK_URLS")
	if len(fallbackURLs) == 0 {
		fallbackURLs = parseCSVEnv("INGESTOR_RELAY_URLS")
	}
	normalizedFallbackURLs, err := normalizeFallbackRelayURLs(fallbackURLs)
	if err != nil {
		return APIConfig{}, err
	}
	profileFallbackURLs := parseCSVEnv("API_RELAY_FALLBACK_PROFILE_URLS")
	if len(profileFallbackURLs) == 0 {
		// Two independent directory relays: a single default was a single
		// point of failure (purplepag.es went down for days and took 100%
		// of profile fallback with it). The lookup client additionally
		// pads profile lookups with general event relays at query time.
		profileFallbackURLs = []string{"wss://purplepag.es", "wss://user.kindpag.es"}
	}
	normalizedProfileFallbackURLs, err := normalizeFallbackRelayURLs(profileFallbackURLs)
	if err != nil {
		return APIConfig{}, err
	}
	fallbackRefreshInterval, err := getEnvPositiveDurationStrict("API_RELAY_FALLBACK_REFRESH_INTERVAL", 5*time.Minute)
	if err != nil {
		return APIConfig{}, err
	}

	cfg := APIConfig{
		Shared: shared,
		HTTP: APIHTTPConfig{
			Addr:                        getEnv("HTTP_ADDR", ":8080"),
			MaxBatchSize:                maxBatchSize,
			RateLimitRPM:                httpRateLimitRPM,
			RateLimitBurst:              httpRateLimitBurst,
			SearchRateLimitRPM:          httpSearchRateLimitRPM,
			BatchRateLimitRPM:           httpBatchRateLimitRPM,
			DiscoveryRateLimitRPM:       httpDiscoveryRateLimitRPM,
			SuggestRateLimitRPM:         httpSuggestRateLimitRPM,
			PublicStatsRateLimitRPM:     httpPublicStatsRateLimitRPM,
			PublicMaxResultLimit:        publicMaxResultLimit,
			PublicMaxPageSize:           publicMaxPageSize,
			PublicMaxPageOffset:         publicMaxPageOffset,
			PublicMaxSearchWindowHrs:    publicMaxSearchWindowHours,
			PublicMaxDiscoveryWindowHrs: publicMaxDiscoveryWindowHours,
			AdminBearerToken:            strings.TrimSpace(getEnv("ADMIN_BEARER_TOKEN", "")),
		},
		PrimalWS: APIPrimalWSConfig{
			MaxSubscriptions:     primalMaxSubscriptions,
			RequestTimeout:       primalRequestTimeout,
			MaxMessageBytes:      primalMaxMessageBytes,
			MaxReqPerMinute:      primalMaxReqPerMinute,
			DMCompatRateLimitRPM: dmCompatRateLimitRPM,
			AllowedOrigins:       parseCSVEnv("PRIMAL_WS_ALLOWED_ORIGINS"),
			AllowAnyOrigin:       getEnvBool("PRIMAL_WS_ALLOW_ANY_ORIGIN", false),
		},
		Relay: APIRelayConfig{
			URLs:     parseCSVEnv("INGESTOR_RELAY_URLS"),
			Disabled: parseCSVEnv("INGESTOR_RELAY_DISABLED"),
		},
		RelayFallback: APIRelayFallbackConfig{
			Enabled:         getEnvBool("API_RELAY_FALLBACK_ENABLED", false),
			URLs:            normalizedFallbackURLs,
			ProfileURLs:     normalizedProfileFallbackURLs,
			Timeout:         relayFallbackTimeout,
			MaxFanout:       relayFallbackMaxFanout,
			UseRegistry:     getEnvBool("API_RELAY_FALLBACK_USE_REGISTRY", true),
			RefreshInterval: fallbackRefreshInterval,
		},
		DiscoveryCache: APIDiscoveryCacheConfig{
			Enabled:        getEnvBool("API_DISCOVERY_CACHE_ENABLED", true),
			MaxEntries:     discoveryCacheMaxEntries,
			BundleTTL:      discoveryBundleTTL,
			DiscoveryTTL:   discoveryDiscoveryTTL,
			SuggestionTTL:  discoverySuggestionTTL,
			TrendingTTL:    discoveryTrendingTTL,
			StatsTTL:       discoveryStatsTTL,
			PublicStatsTTL: discoveryPublicStatsTTL,
		},
		Meilisearch: MeilisearchConfig{
			Enabled:      getEnvBool("MEILI_ENABLED", false),
			URL:          strings.TrimSpace(getEnv("MEILI_URL", "")),
			MasterKey:    strings.TrimSpace(getEnv("MEILI_MASTER_KEY", "")),
			SearchAPIKey: strings.TrimSpace(getEnv("MEILI_SEARCH_API_KEY", "")),
		},
	}
	if cfg.Meilisearch.SearchAPIKey == "" {
		cfg.Meilisearch.SearchAPIKey = cfg.Meilisearch.MasterKey
	}
	hydrationCfg, err := loadHydrationConfig()
	if err != nil {
		return APIConfig{}, err
	}
	cfg.Hydration = hydrationCfg
	if err := validateAPIConfig(cfg); err != nil {
		return APIConfig{}, err
	}
	return cfg, nil
}

func validateAPIConfig(cfg APIConfig) error {
	if strings.TrimSpace(cfg.HTTP.Addr) == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if cfg.PrimalWS.RequestTimeout <= 0 {
		return fmt.Errorf("PRIMAL_WS_REQUEST_TIMEOUT must be > 0")
	}
	if !cfg.PrimalWS.AllowAnyOrigin {
		for _, origin := range cfg.PrimalWS.AllowedOrigins {
			if err := validateAllowedOrigin(origin); err != nil {
				return fmt.Errorf("PRIMAL_WS_ALLOWED_ORIGINS contains invalid origin %q: %w", origin, err)
			}
		}
	}
	if cfg.RelayFallback.Enabled && len(cfg.RelayFallback.URLs) == 0 {
		return fmt.Errorf("API_RELAY_FALLBACK_ENABLED requires API_RELAY_FALLBACK_URLS or INGESTOR_RELAY_URLS")
	}
	if cfg.RelayFallback.Enabled && cfg.RelayFallback.MaxFanout <= 0 {
		return fmt.Errorf("API_RELAY_FALLBACK_MAX_FANOUT must be > 0")
	}
	if cfg.Meilisearch.Enabled {
		if cfg.Meilisearch.URL == "" {
			return fmt.Errorf("MEILI_ENABLED requires MEILI_URL")
		}
		parsed, err := url.Parse(cfg.Meilisearch.URL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			return fmt.Errorf("MEILI_URL must be a valid http(s) URL")
		}
	}
	return nil
}

func validateAllowedOrigin(raw string) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if parsed.Host == "" {
		return fmt.Errorf("host is required")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		return fmt.Errorf("path is not allowed")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return fmt.Errorf("query, fragment, and userinfo are not allowed")
	}
	return nil
}

func normalizeFallbackRelayURLs(urls []string) ([]string, error) {
	out := make([]string, 0, len(urls))
	seen := make(map[string]struct{}, len(urls))
	for _, raw := range urls {
		normalized, err := normalizeRelayURL(raw, false)
		if err != nil {
			return nil, fmt.Errorf("invalid fallback relay URL %q: %w", raw, err)
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out, nil
}
