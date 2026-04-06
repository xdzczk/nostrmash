package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type APIConfig struct {
	Shared   SharedConfig
	HTTP     APIHTTPConfig
	PrimalWS APIPrimalWSConfig
	Relay    APIRelayConfig
}

type APIHTTPConfig struct {
	Addr               string
	MaxBatchSize       int
	RateLimitRPM       int
	RateLimitBurst     int
	SearchRateLimitRPM int
	BatchRateLimitRPM  int
	AdminBearerToken   string
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

	cfg := APIConfig{
		Shared: shared,
		HTTP: APIHTTPConfig{
			Addr:               getEnv("HTTP_ADDR", ":8080"),
			MaxBatchSize:       maxBatchSize,
			RateLimitRPM:       httpRateLimitRPM,
			RateLimitBurst:     httpRateLimitBurst,
			SearchRateLimitRPM: httpSearchRateLimitRPM,
			BatchRateLimitRPM:  httpBatchRateLimitRPM,
			AdminBearerToken:   strings.TrimSpace(getEnv("ADMIN_BEARER_TOKEN", "")),
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
	}
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
