package config

import (
	"fmt"
	"time"
)

// HydrationConfig bounds the on-demand account hydration service. Hydration
// fetches a bounded slice of an account's data from relays and persists it
// through the normal validate/dedupe/canonical-insert path. Every bound exists
// to keep a single hydration run cheap and prevent it from becoming an
// unbounded backfill.
type HydrationConfig struct {
	// Enabled gates the whole feature (the job handler and API). Default true,
	// but inert until something enqueues a job.
	Enabled bool
	// PublicEnabled allows unauthenticated, rate-limited public triggering.
	// Default false: only authenticated admin callers can enqueue.
	PublicEnabled bool
	// MaxRelays caps how many relays a single run queries.
	MaxRelays int
	// MaxEventsPerAccount caps total events persisted per run.
	MaxEventsPerAccount int
	// MaxLookbackDays bounds how far back notes are fetched.
	MaxLookbackDays int
	// MaxRuntime caps wall-clock time for one run.
	MaxRuntime time.Duration
	// MaxBytes caps total bytes fetched per run (0 = unbounded by bytes).
	MaxBytes int64
	// Cooldown is the minimum interval between successive hydrations of the
	// same account (enforced against last_hydrated_at).
	Cooldown time.Duration
	// MaxConcurrency caps simultaneous hydration runs across the worker.
	MaxConcurrency int
	// RateLimitPerMinute bounds public-triggered enqueues per minute.
	RateLimitPerMinute int
	// Relays is the explicit relay set queried during hydration. When empty the
	// worker falls back to the most recently active relays known locally.
	Relays []string
	// ConnectTimeout / IdleTimeout bound the per-relay websocket fetch.
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
}

func loadHydrationConfig() (HydrationConfig, error) {
	maxRelays, err := getEnvPositiveIntStrict("HYDRATION_MAX_RELAYS", 8)
	if err != nil {
		return HydrationConfig{}, err
	}
	maxEvents, err := getEnvPositiveIntStrict("HYDRATION_MAX_EVENTS_PER_ACCOUNT", 2000)
	if err != nil {
		return HydrationConfig{}, err
	}
	maxLookbackDays, err := getEnvPositiveIntStrict("HYDRATION_MAX_LOOKBACK_DAYS", 90)
	if err != nil {
		return HydrationConfig{}, err
	}
	maxRuntime, err := getEnvPositiveDurationStrict("HYDRATION_MAX_RUNTIME", 60*time.Second)
	if err != nil {
		return HydrationConfig{}, err
	}
	maxBytes, err := getEnvNonNegativeInt64Strict("HYDRATION_MAX_BYTES", 32<<20)
	if err != nil {
		return HydrationConfig{}, err
	}
	cooldown, err := getEnvPositiveDurationStrict("HYDRATION_COOLDOWN", 6*time.Hour)
	if err != nil {
		return HydrationConfig{}, err
	}
	maxConcurrency, err := getEnvPositiveIntStrict("HYDRATION_MAX_CONCURRENCY", 2)
	if err != nil {
		return HydrationConfig{}, err
	}
	rateLimit, err := getEnvPositiveIntStrict("HYDRATION_RATE_LIMIT", 10)
	if err != nil {
		return HydrationConfig{}, err
	}
	connectTimeout, err := getEnvPositiveDurationStrict("HYDRATION_CONNECT_TIMEOUT", 10*time.Second)
	if err != nil {
		return HydrationConfig{}, err
	}
	idleTimeout, err := getEnvPositiveDurationStrict("HYDRATION_IDLE_TIMEOUT", 4*time.Second)
	if err != nil {
		return HydrationConfig{}, err
	}
	cfg := HydrationConfig{
		Enabled:             getEnvBool("HYDRATION_ENABLED", true),
		PublicEnabled:       getEnvBool("HYDRATION_PUBLIC_ENABLED", false),
		MaxRelays:           maxRelays,
		MaxEventsPerAccount: maxEvents,
		MaxLookbackDays:     maxLookbackDays,
		MaxRuntime:          maxRuntime,
		MaxBytes:            maxBytes,
		Cooldown:            cooldown,
		MaxConcurrency:      maxConcurrency,
		RateLimitPerMinute:  rateLimit,
		Relays:              parseCSVEnv("HYDRATION_RELAYS"),
		ConnectTimeout:      connectTimeout,
		IdleTimeout:         idleTimeout,
	}
	if err := validateHydrationConfig(cfg); err != nil {
		return HydrationConfig{}, err
	}
	return cfg, nil
}

func validateHydrationConfig(cfg HydrationConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.MaxRelays <= 0 {
		return fmt.Errorf("HYDRATION_MAX_RELAYS must be > 0")
	}
	if cfg.MaxEventsPerAccount <= 0 {
		return fmt.Errorf("HYDRATION_MAX_EVENTS_PER_ACCOUNT must be > 0")
	}
	if cfg.MaxLookbackDays <= 0 {
		return fmt.Errorf("HYDRATION_MAX_LOOKBACK_DAYS must be > 0")
	}
	if cfg.MaxRuntime <= 0 {
		return fmt.Errorf("HYDRATION_MAX_RUNTIME must be > 0")
	}
	if cfg.MaxConcurrency <= 0 {
		return fmt.Errorf("HYDRATION_MAX_CONCURRENCY must be > 0")
	}
	return nil
}
