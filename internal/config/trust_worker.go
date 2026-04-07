package config

import (
	"fmt"
	"strings"
	"time"
)

type RedisConfig struct {
	URL       string
	KeyPrefix string
}

type TrustWorkerConfig struct {
	Shared             SharedConfig
	Redis              RedisConfig
	Concurrency        int
	ClaimBatchSize     int
	PollInterval       time.Duration
	RetryDelay         time.Duration
	EnableRedisSync    bool
	EnableScoreCompute bool
}

func LoadTrustWorker() (TrustWorkerConfig, error) {
	shared, err := loadSharedConfig("trust_worker")
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	concurrency, err := getEnvPositiveIntStrict("TRUST_WORKER_CONCURRENCY", 2)
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	claimBatchSize, err := getEnvPositiveIntStrict("TRUST_WORKER_CLAIM_BATCH_SIZE", 5)
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	pollInterval, err := getEnvPositiveDurationStrict("TRUST_WORKER_POLL_INTERVAL", time.Second)
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	retryDelay, err := getEnvPositiveDurationStrict("TRUST_WORKER_RETRY_DELAY", 5*time.Second)
	if err != nil {
		return TrustWorkerConfig{}, err
	}

	cfg := TrustWorkerConfig{
		Shared: shared,
		Redis: RedisConfig{
			URL:       strings.TrimSpace(getEnv("TRUST_REDIS_URL", "")),
			KeyPrefix: strings.TrimSpace(getEnv("TRUST_REDIS_KEY_PREFIX", "nostrmash")),
		},
		Concurrency:        concurrency,
		ClaimBatchSize:     claimBatchSize,
		PollInterval:       pollInterval,
		RetryDelay:         retryDelay,
		EnableRedisSync:    getEnvBool("TRUST_ENABLE_REDIS_SYNC", false),
		EnableScoreCompute: getEnvBool("TRUST_ENABLE_SCORE_COMPUTE", true),
	}
	if err := validateTrustWorkerConfig(cfg); err != nil {
		return TrustWorkerConfig{}, err
	}
	return cfg, nil
}

func validateTrustWorkerConfig(cfg TrustWorkerConfig) error {
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("TRUST_WORKER_CONCURRENCY must be > 0")
	}
	if cfg.ClaimBatchSize <= 0 {
		return fmt.Errorf("TRUST_WORKER_CLAIM_BATCH_SIZE must be > 0")
	}
	if cfg.PollInterval <= 0 {
		return fmt.Errorf("TRUST_WORKER_POLL_INTERVAL must be > 0")
	}
	if cfg.RetryDelay <= 0 {
		return fmt.Errorf("TRUST_WORKER_RETRY_DELAY must be > 0")
	}
	if strings.TrimSpace(cfg.Redis.URL) == "" {
		return fmt.Errorf("TRUST_REDIS_URL is required")
	}
	if strings.TrimSpace(cfg.Redis.KeyPrefix) == "" {
		return fmt.Errorf("TRUST_REDIS_KEY_PREFIX must not be empty")
	}
	if !cfg.EnableRedisSync && !cfg.EnableScoreCompute {
		return fmt.Errorf("at least one of TRUST_ENABLE_REDIS_SYNC or TRUST_ENABLE_SCORE_COMPUTE must be true")
	}
	return nil
}
