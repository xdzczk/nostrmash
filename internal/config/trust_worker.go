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
	Shared                       SharedConfig
	Redis                        RedisConfig
	Concurrency                  int
	ClaimBatchSize               int
	PollInterval                 time.Duration
	RetryDelay                   time.Duration
	JobRecovery                  WorkerJobRecoveryConfig
	JobRetention                 WorkerJobRetentionConfig
	EnableRedisSync              bool
	EnableScoreCompute           bool
	GraphSnapshotRefreshInterval time.Duration
	RunSchedulerInterval         time.Duration
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
	jobRecovery, err := loadWorkerJobRecoveryConfig()
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	jobRetention, err := loadWorkerJobRetentionConfig()
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	graphSnapshotRefreshInterval, err := getEnvPositiveDurationStrict("TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL", 10*time.Minute)
	if err != nil {
		return TrustWorkerConfig{}, err
	}
	runSchedulerInterval, err := getEnvPositiveDurationStrict("TRUST_RUN_INTERVAL", time.Hour)
	if err != nil {
		return TrustWorkerConfig{}, err
	}

	cfg := TrustWorkerConfig{
		Shared: shared,
		Redis: RedisConfig{
			URL:       strings.TrimSpace(getEnv("TRUST_REDIS_URL", "")),
			KeyPrefix: strings.TrimSpace(getEnv("TRUST_REDIS_KEY_PREFIX", "nostrmash")),
		},
		Concurrency:                  concurrency,
		ClaimBatchSize:               claimBatchSize,
		PollInterval:                 pollInterval,
		RetryDelay:                   retryDelay,
		JobRecovery:                  jobRecovery,
		JobRetention:                 jobRetention,
		EnableRedisSync:              getEnvBool("TRUST_ENABLE_REDIS_SYNC", false),
		EnableScoreCompute:           getEnvBool("TRUST_ENABLE_SCORE_COMPUTE", true),
		GraphSnapshotRefreshInterval: graphSnapshotRefreshInterval,
		RunSchedulerInterval:         runSchedulerInterval,
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
	if err := validateWorkerJobRecoveryConfig(cfg.JobRecovery); err != nil {
		return err
	}
	if cfg.JobRetention.Enabled {
		if cfg.JobRetention.SucceededMaxAge <= 0 {
			return fmt.Errorf("WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE must be > 0")
		}
		if cfg.JobRetention.DeadMaxAge <= 0 {
			return fmt.Errorf("WORKER_JOB_RETENTION_DEAD_MAX_AGE must be > 0")
		}
		if cfg.JobRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_JOB_RETENTION_RUN_INTERVAL must be > 0")
		}
		if cfg.JobRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.GraphSnapshotRefreshInterval <= 0 {
		return fmt.Errorf("TRUST_GRAPH_SNAPSHOT_REFRESH_INTERVAL must be > 0")
	}
	if cfg.RunSchedulerInterval <= 0 {
		return fmt.Errorf("TRUST_RUN_INTERVAL must be > 0")
	}
	if !cfg.EnableRedisSync && !cfg.EnableScoreCompute {
		return fmt.Errorf("at least one of TRUST_ENABLE_REDIS_SYNC or TRUST_ENABLE_SCORE_COMPUTE must be true")
	}
	if cfg.EnableRedisSync && strings.TrimSpace(cfg.Redis.URL) == "" {
		return fmt.Errorf("TRUST_REDIS_URL is required when TRUST_ENABLE_REDIS_SYNC=true")
	}
	if cfg.EnableRedisSync && strings.TrimSpace(cfg.Redis.KeyPrefix) == "" {
		return fmt.Errorf("TRUST_REDIS_KEY_PREFIX must not be empty when TRUST_ENABLE_REDIS_SYNC=true")
	}
	return nil
}
