package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type WorkerConfig struct {
	Shared                SharedConfig
	Concurrency           int
	LiveConcurrency       int
	BackfillConcurrency   int
	ClaimBatchSize        int
	JobRecovery           WorkerJobRecoveryConfig
	JobRetention          WorkerJobRetentionConfig
	InvalidEventRetention WorkerInvalidEventRetentionConfig
	Meilisearch           MeilisearchConfig
	RelayRegistry         RelayRegistryConfig
}

type WorkerJobRetentionConfig struct {
	Enabled          bool
	SucceededMaxAge  time.Duration
	DeadMaxAge       time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

type WorkerInvalidEventRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
	PayloadTrim      WorkerInvalidEventPayloadTrimConfig
}

type WorkerInvalidEventPayloadTrimConfig struct {
	Enabled    bool
	MaxAge     time.Duration
	BatchLimit int
}

func LoadWorker() (WorkerConfig, error) {
	shared, err := loadSharedConfig("worker")
	if err != nil {
		return WorkerConfig{}, err
	}
	concurrency, err := getEnvPositiveIntStrict("WORKER_CONCURRENCY", 4)
	if err != nil {
		return WorkerConfig{}, err
	}
	liveConcurrency, err := getEnvNonNegativeIntStrict("WORKER_LIVE_CONCURRENCY", concurrency)
	if err != nil {
		return WorkerConfig{}, err
	}
	backfillConcurrency, err := getEnvNonNegativeIntStrict("WORKER_BACKFILL_CONCURRENCY", concurrency)
	if err != nil {
		return WorkerConfig{}, err
	}
	claimBatchSize, err := getEnvPositiveIntStrict("WORKER_CLAIM_BATCH_SIZE", 10)
	if err != nil {
		return WorkerConfig{}, err
	}
	jobRecovery, err := loadWorkerJobRecoveryConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	jobRetention, err := loadWorkerJobRetentionConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	invalidRetentionMaxAge, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	invalidRetentionRunInterval, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	invalidRetentionDeleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT", 500)
	if err != nil {
		return WorkerConfig{}, err
	}
	invalidPayloadTrimMaxAge, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE", 7*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	invalidPayloadTrimBatchLimit, err := getEnvPositiveIntStrict("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT", 500)
	if err != nil {
		return WorkerConfig{}, err
	}
	cfg := WorkerConfig{
		Shared:              shared,
		Concurrency:         concurrency,
		LiveConcurrency:     liveConcurrency,
		BackfillConcurrency: backfillConcurrency,
		ClaimBatchSize:      claimBatchSize,
		JobRecovery:         jobRecovery,
		JobRetention:        jobRetention,
		InvalidEventRetention: WorkerInvalidEventRetentionConfig{
			Enabled:          getEnvBool("WORKER_INVALID_EVENTS_RETENTION_ENABLED", true),
			MaxAge:           invalidRetentionMaxAge,
			RunInterval:      invalidRetentionRunInterval,
			DeleteBatchLimit: invalidRetentionDeleteBatchLimit,
			PayloadTrim: WorkerInvalidEventPayloadTrimConfig{
				Enabled:    getEnvBool("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED", true),
				MaxAge:     invalidPayloadTrimMaxAge,
				BatchLimit: invalidPayloadTrimBatchLimit,
			},
		},
		Meilisearch: MeilisearchConfig{
			Enabled:      getEnvBool("MEILI_ENABLED", false),
			URL:          getEnv("MEILI_URL", ""),
			MasterKey:    getEnv("MEILI_MASTER_KEY", ""),
			SearchAPIKey: getEnv("MEILI_SEARCH_API_KEY", ""),
		},
	}
	cfg.Meilisearch.URL = strings.TrimSpace(cfg.Meilisearch.URL)
	cfg.Meilisearch.MasterKey = strings.TrimSpace(cfg.Meilisearch.MasterKey)
	cfg.Meilisearch.SearchAPIKey = strings.TrimSpace(cfg.Meilisearch.SearchAPIKey)
	if cfg.Meilisearch.SearchAPIKey == "" {
		cfg.Meilisearch.SearchAPIKey = cfg.Meilisearch.MasterKey
	}
	relayRegistryCfg, err := LoadRelayRegistryConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	cfg.RelayRegistry = relayRegistryCfg

	if err := validateWorkerConfig(cfg); err != nil {
		return WorkerConfig{}, err
	}
	return cfg, nil
}

// loadWorkerJobRetentionConfig reads the WORKER_JOB_RETENTION_* envs and
// returns the populated config. Called by both LoadWorker and LoadTrustWorker
// so that the trust worker (which previously did not run the retention loop
// at all) shares the same retention semantics. Defaults are tuned for the
// observed steady-state job volume: succeeded jobs are queue exhaust and only
// matter for live debugging, dead jobs deserve a longer triage window.
func loadWorkerJobRetentionConfig() (WorkerJobRetentionConfig, error) {
	succeededMaxAge, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE", 24*time.Hour)
	if err != nil {
		return WorkerJobRetentionConfig{}, err
	}
	deadMaxAge, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_DEAD_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerJobRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_RUN_INTERVAL", 15*time.Minute)
	if err != nil {
		return WorkerJobRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerJobRetentionConfig{}, err
	}
	return WorkerJobRetentionConfig{
		Enabled:          getEnvBool("WORKER_JOB_RETENTION_ENABLED", true),
		SucceededMaxAge:  succeededMaxAge,
		DeadMaxAge:       deadMaxAge,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func validateWorkerConfig(cfg WorkerConfig) error {
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("WORKER_CONCURRENCY must be > 0")
	}
	if cfg.LiveConcurrency < 0 {
		return fmt.Errorf("WORKER_LIVE_CONCURRENCY must be >= 0")
	}
	if cfg.BackfillConcurrency < 0 {
		return fmt.Errorf("WORKER_BACKFILL_CONCURRENCY must be >= 0")
	}
	if cfg.ClaimBatchSize <= 0 {
		return fmt.Errorf("WORKER_CLAIM_BATCH_SIZE must be > 0")
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
	if cfg.InvalidEventRetention.Enabled {
		if cfg.InvalidEventRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE must be > 0")
		}
		if cfg.InvalidEventRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL must be > 0")
		}
		if cfg.InvalidEventRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT must be > 0")
		}
		if cfg.InvalidEventRetention.PayloadTrim.Enabled {
			if cfg.InvalidEventRetention.PayloadTrim.MaxAge <= 0 {
				return fmt.Errorf("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE must be > 0")
			}
			if cfg.InvalidEventRetention.PayloadTrim.BatchLimit <= 0 {
				return fmt.Errorf("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT must be > 0")
			}
			if cfg.InvalidEventRetention.PayloadTrim.MaxAge >= cfg.InvalidEventRetention.MaxAge {
				return fmt.Errorf("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE must be smaller than WORKER_INVALID_EVENTS_RETENTION_MAX_AGE")
			}
		}
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
