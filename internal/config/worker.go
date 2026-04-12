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
	JobRecovery           WorkerJobRecoveryConfig
	JobRetention          WorkerJobRetentionConfig
	InvalidEventRetention WorkerInvalidEventRetentionConfig
	Meilisearch           MeilisearchConfig
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
	jobRecovery, err := loadWorkerJobRecoveryConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	succeededMaxAge, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	deadMaxAge, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_DEAD_MAX_AGE", 180*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_JOB_RETENTION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT", 500)
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
		Shared:      shared,
		Concurrency: concurrency,
		JobRecovery: jobRecovery,
		JobRetention: WorkerJobRetentionConfig{
			Enabled:          getEnvBool("WORKER_JOB_RETENTION_ENABLED", true),
			SucceededMaxAge:  succeededMaxAge,
			DeadMaxAge:       deadMaxAge,
			RunInterval:      runInterval,
			DeleteBatchLimit: deleteBatchLimit,
		},
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
	if err := validateWorkerConfig(cfg); err != nil {
		return WorkerConfig{}, err
	}
	return cfg, nil
}

func validateWorkerConfig(cfg WorkerConfig) error {
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("WORKER_CONCURRENCY must be > 0")
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
