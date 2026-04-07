package config

import (
	"fmt"
	"time"
)

type WorkerConfig struct {
	Shared                SharedConfig
	Concurrency           int
	JobRetention          WorkerJobRetentionConfig
	InvalidEventRetention WorkerInvalidEventRetentionConfig
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
	return nil
}
