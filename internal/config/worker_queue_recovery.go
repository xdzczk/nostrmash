package config

import (
	"fmt"
	"time"
)

type WorkerJobRecoveryConfig struct {
	RunningTimeout          time.Duration
	StaleRecoveryInterval   time.Duration
	StaleRecoveryBatchLimit int
}

func loadWorkerJobRecoveryConfig() (WorkerJobRecoveryConfig, error) {
	runningTimeout, err := getEnvPositiveDurationStrict("WORKER_JOB_RUNNING_TIMEOUT", 15*time.Minute)
	if err != nil {
		return WorkerJobRecoveryConfig{}, err
	}
	staleRecoveryInterval, err := getEnvPositiveDurationStrict("WORKER_JOB_STALE_RECOVERY_INTERVAL", 30*time.Second)
	if err != nil {
		return WorkerJobRecoveryConfig{}, err
	}
	staleRecoveryBatchLimit, err := getEnvPositiveIntStrict("WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT", 100)
	if err != nil {
		return WorkerJobRecoveryConfig{}, err
	}
	cfg := WorkerJobRecoveryConfig{
		RunningTimeout:          runningTimeout,
		StaleRecoveryInterval:   staleRecoveryInterval,
		StaleRecoveryBatchLimit: staleRecoveryBatchLimit,
	}
	if err := validateWorkerJobRecoveryConfig(cfg); err != nil {
		return WorkerJobRecoveryConfig{}, err
	}
	return cfg, nil
}

func validateWorkerJobRecoveryConfig(cfg WorkerJobRecoveryConfig) error {
	if cfg.RunningTimeout <= 0 {
		return fmt.Errorf("WORKER_JOB_RUNNING_TIMEOUT must be > 0")
	}
	if cfg.StaleRecoveryInterval <= 0 {
		return fmt.Errorf("WORKER_JOB_STALE_RECOVERY_INTERVAL must be > 0")
	}
	if cfg.StaleRecoveryBatchLimit <= 0 {
		return fmt.Errorf("WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT must be > 0")
	}
	return nil
}
