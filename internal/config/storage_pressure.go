package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// StoragePressureLevel is the discrete escalation level derived from the
// database-size-to-capacity ratio. Higher levels trigger more aggressive
// (and more product-impacting) defensive actions. Canonical trusted/tracked
// data is never touched by any level.
type StoragePressureLevel int

const (
	// PressureNormal: nothing to do.
	PressureNormal StoragePressureLevel = 0
	// PressureWarn: alert/log only.
	PressureWarn StoragePressureLevel = 1
	// PressureAggressive: drain existing retention loops immediately and wire
	// trust-retention hooks into real deletes.
	PressureAggressive StoragePressureLevel = 2
	// PressureDisableHydration: refuse new on-demand hydration runs.
	PressureDisableHydration StoragePressureLevel = 3
	// PressurePauseCandidate: pause candidate-expanding ingest loops and flip
	// the gate to trusted_only. Open bootstrap kinds and canonical
	// trusted/tracked writes continue.
	PressurePauseCandidate StoragePressureLevel = 4
)

// StoragePressureConfig owns the storage governor knobs. It is part of
// SharedConfig because both the worker (which runs the governor and retention
// loops) and the ingestor (which reacts to PausCandidate) need it.
//
// Defaults are inert: with CapacityBytes == 0 the governor only observes the
// ratio (reported as 0) and takes no defensive action, so shipping this is
// behavior-neutral until an operator sets a capacity budget.
type StoragePressureConfig struct {
	// CapacityBytes is the operator's storage budget for the Postgres database
	// (e.g. ~600 GB SSD). 0 disables all governor actions.
	CapacityBytes int64
	// Percent thresholds for each escalation level (0..100).
	WarnPercent             int
	AggressivePercent       int
	DisableHydrationPercent int
	PauseCandidatePercent   int
	// RunInterval is how often the governor recomputes the level.
	RunInterval time.Duration
}

// Enabled reports whether the governor should take action (capacity configured).
func (c StoragePressureConfig) Enabled() bool {
	return c.CapacityBytes > 0
}

// Ratio returns databaseBytes/capacity, or 0 when capacity is unset.
func (c StoragePressureConfig) Ratio(databaseBytes int64) float64 {
	if c.CapacityBytes <= 0 {
		return 0
	}
	return float64(databaseBytes) / float64(c.CapacityBytes)
}

// Resolve maps a ratio to a discrete level using the configured percent
// thresholds. Returns PressureNormal when the governor is disabled.
func (c StoragePressureConfig) Resolve(ratio float64) StoragePressureLevel {
	if !c.Enabled() {
		return PressureNormal
	}
	pct := ratio * 100
	switch {
	case pct >= float64(c.PauseCandidatePercent):
		return PressurePauseCandidate
	case pct >= float64(c.DisableHydrationPercent):
		return PressureDisableHydration
	case pct >= float64(c.AggressivePercent):
		return PressureAggressive
	case pct >= float64(c.WarnPercent):
		return PressureWarn
	default:
		return PressureNormal
	}
}

func loadStoragePressureConfig() (StoragePressureConfig, error) {
	capacityRaw, err := getEnvNonNegativeInt64Strict("STORAGE_PRESSURE_CAPACITY_BYTES", 0)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	warn, err := getEnvPercentStrict("STORAGE_PRESSURE_WARN_PERCENT", 80)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	aggressive, err := getEnvPercentStrict("STORAGE_PRESSURE_AGGRESSIVE_PERCENT", 90)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	disableHydration, err := getEnvPercentStrict("STORAGE_PRESSURE_DISABLE_HYDRATION_PERCENT", 95)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	pauseCandidate, err := getEnvPercentStrict("STORAGE_PRESSURE_PAUSE_CANDIDATE_PERCENT", 98)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("STORAGE_PRESSURE_RUN_INTERVAL", 2*time.Minute)
	if err != nil {
		return StoragePressureConfig{}, err
	}
	cfg := StoragePressureConfig{
		CapacityBytes:           capacityRaw,
		WarnPercent:             warn,
		AggressivePercent:       aggressive,
		DisableHydrationPercent: disableHydration,
		PauseCandidatePercent:   pauseCandidate,
		RunInterval:             runInterval,
	}
	if cfg.Enabled() {
		if !(cfg.WarnPercent <= cfg.AggressivePercent &&
			cfg.AggressivePercent <= cfg.DisableHydrationPercent &&
			cfg.DisableHydrationPercent <= cfg.PauseCandidatePercent) {
			return StoragePressureConfig{}, fmt.Errorf("STORAGE_PRESSURE_*_PERCENT thresholds must be non-decreasing (warn <= aggressive <= disable_hydration <= pause_candidate)")
		}
	}
	return cfg, nil
}

func getEnvNonNegativeInt64Strict(key string, def int64) (int64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", key)
	}
	return v, nil
}

func getEnvPercentStrict(key string, def int) (int, error) {
	v, err := getEnvNonNegativeIntStrict(key, def)
	if err != nil {
		return 0, err
	}
	if v > 100 {
		return 0, fmt.Errorf("%s must be between 0 and 100", key)
	}
	return v, nil
}
