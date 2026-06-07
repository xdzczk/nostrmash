package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

type WorkerConfig struct {
	Shared                 SharedConfig
	Concurrency            int
	LiveConcurrency        int
	BackfillConcurrency    int
	ClaimBatchSize         int
	JobRecovery            WorkerJobRecoveryConfig
	JobRetention           WorkerJobRetentionConfig
	InvalidEventRetention  WorkerInvalidEventRetentionConfig
	EngagementRetention    WorkerEngagementRetentionConfig
	ReplaceableRetention   WorkerReplaceableRetentionConfig
	DeletionRetention      WorkerDeletionRetentionConfig
	AuthorAnalyticsSweeper WorkerAuthorAnalyticsSweeperConfig
	ProfileStatsSweeper    WorkerProfileStatsSweeperConfig
	MeilisearchSweeper     WorkerMeilisearchSweeperConfig
	Meilisearch            MeilisearchConfig
	RelayRegistry          RelayRegistryConfig
}

// WorkerAuthorAnalyticsSweeperConfig configures the background loop that
// drains pending_author_analytics_recomputes. The per-event derive bundle
// only marks affected pubkeys as dirty (cheap upsert); this sweeper runs
// the heavy aggregation rebuild once per pubkey per cycle, naturally
// coalescing event bursts into a single rebuild.
//
// WindowsDays controls which window_days values the per-pubkey rebuild
// recomputes. The schema permits {7, 30, 90}; the default of {7, 30}
// drops the most expensive window because each rebuild's cost is roughly
// linear in the window count and dominated by the 90d scan. Operators
// who need the 90d window refreshed in real time can set
// WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS=7,30,90.
//
// RebuildTimeout caps how long a single per-pubkey rebuild can hold its
// transaction (and therefore its pgxpool connection). On timeout the
// transaction rolls back, the per-pubkey advisory lock auto-releases,
// and the pending row is restored — so the pubkey is automatically
// retried on the next sweeper cycle. This is a safety net against any
// single hot pubkey monopolizing a connection long enough to starve
// bundle workers.
type WorkerAuthorAnalyticsSweeperConfig struct {
	Enabled        bool
	Interval       time.Duration
	BatchSize      int
	Concurrency    int
	WindowsDays    []int
	RebuildTimeout time.Duration
}

// WorkerProfileStatsSweeperConfig configures the background loop that
// drains pending_profile_stats_recomputes. The per-event derive bundle
// only marks affected pubkeys as dirty (cheap upsert); this sweeper
// runs the heavy ProjectProfilePublicStats and
// ProjectProfileDiscoveryStats recompute once per pubkey per cycle,
// coalescing event bursts into a single recompute and removing the
// per-pubkey advisory-lock contention from the bundle critical path.
type WorkerProfileStatsSweeperConfig struct {
	Enabled     bool
	Interval    time.Duration
	BatchSize   int
	Concurrency int
}

// WorkerMeilisearchSweeperConfig configures the background loop that
// drains pending_meilisearch_syncs. The per-event derive bundle only
// records that an event needs indexing (cheap upsert); this sweeper
// performs the actual HTTP sync to Meilisearch out of band so a slow
// Meilisearch never caps live-pool throughput.
type WorkerMeilisearchSweeperConfig struct {
	Enabled     bool
	Interval    time.Duration
	BatchSize   int
	Concurrency int
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

// WorkerEngagementRetentionConfig configures the purger that deletes raw
// engagement events (kinds 6/7/9735) older than MaxAge. Lifetime aggregate
// counters (reaction_counts/repost_counts have no FK to events) survive the
// delete; only the high-volume raw rows and their cascade-cleaned
// contribution/interaction rows are removed.
//
// DeadGrace is the derivation-safety window: a raw event is never purged while
// its derive_event_bundle job is pending/running, nor while that job is dead
// and was last updated within DeadGrace. Past DeadGrace a permanently-dead
// derivation no longer blocks cleanup, so one broken path cannot pin disk
// forever.
type WorkerEngagementRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// WorkerReplaceableRetentionConfig configures the purger that deletes raw
// replaceable events (kinds 0/3/10002) that have been strictly superseded by a
// newer winner. The latest-version projections (contact_lists_latest,
// relay_lists_latest, profiles_latest, replaceable_state) all reference the
// winner, so only superseded versions are removed and the read models survive.
//
// MinAge is the stability window applied to events.first_seen_at: a superseded
// version is only eligible once it has existed for at least this long, so a
// freshly-ingested version (including backfilled events with an ancient
// author-claimed created_at) is never purged immediately after being
// superseded.
//
// DeadGrace is the derivation-safety window: a candidate is never purged while
// its derive_event_bundle job is pending/running, nor while that job is dead
// and was last updated within DeadGrace.
type WorkerReplaceableRetentionConfig struct {
	Enabled          bool
	MinAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// WorkerDeletionRetentionConfig configures the purger that deletes raw deletion
// events (kind 5) older than MaxAge once their derivation has completed. The
// distilled deletion_events ledger row survives (migration 000050 dropped the
// events FK cascade), so tombstone knowledge is preserved while the
// high-volume raw rows and their cascade-cleaned tags/references are removed.
//
// DeadGrace is the derivation-safety window: a raw event is never purged while
// its derive_event_bundle job is pending/running, nor while that job is dead
// and was last updated within DeadGrace.
type WorkerDeletionRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
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
	engagementRetentionMaxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	engagementRetentionDeadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	engagementRetentionRunInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	engagementRetentionDeleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_ENGAGEMENT_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerConfig{}, err
	}
	replaceableRetentionMinAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_MIN_AGE", 24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	replaceableRetentionDeadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	replaceableRetentionRunInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	replaceableRetentionDeleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_REPLACEABLE_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerConfig{}, err
	}
	deletionRetentionMaxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	deletionRetentionDeadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	deletionRetentionRunInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerConfig{}, err
	}
	deletionRetentionDeleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_DELETION_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerConfig{}, err
	}
	authorAnalyticsSweeperInterval, err := getEnvPositiveDurationStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_INTERVAL", 5*time.Second)
	if err != nil {
		return WorkerConfig{}, err
	}
	authorAnalyticsSweeperBatch, err := getEnvPositiveIntStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_BATCH_SIZE", 25)
	if err != nil {
		return WorkerConfig{}, err
	}
	authorAnalyticsSweeperConcurrency, err := getEnvPositiveIntStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_CONCURRENCY", 4)
	if err != nil {
		return WorkerConfig{}, err
	}
	authorAnalyticsWindowsDays, err := getEnvIntListStrict("WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS", []int{7, 30})
	if err != nil {
		return WorkerConfig{}, err
	}
	authorAnalyticsRebuildTimeout, err := getEnvPositiveDurationStrict("WORKER_AUTHOR_ANALYTICS_REBUILD_TIMEOUT", 90*time.Second)
	if err != nil {
		return WorkerConfig{}, err
	}
	profileStatsSweeperInterval, err := getEnvPositiveDurationStrict("WORKER_PROFILE_STATS_SWEEPER_INTERVAL", 5*time.Second)
	if err != nil {
		return WorkerConfig{}, err
	}
	profileStatsSweeperBatch, err := getEnvPositiveIntStrict("WORKER_PROFILE_STATS_SWEEPER_BATCH_SIZE", 25)
	if err != nil {
		return WorkerConfig{}, err
	}
	profileStatsSweeperConcurrency, err := getEnvPositiveIntStrict("WORKER_PROFILE_STATS_SWEEPER_CONCURRENCY", 4)
	if err != nil {
		return WorkerConfig{}, err
	}
	meilisearchSweeperInterval, err := getEnvPositiveDurationStrict("WORKER_MEILISEARCH_SWEEPER_INTERVAL", 2*time.Second)
	if err != nil {
		return WorkerConfig{}, err
	}
	meilisearchSweeperBatch, err := getEnvPositiveIntStrict("WORKER_MEILISEARCH_SWEEPER_BATCH_SIZE", 50)
	if err != nil {
		return WorkerConfig{}, err
	}
	meilisearchSweeperConcurrency, err := getEnvPositiveIntStrict("WORKER_MEILISEARCH_SWEEPER_CONCURRENCY", 4)
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
		EngagementRetention: WorkerEngagementRetentionConfig{
			Enabled:          getEnvBool("WORKER_RETENTION_ENGAGEMENT_ENABLED", true),
			MaxAge:           engagementRetentionMaxAge,
			DeadGrace:        engagementRetentionDeadGrace,
			RunInterval:      engagementRetentionRunInterval,
			DeleteBatchLimit: engagementRetentionDeleteBatchLimit,
		},
		ReplaceableRetention: WorkerReplaceableRetentionConfig{
			Enabled:          getEnvBool("WORKER_RETENTION_REPLACEABLE_ENABLED", true),
			MinAge:           replaceableRetentionMinAge,
			DeadGrace:        replaceableRetentionDeadGrace,
			RunInterval:      replaceableRetentionRunInterval,
			DeleteBatchLimit: replaceableRetentionDeleteBatchLimit,
		},
		DeletionRetention: WorkerDeletionRetentionConfig{
			Enabled:          getEnvBool("WORKER_RETENTION_DELETION_ENABLED", true),
			MaxAge:           deletionRetentionMaxAge,
			DeadGrace:        deletionRetentionDeadGrace,
			RunInterval:      deletionRetentionRunInterval,
			DeleteBatchLimit: deletionRetentionDeleteBatchLimit,
		},
		AuthorAnalyticsSweeper: WorkerAuthorAnalyticsSweeperConfig{
			Enabled:        getEnvBool("WORKER_AUTHOR_ANALYTICS_SWEEPER_ENABLED", true),
			Interval:       authorAnalyticsSweeperInterval,
			BatchSize:      authorAnalyticsSweeperBatch,
			Concurrency:    authorAnalyticsSweeperConcurrency,
			WindowsDays:    authorAnalyticsWindowsDays,
			RebuildTimeout: authorAnalyticsRebuildTimeout,
		},
		ProfileStatsSweeper: WorkerProfileStatsSweeperConfig{
			Enabled:     getEnvBool("WORKER_PROFILE_STATS_SWEEPER_ENABLED", true),
			Interval:    profileStatsSweeperInterval,
			BatchSize:   profileStatsSweeperBatch,
			Concurrency: profileStatsSweeperConcurrency,
		},
		MeilisearchSweeper: WorkerMeilisearchSweeperConfig{
			Enabled:     getEnvBool("WORKER_MEILISEARCH_SWEEPER_ENABLED", true),
			Interval:    meilisearchSweeperInterval,
			BatchSize:   meilisearchSweeperBatch,
			Concurrency: meilisearchSweeperConcurrency,
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
	if cfg.EngagementRetention.Enabled {
		if cfg.EngagementRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_ENGAGEMENT_MAX_AGE must be > 0")
		}
		if cfg.EngagementRetention.DeadGrace <= 0 {
			return fmt.Errorf("WORKER_RETENTION_ENGAGEMENT_DEAD_GRACE must be > 0")
		}
		if cfg.EngagementRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_ENGAGEMENT_RUN_INTERVAL must be > 0")
		}
		if cfg.EngagementRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_ENGAGEMENT_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.ReplaceableRetention.Enabled {
		if cfg.ReplaceableRetention.MinAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_REPLACEABLE_MIN_AGE must be > 0")
		}
		if cfg.ReplaceableRetention.DeadGrace <= 0 {
			return fmt.Errorf("WORKER_RETENTION_REPLACEABLE_DEAD_GRACE must be > 0")
		}
		if cfg.ReplaceableRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_REPLACEABLE_RUN_INTERVAL must be > 0")
		}
		if cfg.ReplaceableRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_REPLACEABLE_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.DeletionRetention.Enabled {
		if cfg.DeletionRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_DELETION_MAX_AGE must be > 0")
		}
		if cfg.DeletionRetention.DeadGrace <= 0 {
			return fmt.Errorf("WORKER_RETENTION_DELETION_DEAD_GRACE must be > 0")
		}
		if cfg.DeletionRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_DELETION_RUN_INTERVAL must be > 0")
		}
		if cfg.DeletionRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_DELETION_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.AuthorAnalyticsSweeper.Enabled {
		if cfg.AuthorAnalyticsSweeper.Interval <= 0 {
			return fmt.Errorf("WORKER_AUTHOR_ANALYTICS_SWEEPER_INTERVAL must be > 0")
		}
		if cfg.AuthorAnalyticsSweeper.BatchSize <= 0 {
			return fmt.Errorf("WORKER_AUTHOR_ANALYTICS_SWEEPER_BATCH_SIZE must be > 0")
		}
		if cfg.AuthorAnalyticsSweeper.Concurrency <= 0 {
			return fmt.Errorf("WORKER_AUTHOR_ANALYTICS_SWEEPER_CONCURRENCY must be > 0")
		}
	}
	if cfg.ProfileStatsSweeper.Enabled {
		if cfg.ProfileStatsSweeper.Interval <= 0 {
			return fmt.Errorf("WORKER_PROFILE_STATS_SWEEPER_INTERVAL must be > 0")
		}
		if cfg.ProfileStatsSweeper.BatchSize <= 0 {
			return fmt.Errorf("WORKER_PROFILE_STATS_SWEEPER_BATCH_SIZE must be > 0")
		}
		if cfg.ProfileStatsSweeper.Concurrency <= 0 {
			return fmt.Errorf("WORKER_PROFILE_STATS_SWEEPER_CONCURRENCY must be > 0")
		}
	}
	if cfg.MeilisearchSweeper.Enabled {
		if cfg.MeilisearchSweeper.Interval <= 0 {
			return fmt.Errorf("WORKER_MEILISEARCH_SWEEPER_INTERVAL must be > 0")
		}
		if cfg.MeilisearchSweeper.BatchSize <= 0 {
			return fmt.Errorf("WORKER_MEILISEARCH_SWEEPER_BATCH_SIZE must be > 0")
		}
		if cfg.MeilisearchSweeper.Concurrency <= 0 {
			return fmt.Errorf("WORKER_MEILISEARCH_SWEEPER_CONCURRENCY must be > 0")
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
