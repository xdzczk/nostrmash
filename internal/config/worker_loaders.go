package config

import (
	"strings"
	"time"
)

// workerConcurrency bundles the top-level worker concurrency knobs.
type workerConcurrency struct {
	Concurrency         int
	LiveConcurrency     int
	BackfillConcurrency int
	ClaimBatchSize      int
}

func loadWorkerConcurrency() (workerConcurrency, error) {
	concurrency, err := getEnvPositiveIntStrict("WORKER_CONCURRENCY", 4)
	if err != nil {
		return workerConcurrency{}, err
	}
	liveConcurrency, err := getEnvNonNegativeIntStrict("WORKER_LIVE_CONCURRENCY", concurrency)
	if err != nil {
		return workerConcurrency{}, err
	}
	backfillConcurrency, err := getEnvNonNegativeIntStrict("WORKER_BACKFILL_CONCURRENCY", concurrency)
	if err != nil {
		return workerConcurrency{}, err
	}
	claimBatchSize, err := getEnvPositiveIntStrict("WORKER_CLAIM_BATCH_SIZE", 10)
	if err != nil {
		return workerConcurrency{}, err
	}
	return workerConcurrency{
		Concurrency:         concurrency,
		LiveConcurrency:     liveConcurrency,
		BackfillConcurrency: backfillConcurrency,
		ClaimBatchSize:      claimBatchSize,
	}, nil
}

// workerRetentionConfigs bundles every worker retention/purge sub-config so the
// retention module can be loaded and reasoned about as one unit.
type workerRetentionConfigs struct {
	InvalidEvent      WorkerInvalidEventRetentionConfig
	Engagement        WorkerEngagementRetentionConfig
	Replaceable       WorkerReplaceableRetentionConfig
	Deletion          WorkerDeletionRetentionConfig
	UntrustedAuthor   WorkerUntrustedAuthorRetentionConfig
	AuthorRecent      WorkerAuthorRecentRetentionConfig
	SearchDocs        WorkerSearchDocsRetentionConfig
	EventRelays       WorkerEventRelaysRetentionConfig
	EventTags         WorkerEventTagsRetentionConfig
	AppliedStatDeltas WorkerAppliedStatDeltasRetentionConfig
	TrustRetention    WorkerTrustRetentionLoopConfig
}

func loadWorkerRetentionConfigs() (workerRetentionConfigs, error) {
	var out workerRetentionConfigs
	var err error

	if out.InvalidEvent, err = loadInvalidEventRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.Engagement, err = loadEngagementRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.Replaceable, err = loadReplaceableRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.Deletion, err = loadDeletionRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.UntrustedAuthor, err = loadUntrustedAuthorRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.AuthorRecent, err = loadAuthorRecentRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.SearchDocs, err = loadSearchDocsRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.EventRelays, err = loadEventRelaysRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.EventTags, err = loadEventTagsRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.AppliedStatDeltas, err = loadAppliedStatDeltasRetentionConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	if out.TrustRetention, err = loadTrustRetentionLoopConfig(); err != nil {
		return workerRetentionConfigs{}, err
	}
	return out, nil
}

func loadInvalidEventRetentionConfig() (WorkerInvalidEventRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerInvalidEventRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerInvalidEventRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT", 500)
	if err != nil {
		return WorkerInvalidEventRetentionConfig{}, err
	}
	payloadTrimMaxAge, err := getEnvPositiveDurationStrict("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE", 7*24*time.Hour)
	if err != nil {
		return WorkerInvalidEventRetentionConfig{}, err
	}
	payloadTrimBatchLimit, err := getEnvPositiveIntStrict("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT", 500)
	if err != nil {
		return WorkerInvalidEventRetentionConfig{}, err
	}
	return WorkerInvalidEventRetentionConfig{
		Enabled:          getEnvBool("WORKER_INVALID_EVENTS_RETENTION_ENABLED", true),
		MaxAge:           maxAge,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
		PayloadTrim: WorkerInvalidEventPayloadTrimConfig{
			Enabled:    getEnvBool("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED", true),
			MaxAge:     payloadTrimMaxAge,
			BatchLimit: payloadTrimBatchLimit,
		},
	}, nil
}

func loadEngagementRetentionConfig() (WorkerEngagementRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerEngagementRetentionConfig{}, err
	}
	deadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerEngagementRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_ENGAGEMENT_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerEngagementRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_ENGAGEMENT_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerEngagementRetentionConfig{}, err
	}
	return WorkerEngagementRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_ENGAGEMENT_ENABLED", true),
		MaxAge:           maxAge,
		DeadGrace:        deadGrace,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadReplaceableRetentionConfig() (WorkerReplaceableRetentionConfig, error) {
	minAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_MIN_AGE", 24*time.Hour)
	if err != nil {
		return WorkerReplaceableRetentionConfig{}, err
	}
	deadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerReplaceableRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_REPLACEABLE_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerReplaceableRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_REPLACEABLE_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerReplaceableRetentionConfig{}, err
	}
	return WorkerReplaceableRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_REPLACEABLE_ENABLED", true),
		MinAge:           minAge,
		DeadGrace:        deadGrace,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadDeletionRetentionConfig() (WorkerDeletionRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerDeletionRetentionConfig{}, err
	}
	deadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerDeletionRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_DELETION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerDeletionRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_DELETION_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerDeletionRetentionConfig{}, err
	}
	return WorkerDeletionRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_DELETION_ENABLED", true),
		MaxAge:           maxAge,
		DeadGrace:        deadGrace,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadUntrustedAuthorRetentionConfig() (WorkerUntrustedAuthorRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_UNTRUSTED_AUTHOR_MAX_AGE", 14*24*time.Hour)
	if err != nil {
		return WorkerUntrustedAuthorRetentionConfig{}, err
	}
	deadGrace, err := getEnvPositiveDurationStrict("WORKER_RETENTION_UNTRUSTED_AUTHOR_DEAD_GRACE", 7*24*time.Hour)
	if err != nil {
		return WorkerUntrustedAuthorRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_UNTRUSTED_AUTHOR_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerUntrustedAuthorRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_UNTRUSTED_AUTHOR_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerUntrustedAuthorRetentionConfig{}, err
	}
	return WorkerUntrustedAuthorRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_UNTRUSTED_AUTHOR_ENABLED", true),
		MaxAge:           maxAge,
		DeadGrace:        deadGrace,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadAuthorRecentRetentionConfig() (WorkerAuthorRecentRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_AUTHOR_RECENT_MAX_AGE", 90*24*time.Hour)
	if err != nil {
		return WorkerAuthorRecentRetentionConfig{}, err
	}
	perAuthorCap, err := getEnvPositiveIntStrict("WORKER_RETENTION_AUTHOR_RECENT_PER_AUTHOR_CAP", 200)
	if err != nil {
		return WorkerAuthorRecentRetentionConfig{}, err
	}
	authorBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_AUTHOR_RECENT_AUTHOR_BATCH_LIMIT", 500)
	if err != nil {
		return WorkerAuthorRecentRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_AUTHOR_RECENT_RUN_INTERVAL", 6*time.Hour)
	if err != nil {
		return WorkerAuthorRecentRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_AUTHOR_RECENT_DELETE_BATCH_LIMIT", 5000)
	if err != nil {
		return WorkerAuthorRecentRetentionConfig{}, err
	}
	return WorkerAuthorRecentRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_AUTHOR_RECENT_ENABLED", true),
		MaxAge:           maxAge,
		PerAuthorCap:     perAuthorCap,
		AuthorBatchLimit: authorBatchLimit,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadSearchDocsRetentionConfig() (WorkerSearchDocsRetentionConfig, error) {
	bodyMaxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_SEARCH_DOCS_BODY_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerSearchDocsRetentionConfig{}, err
	}
	bodyMaxChars, err := getEnvPositiveIntStrict("WORKER_RETENTION_SEARCH_DOCS_BODY_MAX_CHARS", 280)
	if err != nil {
		return WorkerSearchDocsRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_SEARCH_DOCS_RUN_INTERVAL", 6*time.Hour)
	if err != nil {
		return WorkerSearchDocsRetentionConfig{}, err
	}
	batchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_SEARCH_DOCS_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerSearchDocsRetentionConfig{}, err
	}
	return WorkerSearchDocsRetentionConfig{
		Enabled:      getEnvBool("WORKER_RETENTION_SEARCH_DOCS_ENABLED", true),
		BodyMaxAge:   bodyMaxAge,
		BodyMaxChars: bodyMaxChars,
		RunInterval:  runInterval,
		BatchLimit:   batchLimit,
	}, nil
}

func loadEventRelaysRetentionConfig() (WorkerEventRelaysRetentionConfig, error) {
	maxAge, err := getEnvPositiveDurationStrict("WORKER_RETENTION_EVENT_RELAYS_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerEventRelaysRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_EVENT_RELAYS_RUN_INTERVAL", 6*time.Hour)
	if err != nil {
		return WorkerEventRelaysRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_EVENT_RELAYS_DELETE_BATCH_LIMIT", 5000)
	if err != nil {
		return WorkerEventRelaysRetentionConfig{}, err
	}
	return WorkerEventRelaysRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_EVENT_RELAYS_ENABLED", true),
		MaxAge:           maxAge,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

// loadEventTagsRetentionConfig reads WORKER_RETENTION_EVENT_TAGS_*. Defaults
// are aggressive (5m / 20k) so the historical backlog of kind-3 p-tags and
// junk names drains quickly after deploy; once empty each tick is a no-op.
func loadEventTagsRetentionConfig() (WorkerEventTagsRetentionConfig, error) {
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_EVENT_TAGS_RUN_INTERVAL", 5*time.Minute)
	if err != nil {
		return WorkerEventTagsRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_EVENT_TAGS_DELETE_BATCH_LIMIT", 20000)
	if err != nil {
		return WorkerEventTagsRetentionConfig{}, err
	}
	return WorkerEventTagsRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_EVENT_TAGS_ENABLED", true),
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

// loadAppliedStatDeltasRetentionConfig reads the
// WORKER_RETENTION_APPLIED_STAT_DELTAS_* envs. The default grace period
// (1h) is generous padding on top of the orphan-only pruning condition
// (see internal/store/retention/applied_stat_deltas_retention.go), not a
// tuned correctness horizon; run interval matches the other low-priority
// hygiene loops (event_relays, search_docs, author_recent).
func loadAppliedStatDeltasRetentionConfig() (WorkerAppliedStatDeltasRetentionConfig, error) {
	gracePeriod, err := getEnvPositiveDurationStrict("WORKER_RETENTION_APPLIED_STAT_DELTAS_GRACE_PERIOD", 1*time.Hour)
	if err != nil {
		return WorkerAppliedStatDeltasRetentionConfig{}, err
	}
	runInterval, err := getEnvPositiveDurationStrict("WORKER_RETENTION_APPLIED_STAT_DELTAS_RUN_INTERVAL", 6*time.Hour)
	if err != nil {
		return WorkerAppliedStatDeltasRetentionConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("WORKER_RETENTION_APPLIED_STAT_DELTAS_DELETE_BATCH_LIMIT", 5000)
	if err != nil {
		return WorkerAppliedStatDeltasRetentionConfig{}, err
	}
	return WorkerAppliedStatDeltasRetentionConfig{
		Enabled:          getEnvBool("WORKER_RETENTION_APPLIED_STAT_DELTAS_ENABLED", true),
		GracePeriod:      gracePeriod,
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

func loadTrustRetentionLoopConfig() (WorkerTrustRetentionLoopConfig, error) {
	runInterval, err := getEnvPositiveDurationStrict("TRUST_RETENTION_RUN_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerTrustRetentionLoopConfig{}, err
	}
	deleteBatchLimit, err := getEnvPositiveIntStrict("TRUST_RETENTION_DELETE_BATCH_LIMIT", 2000)
	if err != nil {
		return WorkerTrustRetentionLoopConfig{}, err
	}
	return WorkerTrustRetentionLoopConfig{
		RunInterval:      runInterval,
		DeleteBatchLimit: deleteBatchLimit,
	}, nil
}

// workerSweeperConfigs bundles the out-of-band projection sweepers.
type workerSweeperConfigs struct {
	AuthorAnalytics WorkerAuthorAnalyticsSweeperConfig
	ProfileStats    WorkerProfileStatsSweeperConfig
	Meilisearch     WorkerMeilisearchSweeperConfig
}

func loadWorkerSweeperConfigs() (workerSweeperConfigs, error) {
	var out workerSweeperConfigs
	var err error
	if out.AuthorAnalytics, err = loadAuthorAnalyticsSweeperConfig(); err != nil {
		return workerSweeperConfigs{}, err
	}
	if out.ProfileStats, err = loadProfileStatsSweeperConfig(); err != nil {
		return workerSweeperConfigs{}, err
	}
	if out.Meilisearch, err = loadMeilisearchSweeperConfig(); err != nil {
		return workerSweeperConfigs{}, err
	}
	return out, nil
}

func loadWorkerIncrementalStatsConfig() (WorkerIncrementalStatsConfig, error) {
	reconciliation, err := loadIncrementalStatsReconciliationConfig()
	if err != nil {
		return WorkerIncrementalStatsConfig{}, err
	}
	return WorkerIncrementalStatsConfig{
		ProfilePublicStats:    getEnvBool("WORKER_INCREMENTAL_PROFILE_PUBLIC_STATS", true),
		AuthorActivityDaily:   getEnvBool("WORKER_INCREMENTAL_AUTHOR_ACTIVITY_DAILY", true),
		WindowedRollups:       getEnvBool("WORKER_INCREMENTAL_WINDOWED_ROLLUPS", true),
		ProfileDiscoveryStats: getEnvBool("WORKER_INCREMENTAL_PROFILE_DISCOVERY_STATS", true),
		Reconciliation:        reconciliation,
	}, nil
}

// loadIncrementalStatsReconciliationConfig reads the
// WORKER_INCREMENTAL_STATS_RECONCILIATION_* envs. Default interval (1h) and
// sample size (200) are deliberately modest: this is a low-priority
// correctness backstop, not the steady-state path, so it should cost
// negligible DB time even on a busy instance.
func loadIncrementalStatsReconciliationConfig() (WorkerIncrementalStatsReconciliationConfig, error) {
	interval, err := getEnvPositiveDurationStrict("WORKER_INCREMENTAL_STATS_RECONCILIATION_INTERVAL", 1*time.Hour)
	if err != nil {
		return WorkerIncrementalStatsReconciliationConfig{}, err
	}
	sampleSize, err := getEnvPositiveIntStrict("WORKER_INCREMENTAL_STATS_RECONCILIATION_SAMPLE_SIZE", 200)
	if err != nil {
		return WorkerIncrementalStatsReconciliationConfig{}, err
	}
	return WorkerIncrementalStatsReconciliationConfig{
		Enabled:    getEnvBool("WORKER_INCREMENTAL_STATS_RECONCILIATION_ENABLED", true),
		Interval:   interval,
		SampleSize: sampleSize,
	}, nil
}

func loadAuthorAnalyticsSweeperConfig() (WorkerAuthorAnalyticsSweeperConfig, error) {
	interval, err := getEnvPositiveDurationStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_INTERVAL", 5*time.Second)
	if err != nil {
		return WorkerAuthorAnalyticsSweeperConfig{}, err
	}
	batch, err := getEnvPositiveIntStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_BATCH_SIZE", 25)
	if err != nil {
		return WorkerAuthorAnalyticsSweeperConfig{}, err
	}
	concurrency, err := getEnvPositiveIntStrict("WORKER_AUTHOR_ANALYTICS_SWEEPER_CONCURRENCY", 4)
	if err != nil {
		return WorkerAuthorAnalyticsSweeperConfig{}, err
	}
	windowsDays, err := getEnvIntListStrict("WORKER_AUTHOR_ANALYTICS_WINDOWS_DAYS", []int{7, 30})
	if err != nil {
		return WorkerAuthorAnalyticsSweeperConfig{}, err
	}
	// 300s: after VACUUM ANALYZE restored index-friendly plans, typical
	// pubkeys finish well under 90s, but whale authors (tens of thousands of
	// events) still need 2–4 minutes for the all-history daily rebuild. The
	// previous 90s default caused perpetual timeout→retry loops that wasted
	// disk I/O without draining pending_author_analytics_recomputes.
	rebuildTimeout, err := getEnvPositiveDurationStrict("WORKER_AUTHOR_ANALYTICS_REBUILD_TIMEOUT", 300*time.Second)
	if err != nil {
		return WorkerAuthorAnalyticsSweeperConfig{}, err
	}
	return WorkerAuthorAnalyticsSweeperConfig{
		Enabled:        getEnvBool("WORKER_AUTHOR_ANALYTICS_SWEEPER_ENABLED", true),
		Interval:       interval,
		BatchSize:      batch,
		Concurrency:    concurrency,
		WindowsDays:    windowsDays,
		RebuildTimeout: rebuildTimeout,
	}, nil
}

func loadProfileStatsSweeperConfig() (WorkerProfileStatsSweeperConfig, error) {
	interval, err := getEnvPositiveDurationStrict("WORKER_PROFILE_STATS_SWEEPER_INTERVAL", 5*time.Second)
	if err != nil {
		return WorkerProfileStatsSweeperConfig{}, err
	}
	batch, err := getEnvPositiveIntStrict("WORKER_PROFILE_STATS_SWEEPER_BATCH_SIZE", 25)
	if err != nil {
		return WorkerProfileStatsSweeperConfig{}, err
	}
	concurrency, err := getEnvPositiveIntStrict("WORKER_PROFILE_STATS_SWEEPER_CONCURRENCY", 4)
	if err != nil {
		return WorkerProfileStatsSweeperConfig{}, err
	}
	return WorkerProfileStatsSweeperConfig{
		Enabled:     getEnvBool("WORKER_PROFILE_STATS_SWEEPER_ENABLED", true),
		Interval:    interval,
		BatchSize:   batch,
		Concurrency: concurrency,
	}, nil
}

func loadMeilisearchSweeperConfig() (WorkerMeilisearchSweeperConfig, error) {
	interval, err := getEnvPositiveDurationStrict("WORKER_MEILISEARCH_SWEEPER_INTERVAL", 2*time.Second)
	if err != nil {
		return WorkerMeilisearchSweeperConfig{}, err
	}
	batch, err := getEnvPositiveIntStrict("WORKER_MEILISEARCH_SWEEPER_BATCH_SIZE", 50)
	if err != nil {
		return WorkerMeilisearchSweeperConfig{}, err
	}
	concurrency, err := getEnvPositiveIntStrict("WORKER_MEILISEARCH_SWEEPER_CONCURRENCY", 4)
	if err != nil {
		return WorkerMeilisearchSweeperConfig{}, err
	}
	return WorkerMeilisearchSweeperConfig{
		Enabled:     getEnvBool("WORKER_MEILISEARCH_SWEEPER_ENABLED", true),
		Interval:    interval,
		BatchSize:   batch,
		Concurrency: concurrency,
	}, nil
}

func loadWorkerAccountStateConfig() (WorkerAccountStateConfig, error) {
	interval, err := getEnvPositiveDurationStrict("WORKER_ACCOUNT_STATE_INTERVAL", 1*time.Minute)
	if err != nil {
		return WorkerAccountStateConfig{}, err
	}
	batch, err := getEnvPositiveIntStrict("WORKER_ACCOUNT_STATE_BATCH_SIZE", 500)
	if err != nil {
		return WorkerAccountStateConfig{}, err
	}
	staleAfter, err := getEnvPositiveDurationStrict("WORKER_ACCOUNT_STATE_STALE_AFTER", 15*time.Minute)
	if err != nil {
		return WorkerAccountStateConfig{}, err
	}
	transitionMaxAge, err := getEnvPositiveDurationStrict("WORKER_ACCOUNT_STATE_TRANSITION_MAX_AGE", 30*24*time.Hour)
	if err != nil {
		return WorkerAccountStateConfig{}, err
	}
	return WorkerAccountStateConfig{
		Enabled:                   getEnvBool("WORKER_ACCOUNT_STATE_ENABLED", true),
		Interval:                  interval,
		BatchSize:                 batch,
		StaleAfter:                staleAfter,
		TransitionRetentionMaxAge: transitionMaxAge,
	}, nil
}

func loadWorkerMeilisearchConfig() MeilisearchConfig {
	cfg := MeilisearchConfig{
		Enabled:      getEnvBool("MEILI_ENABLED", false),
		URL:          strings.TrimSpace(getEnv("MEILI_URL", "")),
		MasterKey:    strings.TrimSpace(getEnv("MEILI_MASTER_KEY", "")),
		SearchAPIKey: strings.TrimSpace(getEnv("MEILI_SEARCH_API_KEY", "")),
	}
	if cfg.SearchAPIKey == "" {
		cfg.SearchAPIKey = cfg.MasterKey
	}
	return cfg
}
