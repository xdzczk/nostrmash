package config

import (
	"fmt"
	"net/url"
	"time"
)

type WorkerConfig struct {
	Shared                   SharedConfig
	Concurrency              int
	LiveConcurrency          int
	BackfillConcurrency      int
	ClaimBatchSize           int
	JobRecovery              WorkerJobRecoveryConfig
	JobRetention             WorkerJobRetentionConfig
	InvalidEventRetention    WorkerInvalidEventRetentionConfig
	EngagementRetention      WorkerEngagementRetentionConfig
	ReplaceableRetention     WorkerReplaceableRetentionConfig
	DeletionRetention        WorkerDeletionRetentionConfig
	UntrustedAuthorRetention WorkerUntrustedAuthorRetentionConfig
	AuthorRecentRetention    WorkerAuthorRecentRetentionConfig
	SearchDocsRetention      WorkerSearchDocsRetentionConfig
	EventRelaysRetention     WorkerEventRelaysRetentionConfig
	TrustRetentionHooks      TrustRetentionHooksConfig
	TrustRetentionLoop       WorkerTrustRetentionLoopConfig
	AuthorAnalyticsSweeper   WorkerAuthorAnalyticsSweeperConfig
	ProfileStatsSweeper      WorkerProfileStatsSweeperConfig
	MeilisearchSweeper       WorkerMeilisearchSweeperConfig
	AccountState             WorkerAccountStateConfig
	Hydration                HydrationConfig
	Meilisearch              MeilisearchConfig
	RelayRegistry            RelayRegistryConfig
}

// WorkerAccountStateConfig configures the derived account-state recompute loop.
// The loop periodically reads a batch of accounts whose state is stale, derives
// a fresh state from cheap signals (trust hops, observation count, profile
// presence, note count), records any transition, and refreshes the per-state
// count metrics. It also prunes the append-only transition audit table.
type WorkerAccountStateConfig struct {
	Enabled                   bool
	Interval                  time.Duration
	BatchSize                 int
	StaleAfter                time.Duration
	TransitionRetentionMaxAge time.Duration
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
// replaceable events (kinds 0/3/10000/10002/10003 and parameterized 30023)
// that have been strictly superseded by a newer winner. The latest-version
// projections (contact_lists_latest, relay_lists_latest, profiles_latest,
// replaceable_state) all reference the winner, so only superseded versions are
// removed and the read models survive.
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

// WorkerUntrustedAuthorRetentionConfig configures the purger that deletes raw
// author-gated events (kinds 1/4/9802/10000/10003/30023) whose author is
// absent from trust_graph_snapshot once they are older than MaxAge (enforced
// on both created_at and first_seen_at). This is the months-scale complement
// to the ingest trust gate: the gate bounds inflow, this reclaims untrusted
// residue accepted while the gate was open (shadow mode, pre-gate history).
//
// The default MaxAge is deliberately short (14 d): with the default
// INGESTOR_TRUST_GATE_MODE=open a firehose deployment ingests everything, so
// the untrusted horizon is what actually bounds steady-state disk usage.
//
// Fail-safe: the store-side purge deletes nothing while trust_graph_snapshot
// is empty, so an unbootstrapped trust pipeline never causes mass deletion.
//
// DeadGrace mirrors the other retention loops (derivation-safety window).
type WorkerUntrustedAuthorRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	DeadGrace        time.Duration
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// WorkerAuthorRecentRetentionConfig configures the pruner that bounds the
// author_recent_events projection: rows older than MaxAge are removed, and
// each author keeps at most PerAuthorCap newest rows. The projection is
// rebuildable from canonical events, so this retention is purely a disk/read
// bound; PerAuthorCap must stay above the API's per-request max limit so
// bounded reads are unaffected.
//
// AuthorBatchLimit caps how many over-cap authors a single cap pass trims,
// keeping the per-run GROUP BY work bounded.
type WorkerAuthorRecentRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
	PerAuthorCap     int
	AuthorBatchLimit int
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// WorkerSearchDocsRetentionConfig configures the search_documents groomer:
// note bodies older than BodyMaxAge are trimmed to BodyMaxChars (the
// generated search_tsv shrinks with the body), and rows whose source event no
// longer exists are pruned. Both operations are rebuildable state changes;
// stale trimmed rows regain their full body if the source is re-indexed.
type WorkerSearchDocsRetentionConfig struct {
	Enabled      bool
	BodyMaxAge   time.Duration
	BodyMaxChars int
	RunInterval  time.Duration
	BatchLimit   int
}

// WorkerTrustRetentionLoopConfig owns the loop cadence for the durable
// trust-retention hooks (stale trusted discovery candidates, idle low-value
// account_states rows). The per-scope enable flags and horizons live in
// TrustRetentionHooksConfig (TRUST_RETENTION_* envs); this struct only adds
// the worker-side execution knobs.
type WorkerTrustRetentionLoopConfig struct {
	RunInterval      time.Duration
	DeleteBatchLimit int
}

// WorkerEventRelaysRetentionConfig configures the provenance pruner that
// deletes event_relays rows seen before MaxAge, always retaining the
// earliest-seen row per event so first-provenance survives. Windowed relay
// analytics read far inside the horizon and are unaffected.
type WorkerEventRelaysRetentionConfig struct {
	Enabled          bool
	MaxAge           time.Duration
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
	concurrency, err := loadWorkerConcurrency()
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
	retention, err := loadWorkerRetentionConfigs()
	if err != nil {
		return WorkerConfig{}, err
	}
	trustRetentionHooks, err := loadTrustRetentionHooksConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	sweepers, err := loadWorkerSweeperConfigs()
	if err != nil {
		return WorkerConfig{}, err
	}
	accountState, err := loadWorkerAccountStateConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	hydrationCfg, err := loadHydrationConfig()
	if err != nil {
		return WorkerConfig{}, err
	}
	cfg := WorkerConfig{
		Shared:                   shared,
		Concurrency:              concurrency.Concurrency,
		LiveConcurrency:          concurrency.LiveConcurrency,
		BackfillConcurrency:      concurrency.BackfillConcurrency,
		ClaimBatchSize:           concurrency.ClaimBatchSize,
		JobRecovery:              jobRecovery,
		JobRetention:             jobRetention,
		InvalidEventRetention:    retention.InvalidEvent,
		EngagementRetention:      retention.Engagement,
		ReplaceableRetention:     retention.Replaceable,
		DeletionRetention:        retention.Deletion,
		UntrustedAuthorRetention: retention.UntrustedAuthor,
		AuthorRecentRetention:    retention.AuthorRecent,
		SearchDocsRetention:      retention.SearchDocs,
		EventRelaysRetention:     retention.EventRelays,
		TrustRetentionHooks:      trustRetentionHooks,
		TrustRetentionLoop:       retention.TrustRetention,
		AuthorAnalyticsSweeper:   sweepers.AuthorAnalytics,
		ProfileStatsSweeper:      sweepers.ProfileStats,
		MeilisearchSweeper:       sweepers.Meilisearch,
		AccountState:             accountState,
		Hydration:                hydrationCfg,
		Meilisearch:              loadWorkerMeilisearchConfig(),
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
	if cfg.UntrustedAuthorRetention.Enabled {
		if cfg.UntrustedAuthorRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_UNTRUSTED_AUTHOR_MAX_AGE must be > 0")
		}
		if cfg.UntrustedAuthorRetention.DeadGrace <= 0 {
			return fmt.Errorf("WORKER_RETENTION_UNTRUSTED_AUTHOR_DEAD_GRACE must be > 0")
		}
		if cfg.UntrustedAuthorRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_UNTRUSTED_AUTHOR_RUN_INTERVAL must be > 0")
		}
		if cfg.UntrustedAuthorRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_UNTRUSTED_AUTHOR_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.AuthorRecentRetention.Enabled {
		if cfg.AuthorRecentRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_MAX_AGE must be > 0")
		}
		if cfg.AuthorRecentRetention.PerAuthorCap <= 0 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_PER_AUTHOR_CAP must be > 0")
		}
		if cfg.AuthorRecentRetention.PerAuthorCap < 100 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_PER_AUTHOR_CAP must be >= 100 (the API serves up to 100 recent events per request)")
		}
		if cfg.AuthorRecentRetention.AuthorBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_AUTHOR_BATCH_LIMIT must be > 0")
		}
		if cfg.AuthorRecentRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_RUN_INTERVAL must be > 0")
		}
		if cfg.AuthorRecentRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_AUTHOR_RECENT_DELETE_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.SearchDocsRetention.Enabled {
		if cfg.SearchDocsRetention.BodyMaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_SEARCH_DOCS_BODY_MAX_AGE must be > 0")
		}
		if cfg.SearchDocsRetention.BodyMaxChars <= 0 {
			return fmt.Errorf("WORKER_RETENTION_SEARCH_DOCS_BODY_MAX_CHARS must be > 0")
		}
		if cfg.SearchDocsRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_SEARCH_DOCS_RUN_INTERVAL must be > 0")
		}
		if cfg.SearchDocsRetention.BatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_SEARCH_DOCS_BATCH_LIMIT must be > 0")
		}
	}
	if cfg.EventRelaysRetention.Enabled {
		if cfg.EventRelaysRetention.MaxAge <= 0 {
			return fmt.Errorf("WORKER_RETENTION_EVENT_RELAYS_MAX_AGE must be > 0")
		}
		if cfg.EventRelaysRetention.RunInterval <= 0 {
			return fmt.Errorf("WORKER_RETENTION_EVENT_RELAYS_RUN_INTERVAL must be > 0")
		}
		if cfg.EventRelaysRetention.DeleteBatchLimit <= 0 {
			return fmt.Errorf("WORKER_RETENTION_EVENT_RELAYS_DELETE_BATCH_LIMIT must be > 0")
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
