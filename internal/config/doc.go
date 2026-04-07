package config

import (
	"cmp"
	"slices"
	"strings"
)

// EnvVarDoc describes one environment variable consumed by runtime config.
type EnvVarDoc struct {
	Name         string
	Runtimes     []string
	Required     bool
	DefaultValue string
	Description  string
}

// ConfigEnvDocs is the single source of truth for config documentation.
func ConfigEnvDocs() []EnvVarDoc {
	return []EnvVarDoc{
		// API runtime.
		{
			Name:         "ADMIN_BEARER_TOKEN",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional bearer token for admin HTTP endpoints.",
		},
		{
			Name:         "API_MAX_BATCH_SIZE",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "200",
			Description:  "Maximum IDs accepted by batch event/profile API requests.",
		},
		{
			Name:         "API_RELAY_FALLBACK_ENABLED",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enable best-effort relay fallback for local event/profile misses.",
		},
		{
			Name:         "API_RELAY_FALLBACK_MAX_FANOUT",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "3",
			Description:  "Maximum number of fallback relays queried per lookup.",
		},
		{
			Name:         "API_RELAY_FALLBACK_TIMEOUT",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "2s",
			Description:  "Per-relay timeout budget for fallback lookups.",
		},
		{
			Name:         "API_RELAY_FALLBACK_URLS",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "",
			Description:  "CSV fallback relay URLs. If empty, API falls back to INGESTOR_RELAY_URLS.",
		},
		// Shared runtime settings.
		{
			Name:         "DATABASE_URL",
			Runtimes:     []string{"api", "ingestor", "trust_worker", "worker"},
			Required:     true,
			DefaultValue: "",
			Description:  "PostgreSQL connection string.",
		},
		{
			Name:         "DEBUG_ADDR",
			Runtimes:     []string{"api", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional debug/pprof listen address. Leave empty by default; prefer localhost binding.",
		},
		{
			Name:         "ENVIRONMENT",
			Runtimes:     []string{"api", "ingestor", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: "development",
			Description:  "Deployment environment label.",
		},
		{
			Name:         "HTTP_ADDR",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: ":8080",
			Description:  "HTTP listen address for the API server.",
		},
		{
			Name:         "HTTP_BATCH_RATE_LIMIT_RPM",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "40",
			Description:  "Per-IP requests per minute for HTTP batch endpoints.",
		},
		{
			Name:         "HTTP_DM_COMPAT_RATE_LIMIT_RPM",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "30",
			Description:  "Per-connection requests per minute for DM compatibility calls on the Primal WebSocket gateway.",
		},
		{
			Name:         "HTTP_RATE_LIMIT_BURST",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "60",
			Description:  "Burst size for the default HTTP rate limiter.",
		},
		{
			Name:         "HTTP_RATE_LIMIT_RPM",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "240",
			Description:  "Per-IP requests per minute for default HTTP routes.",
		},
		{
			Name:         "HTTP_SEARCH_RATE_LIMIT_RPM",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "60",
			Description:  "Per-IP requests per minute for the search endpoint.",
		},
		// Ingestor runtime.
		{
			Name:         "INGESTOR_BACKFILL_CONNECT_TIMEOUT",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "10s",
			Description:  "Connection timeout used by backfill relay sessions.",
		},
		{
			Name:         "INGESTOR_BACKFILL_EMPTY_PAGE_MAX",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "2",
			Description:  "Maximum consecutive empty pages before backfill stops.",
		},
		{
			Name:         "INGESTOR_BACKFILL_ENABLED",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enables bootstrap/backfill mode.",
		},
		{
			Name:         "INGESTOR_BACKFILL_IDLE_TIMEOUT",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "3s",
			Description:  "Idle timeout while waiting for backfill events.",
		},
		{
			Name:         "INGESTOR_BACKFILL_MODE",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "backfill",
			Description:  "Backfill strategy selector (currently only backfill).",
		},
		{
			Name:         "INGESTOR_BACKFILL_PAGE_LIMIT",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "500",
			Description:  "Page size used by backfill fetches.",
		},
		{
			Name:         "INGESTOR_BACKFILL_SINCE",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional inclusive lower timestamp bound for backfill.",
		},
		{
			Name:         "INGESTOR_BACKFILL_UNTIL",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional inclusive upper timestamp bound for backfill.",
		},
		{
			Name:         "INGESTOR_FILTER_GROUP",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "default_v1",
			Description:  "Active relay filter group name.",
		},
		{
			Name:         "INGESTOR_FILTER_GROUPS_JSON",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional JSON map of relay filter groups; only `default_v1` is currently implemented for live ingest.",
		},
		{
			Name:         "INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "300",
			Description:  "Live mode lookback window in seconds before tailing.",
		},
		{
			Name:         "INGESTOR_LIVE_RESUME_OVERLAP_SECONDS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "60",
			Description:  "Live mode overlap window in seconds for resume safety.",
		},
		{
			Name:         "INGESTOR_MODE",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "live",
			Description:  "Ingestor runtime mode (live or replay).",
		},
		{
			Name:         "INGESTOR_RELAY_ALLOWLIST",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "Allowed relay URLs. Required when INGESTOR_RELAY_URLS is set.",
		},
		{
			Name:         "INGESTOR_RELAY_BACKOFF_INITIAL",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "1s",
			Description:  "Initial reconnect backoff for relay connections.",
		},
		{
			Name:         "INGESTOR_RELAY_BACKOFF_MAX",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "30s",
			Description:  "Maximum reconnect backoff for relay connections.",
		},
		{
			Name:         "INGESTOR_RELAY_CONNECT_TIMEOUT",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "10s",
			Description:  "Connection timeout for relay sessions.",
		},
		{
			Name:         "INGESTOR_RELAY_DISABLED",
			Runtimes:     []string{"api", "ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "CSV list of allowlisted relay URLs to disable.",
		},
		{
			Name:         "INGESTOR_RELAY_LAG_THRESHOLD",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "45s",
			Description:  "Lag threshold used for relay health reporting.",
		},
		{
			Name:         "INGESTOR_RELAY_REQUIRE_TLS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "true (non-local env), false (local env)",
			Description:  "Require wss:// relay URLs when enabled.",
		},
		{
			Name:         "INGESTOR_RELAY_URLS",
			Runtimes:     []string{"api", "ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "CSV list of relay URLs (required for INGESTOR_MODE=live).",
		},
		{
			Name:         "INGESTOR_REPLAY_FIXTURE_PATH",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "",
			Description:  "Replay fixture path (required for INGESTOR_MODE=replay).",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_COOLDOWN",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "30m",
			Description:  "Cooldown between successful trust-targeted fetches for the same pubkey.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_ENABLED",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enable bounded trust-targeted pubkey fetch scheduling.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "50",
			Description:  "Maximum frontier/suggestion promotions allowed per scheduler cycle.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "100",
			Description:  "Maximum trust frontier pubkeys selected for targeted fetch per cycle.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "5000",
			Description:  "Maximum trust-ranked pubkeys maintained in the ingest frontier.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_PAGE_LIMIT_PER_RELAY",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "200",
			Description:  "Per-relay page size for trust-targeted pubkey fetch requests.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "86400",
			Description:  "Recent history lookback window in seconds for targeted pubkey fetches.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_REFRESH_INTERVAL",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "2m",
			Description:  "Scheduler interval for trust frontier refresh and targeted fetch cycles.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_RETRY_DELAY",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "10m",
			Description:  "Delay before retrying a pubkey after targeted fetch errors.",
		},
		{
			Name:         "INGESTOR_TRUST_FETCH_STABLE_WINDOW",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "10m",
			Description:  "Minimum stability window before newly seen trust candidates become active.",
		},
		{
			Name:         "INGESTOR_TRUST_PRIORITIZATION_ENABLED",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable trust-driven relay ordering for ingestor startup ordering.",
		},
		{
			Name:         "INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS",
			Runtimes:     []string{"ingestor"},
			Required:     false,
			DefaultValue: "2000",
			Description:  "Maximum top trust pubkeys considered for relay ordering.",
		},
		// Shared observability/runtime-adjacent settings.
		{
			Name:         "METRICS_ADDR",
			Runtimes:     []string{"ingestor", "trust_worker", "worker"},
			Required:     false,
			DefaultValue: ":9090",
			Description:  "Prometheus metrics listen address for ingestor/worker. API exposes /metrics on HTTP_ADDR.",
		},
		{
			Name:         "PRIMAL_WS_ALLOWED_ORIGINS",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "",
			Description:  "CSV allowlist of browser origins for Primal WS.",
		},
		{
			Name:         "PRIMAL_WS_ALLOW_ANY_ORIGIN",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Allow all WS origins, bypassing PRIMAL_WS_ALLOWED_ORIGINS validation.",
		},
		{
			Name:         "PRIMAL_WS_MAX_MESSAGE_BYTES",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "1048576",
			Description:  "Maximum inbound WebSocket message size in bytes.",
		},
		{
			Name:         "PRIMAL_WS_MAX_REQ_PER_MINUTE",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "240",
			Description:  "Per-connection request rate limit for Primal WebSocket REQ calls.",
		},
		{
			Name:         "PRIMAL_WS_MAX_SUBSCRIPTIONS",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "200",
			Description:  "Maximum concurrent Primal WS subscriptions per connection.",
		},
		{
			Name:         "PRIMAL_WS_REQUEST_TIMEOUT",
			Runtimes:     []string{"api"},
			Required:     false,
			DefaultValue: "10s",
			Description:  "Timeout for individual Primal WS request handling.",
		},
		// Worker runtime.
		{
			Name:         "WORKER_CONCURRENCY",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "4",
			Description:  "Worker goroutine concurrency.",
		},
		{
			Name:         "WORKER_JOB_RUNNING_TIMEOUT",
			Runtimes:     []string{"trust_worker", "worker"},
			Required:     false,
			DefaultValue: "15m0s",
			Description:  "Lease timeout for running jobs before stale recovery treats them as orphaned.",
		},
		{
			Name:         "WORKER_JOB_STALE_RECOVERY_BATCH_LIMIT",
			Runtimes:     []string{"trust_worker", "worker"},
			Required:     false,
			DefaultValue: "100",
			Description:  "Maximum stale running jobs processed per recovery interval.",
		},
		{
			Name:         "WORKER_JOB_STALE_RECOVERY_INTERVAL",
			Runtimes:     []string{"trust_worker", "worker"},
			Required:     false,
			DefaultValue: "30s",
			Description:  "Interval between stale running-job recovery scans.",
		},
		{
			Name:         "WORKER_JOB_RETENTION_DEAD_MAX_AGE",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "4320h0m0s",
			Description:  "Max age for dead jobs before retention purges terminal history.",
		},
		{
			Name:         "WORKER_JOB_RETENTION_DELETE_BATCH_LIMIT",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "500",
			Description:  "Maximum terminal jobs deleted per retention purge run.",
		},
		{
			Name:         "WORKER_JOB_RETENTION_ENABLED",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable periodic retention purge of terminal job history.",
		},
		{
			Name:         "WORKER_JOB_RETENTION_RUN_INTERVAL",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "1h0m0s",
			Description:  "Interval between terminal job retention purge runs.",
		},
		{
			Name:         "WORKER_JOB_RETENTION_SUCCEEDED_MAX_AGE",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "720h0m0s",
			Description:  "Max age for succeeded jobs before retention purges terminal history.",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_PAYLOAD_TRIM_BATCH_LIMIT",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "500",
			Description:  "Maximum invalid_events rows with raw_payload trimmed to NULL per retention run when payload trimming is enabled.",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable optional second-stage invalid_events payload trimming (raw_payload set to NULL before full-row retention purge).",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "168h0m0s",
			Description:  "Max age for invalid_events rows before payload-only trimming (must be smaller than WORKER_INVALID_EVENTS_RETENTION_MAX_AGE).",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "500",
			Description:  "Maximum invalid_events rows deleted per retention purge run.",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_RETENTION_ENABLED",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable periodic invalid_events retention purge.",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_RETENTION_MAX_AGE",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "720h0m0s",
			Description:  "Max age for invalid_events rows before retention purge.",
		},
		{
			Name:         "WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "1h0m0s",
			Description:  "Interval between invalid_events retention purge runs.",
		},
		// Trust worker runtime.
		{
			Name:         "TRUST_ENABLE_REDIS_SYNC",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "false",
			Description:  "Enable Redis graph synchronization trust job phases.",
		},
		{
			Name:         "TRUST_ENABLE_SCORE_COMPUTE",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "true",
			Description:  "Enable trust score computation trust job phases.",
		},
		{
			Name:         "TRUST_REDIS_URL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "",
			Description:  "Redis connection string used for trust graph working state; required when TRUST_ENABLE_REDIS_SYNC=true.",
		},
		{
			Name:         "TRUST_REDIS_KEY_PREFIX",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "nostrmash",
			Description:  "Prefix namespace for trust-worker Redis graph/snapshot keys.",
		},
		{
			Name:         "TRUST_WORKER_CLAIM_BATCH_SIZE",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "5",
			Description:  "Maximum trust jobs claimed per poll loop.",
		},
		{
			Name:         "TRUST_WORKER_CONCURRENCY",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "2",
			Description:  "Trust worker goroutine concurrency.",
		},
		{
			Name:         "TRUST_WORKER_POLL_INTERVAL",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "1s",
			Description:  "Polling interval for trust queue claims.",
		},
		{
			Name:         "TRUST_WORKER_RETRY_DELAY",
			Runtimes:     []string{"trust_worker"},
			Required:     false,
			DefaultValue: "5s",
			Description:  "Retry delay when trust jobs fail.",
		},
	}
}

// GenerateConfigurationMarkdown renders docs/configuration.md.
func GenerateConfigurationMarkdown() string {
	docs := append([]EnvVarDoc(nil), ConfigEnvDocs()...)
	for i := range docs {
		runtimes := append([]string(nil), docs[i].Runtimes...)
		slices.Sort(runtimes)
		docs[i].Runtimes = runtimes
	}
	slices.SortFunc(docs, func(a, b EnvVarDoc) int {
		return cmp.Compare(a.Name, b.Name)
	})

	var b strings.Builder
	b.WriteString("# Configuration\n\n")
	b.WriteString("Use this page as the environment-variable reference for NostrMash runtimes.\n")
	b.WriteString("It is generated by `go run ./cmd/configdoc` from `internal/config` metadata.\n")
	b.WriteString("Read [docs/operations.md](operations.md) for operator workflow and tuning context; use this page as the lookup layer for exact variable names, defaults, and runtime ownership.\n")
	b.WriteString("Operational values like `APP_VERSION` and test-only knobs like `TEST_DATABASE_URL` are intentionally out of scope.\n")
	b.WriteString("Do not hand-edit this file.\n\n")
	b.WriteString("| Env var | Runtime(s) | Required | Default | Description |\n")
	b.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, doc := range docs {
		required := "optional"
		if doc.Required {
			required = "required"
		}
		defaultValue := doc.DefaultValue
		if strings.TrimSpace(defaultValue) == "" {
			defaultValue = "-"
		}
		b.WriteString("| `")
		b.WriteString(doc.Name)
		b.WriteString("` | ")
		b.WriteString("`")
		b.WriteString(strings.Join(doc.Runtimes, ", "))
		b.WriteString("` | ")
		b.WriteString(required)
		b.WriteString(" | `")
		b.WriteString(defaultValue)
		b.WriteString("` | ")
		b.WriteString(doc.Description)
		b.WriteString(" |\n")
	}
	return b.String()
}
