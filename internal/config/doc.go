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
			Name:         "DATABASE_URL",
			Runtimes:     []string{"api", "ingestor", "worker"},
			Required:     true,
			DefaultValue: "",
			Description:  "PostgreSQL connection string.",
		},
		{
			Name:         "DEBUG_ADDR",
			Runtimes:     []string{"api", "worker"},
			Required:     false,
			DefaultValue: "",
			Description:  "Optional debug/pprof listen address. Leave empty by default; prefer localhost binding.",
		},
		{
			Name:         "ENVIRONMENT",
			Runtimes:     []string{"api", "ingestor", "worker"},
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
			Description:  "Per-IP requests per minute for DM compatibility routes.",
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
			Description:  "Optional JSON map of additional relay filter groups.",
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
			Name:         "METRICS_ADDR",
			Runtimes:     []string{"ingestor", "worker"},
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
			Description:  "Per-IP request rate limit for Primal WebSocket calls.",
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
		{
			Name:         "WORKER_CONCURRENCY",
			Runtimes:     []string{"worker"},
			Required:     false,
			DefaultValue: "4",
			Description:  "Worker goroutine concurrency.",
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
	b.WriteString("This file is generated by `go run ./cmd/configdoc` from `internal/config` metadata.\n")
	b.WriteString("It documents environment variables parsed by runtime config loaders in this package.\n")
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
