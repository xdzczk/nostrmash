package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestValidateRelayConfig_RejectsNonTLSInTLSMode(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"ws://localhost:8080"}
	cfg.Allowlist = []string{"ws://localhost:8080"}
	cfg.RequireTLS = true
	if err := validateRelayConfig(cfg); err == nil {
		t.Fatal("expected error for ws relay when TLS is required")
	}
}

func TestValidateRelayConfig_EnforcesAllowlist(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"wss://relay.example.com"}
	cfg.Allowlist = []string{"wss://different.example.com"}
	if err := validateRelayConfig(cfg); err == nil {
		t.Fatal("expected allowlist enforcement error")
	}
}

func TestValidateBackfillConfig_RequiresValidRange(t *testing.T) {
	since := int64(20)
	until := int64(10)
	err := validateBackfillConfig(BackfillConfig{
		Enabled:        true,
		Mode:           "backfill",
		Since:          &since,
		Until:          &until,
		PageLimit:      100,
		IdleTimeout:    2 * time.Second,
		EmptyPageMax:   1,
		ConnectTimeout: 2 * time.Second,
	})
	if err == nil {
		t.Fatal("expected invalid range validation error")
	}
}

func TestLoad_NonIngestorRejected(t *testing.T) {
	if _, err := Load("api"); err == nil {
		t.Fatal("expected non-ingestor Load to be rejected")
	}
}

func TestLoad_IngestorCompatibilityWrapper(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	cfg, err := Load("ingestor")
	if err != nil {
		t.Fatalf("expected Load ingestor compatibility to work: %v", err)
	}
	if cfg.DatabaseURL != "postgres://example" || cfg.Mode != "live" {
		t.Fatalf("unexpected legacy wrapper values: %#v", cfg)
	}
}

func TestLoadAPI_DefaultsAndEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("METRICS_ADDR", "127.0.0.1:19090")
	t.Setenv("DEBUG_ADDR", "")
	t.Setenv("API_MAX_BATCH_SIZE", "")
	t.Setenv("HTTP_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_RATE_LIMIT_BURST", "")
	t.Setenv("HTTP_SEARCH_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_BATCH_RATE_LIMIT_RPM", "")
	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("load api defaults: %v", err)
	}
	if cfg.HTTP.MaxBatchSize != 200 || cfg.HTTP.RateLimitRPM != 240 || cfg.HTTP.RateLimitBurst != 60 {
		t.Fatalf("unexpected api defaults: %#v", cfg.HTTP)
	}
	if cfg.PrimalWS.MaxSubscriptions != 200 || cfg.PrimalWS.MaxReqPerMinute != 240 {
		t.Fatalf("unexpected ws defaults: %#v", cfg.PrimalWS)
	}
	if cfg.Shared.Observability.DebugAddr != "" {
		t.Fatalf("expected empty debug addr by default, got %q", cfg.Shared.Observability.DebugAddr)
	}
	if cfg.Shared.Observability.MetricsAddr != "" {
		t.Fatalf("expected API metrics addr to be ignored, got %q", cfg.Shared.Observability.MetricsAddr)
	}

	t.Setenv("API_MAX_BATCH_SIZE", "75")
	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "50")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "2s")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "2048")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "33")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "11")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "https://app.primal.net,https://nostrmash.local")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "true")
	t.Setenv("DEBUG_ADDR", "127.0.0.1:6060")

	cfg, err = LoadAPI()
	if err != nil {
		t.Fatalf("load api env: %v", err)
	}
	if cfg.HTTP.MaxBatchSize != 75 {
		t.Fatalf("unexpected max batch size: %d", cfg.HTTP.MaxBatchSize)
	}
	if cfg.PrimalWS.MaxSubscriptions != 50 || cfg.PrimalWS.MaxMessageBytes != 2048 || cfg.PrimalWS.MaxReqPerMinute != 33 {
		t.Fatalf("unexpected ws env config: %#v", cfg.PrimalWS)
	}
	if cfg.Shared.Observability.DebugAddr != "127.0.0.1:6060" {
		t.Fatalf("unexpected debug addr: %q", cfg.Shared.Observability.DebugAddr)
	}
}

func TestLoadAPI_InvalidEnvIsActionable(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_MAX_BATCH_SIZE", "nope")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "API_MAX_BATCH_SIZE") {
		t.Fatalf("expected actionable API_MAX_BATCH_SIZE error, got %v", err)
	}
}

func TestLoadAPI_InvalidAllowedOriginFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "ws://bad-origin")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "PRIMAL_WS_ALLOWED_ORIGINS") {
		t.Fatalf("expected PRIMAL_WS_ALLOWED_ORIGINS validation error, got %v", err)
	}
}

func TestLoadAPI_InvalidDurationFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "bad")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "PRIMAL_WS_REQUEST_TIMEOUT") {
		t.Fatalf("expected PRIMAL_WS_REQUEST_TIMEOUT validation error, got %v", err)
	}
}

func TestLoadAPI_FallbackRelayURLsNormalizeAndDeduplicate(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_RELAY_FALLBACK_ENABLED", "true")
	t.Setenv("API_RELAY_FALLBACK_URLS", "WSS://Relay.Example.com/,wss://relay.example.com")

	cfg, err := LoadAPI()
	if err != nil {
		t.Fatalf("load api fallback relay urls: %v", err)
	}
	if len(cfg.RelayFallback.URLs) != 1 || cfg.RelayFallback.URLs[0] != "wss://relay.example.com" {
		t.Fatalf("unexpected normalized fallback urls: %#v", cfg.RelayFallback.URLs)
	}
}

func TestLoadAPI_FallbackRelayInvalidURLFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("API_RELAY_FALLBACK_ENABLED", "true")
	t.Setenv("API_RELAY_FALLBACK_URLS", "https://relay.example.com")

	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "invalid fallback relay URL") {
		t.Fatalf("expected invalid fallback relay URL validation error, got %v", err)
	}
}

func TestLoadWorker_DefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_RUN_INTERVAL", "")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_DELETE_BATCH_LIMIT", "")
	t.Setenv("DEBUG_ADDR", "")
	cfg, err := LoadWorker()
	if err != nil {
		t.Fatalf("load worker defaults: %v", err)
	}
	if cfg.Concurrency != 4 {
		t.Fatalf("unexpected worker default concurrency: %d", cfg.Concurrency)
	}
	if !cfg.JobRetention.Enabled {
		t.Fatalf("expected job retention enabled by default")
	}
	if cfg.JobRetention.DeleteBatchLimit != 500 {
		t.Fatalf("unexpected default retention batch limit: %d", cfg.JobRetention.DeleteBatchLimit)
	}
	if !cfg.InvalidEventRetention.Enabled {
		t.Fatalf("expected invalid event retention enabled by default")
	}
	if cfg.InvalidEventRetention.MaxAge != 30*24*time.Hour {
		t.Fatalf("unexpected invalid event retention max age: %s", cfg.InvalidEventRetention.MaxAge)
	}
	if !cfg.InvalidEventRetention.PayloadTrim.Enabled {
		t.Fatalf("expected invalid event payload trim enabled by default")
	}
	if cfg.InvalidEventRetention.PayloadTrim.MaxAge != 7*24*time.Hour {
		t.Fatalf("unexpected invalid event payload trim max age: %s", cfg.InvalidEventRetention.PayloadTrim.MaxAge)
	}

	t.Setenv("WORKER_CONCURRENCY", "8")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker env: %v", err)
	}
	if cfg.Concurrency != 8 {
		t.Fatalf("unexpected worker env concurrency: %d", cfg.Concurrency)
	}
	t.Setenv("WORKER_JOB_RETENTION_ENABLED", "false")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker retention disabled: %v", err)
	}
	if cfg.JobRetention.Enabled {
		t.Fatalf("expected retention to be disabled by env")
	}
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "false")
	cfg, err = LoadWorker()
	if err != nil {
		t.Fatalf("load worker invalid retention disabled: %v", err)
	}
	if cfg.InvalidEventRetention.Enabled {
		t.Fatalf("expected invalid event retention to be disabled by env")
	}

	t.Setenv("WORKER_CONCURRENCY", "0")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_CONCURRENCY") {
		t.Fatalf("expected actionable WORKER_CONCURRENCY error, got %v", err)
	}
}

func TestLoadWorker_InvalidInvalidEventsRetentionConfigFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "0s")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_INVALID_EVENTS_RETENTION_MAX_AGE") {
		t.Fatalf("expected actionable WORKER_INVALID_EVENTS_RETENTION_MAX_AGE error, got %v", err)
	}
}

func TestLoadWorker_InvalidInvalidEventsPayloadTrimConfigFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_ENABLED", "true")
	t.Setenv("WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE", "720h")
	t.Setenv("WORKER_INVALID_EVENTS_RETENTION_MAX_AGE", "720h")
	if _, err := LoadWorker(); err == nil || !strings.Contains(err.Error(), "WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE") {
		t.Fatalf("expected actionable WORKER_INVALID_EVENTS_PAYLOAD_TRIM_MAX_AGE error, got %v", err)
	}
}

func TestLoadTrustWorker_DefaultsAndValidation(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("TRUST_REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("TRUST_WORKER_CONCURRENCY", "")
	t.Setenv("TRUST_WORKER_CLAIM_BATCH_SIZE", "")
	t.Setenv("TRUST_WORKER_POLL_INTERVAL", "")
	t.Setenv("TRUST_WORKER_RETRY_DELAY", "")
	t.Setenv("TRUST_ENABLE_REDIS_SYNC", "")
	t.Setenv("TRUST_ENABLE_SCORE_COMPUTE", "")

	cfg, err := LoadTrustWorker()
	if err != nil {
		t.Fatalf("load trust worker defaults: %v", err)
	}
	if cfg.Concurrency != 2 || cfg.ClaimBatchSize != 5 {
		t.Fatalf("unexpected trust worker defaults: %#v", cfg)
	}
	if cfg.Redis.URL != "redis://localhost:6379/0" {
		t.Fatalf("unexpected redis url: %q", cfg.Redis.URL)
	}
	if cfg.Redis.KeyPrefix != "nostrmash" {
		t.Fatalf("unexpected redis key prefix: %q", cfg.Redis.KeyPrefix)
	}

	t.Setenv("TRUST_WORKER_CONCURRENCY", "0")
	if _, err := LoadTrustWorker(); err == nil || !strings.Contains(err.Error(), "TRUST_WORKER_CONCURRENCY") {
		t.Fatalf("expected actionable TRUST_WORKER_CONCURRENCY error, got %v", err)
	}
}

func TestLoadAPI_InvalidDebugAddrFails(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("DEBUG_ADDR", "not-a-valid-addr")
	_, err := LoadAPI()
	if err == nil || !strings.Contains(err.Error(), "DEBUG_ADDR") {
		t.Fatalf("expected actionable DEBUG_ADDR validation error, got %v", err)
	}
}

func TestValidateIngestorMode_ReplayRequiresFixturePath(t *testing.T) {
	if err := validateIngestorMode("ingestor", "replay", ReplayConfig{}, testRelayConfig()); err == nil {
		t.Fatal("expected replay mode to require fixture path")
	}
}

func TestLoad_LiveResumeDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS", "")
	t.Setenv("INGESTOR_LIVE_RESUME_OVERLAP_SECONDS", "")

	cfg, err := LoadIngestor()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Shared.Database.URL != "postgres://example" {
		t.Fatalf("expected shared database url, got %q", cfg.Shared.Database.URL)
	}
	if cfg.Runtime.Mode != "live" {
		t.Fatalf("expected runtime mode live, got %q", cfg.Runtime.Mode)
	}
	if cfg.Relay.LiveBootstrapLookbackSeconds != 300 || cfg.Relay.LiveResumeOverlapSeconds != 60 {
		t.Fatalf("unexpected live resume defaults: %#v", cfg.Relay)
	}
	if !cfg.TrustPrioritization.Enabled || cfg.TrustPrioritization.TopPubkeys != 2000 {
		t.Fatalf("unexpected ingest trust prioritization defaults: %#v", cfg.TrustPrioritization)
	}
	if cfg.TrustFetch.Enabled {
		t.Fatalf("expected trust fetch disabled by default, got %#v", cfg.TrustFetch)
	}
}

func TestLoad_RejectsInvalidLiveResumeConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS", "abc")
	if _, err := LoadIngestor(); err == nil {
		t.Fatal("expected invalid bootstrap lookback to fail")
	}
}

func TestLoadIngestor_TrustPrioritizationOverrides(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_TRUST_PRIORITIZATION_ENABLED", "true")
	t.Setenv("INGESTOR_TRUST_PRIORITIZATION_TOP_PUBKEYS", "123")
	t.Setenv("INGESTOR_TRUST_FETCH_ENABLED", "true")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_TRACKED_PUBKEYS", "1111")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_SELECTED_PER_CYCLE", "17")
	t.Setenv("INGESTOR_TRUST_FETCH_REFRESH_INTERVAL", "45s")
	t.Setenv("INGESTOR_TRUST_FETCH_COOLDOWN", "20m")
	t.Setenv("INGESTOR_TRUST_FETCH_STABLE_WINDOW", "8m")
	t.Setenv("INGESTOR_TRUST_FETCH_MAX_PROMOTIONS_PER_CYCLE", "12")
	t.Setenv("INGESTOR_TRUST_FETCH_RECENT_LOOKBACK_SECONDS", "7200")
	t.Setenv("INGESTOR_TRUST_FETCH_PAGE_LIMIT_PER_RELAY", "77")
	t.Setenv("INGESTOR_TRUST_FETCH_RETRY_DELAY", "3m")

	cfg, err := LoadIngestor()
	if err != nil {
		t.Fatalf("load ingestor trust prioritization overrides: %v", err)
	}
	if !cfg.TrustPrioritization.Enabled || cfg.TrustPrioritization.TopPubkeys != 123 {
		t.Fatalf("unexpected trust prioritization config: %#v", cfg.TrustPrioritization)
	}
	if !cfg.TrustFetch.Enabled ||
		cfg.TrustFetch.MaxTrackedPubkeys != 1111 ||
		cfg.TrustFetch.MaxSelectedPerCycle != 17 ||
		cfg.TrustFetch.RefreshInterval != 45*time.Second ||
		cfg.TrustFetch.FetchCooldown != 20*time.Minute ||
		cfg.TrustFetch.StableWindow != 8*time.Minute ||
		cfg.TrustFetch.MaxPromotionsPerCycle != 12 ||
		cfg.TrustFetch.RecentLookbackSeconds != 7200 ||
		cfg.TrustFetch.PageLimitPerRelay != 77 ||
		cfg.TrustFetch.RetryDelay != 3*time.Minute {
		t.Fatalf("unexpected trust fetch config: %#v", cfg.TrustFetch)
	}
}

func TestConfigEnvDocs_BasicCoverageAndUniqueness(t *testing.T) {
	docs := ConfigEnvDocs()
	if len(docs) == 0 {
		t.Fatal("expected config env docs to be non-empty")
	}

	seen := make(map[string]struct{}, len(docs))
	for _, doc := range docs {
		if strings.TrimSpace(doc.Name) == "" {
			t.Fatal("env doc name must not be empty")
		}
		if _, ok := seen[doc.Name]; ok {
			t.Fatalf("duplicate env doc name: %s", doc.Name)
		}
		seen[doc.Name] = struct{}{}
		if len(doc.Runtimes) == 0 {
			t.Fatalf("env doc %s must list at least one runtime", doc.Name)
		}
		if strings.TrimSpace(doc.Description) == "" {
			t.Fatalf("env doc %s must include a description", doc.Name)
		}
	}

	for _, want := range []string{
		"DATABASE_URL",
		"INGESTOR_MODE",
		"HTTP_ADDR",
		"WORKER_CONCURRENCY",
	} {
		if _, ok := seen[want]; !ok {
			t.Fatalf("expected env doc entry for %s", want)
		}
	}
}

func TestGenerateConfigurationMarkdown_ContainsExpectedTableRows(t *testing.T) {
	content := GenerateConfigurationMarkdown()
	if !strings.Contains(content, "# Configuration") {
		t.Fatal("expected configuration heading")
	}
	if !strings.Contains(content, "| Env var | Runtime(s) | Required | Default | Description |") {
		t.Fatal("expected markdown table header")
	}
	if !strings.Contains(content, "`DATABASE_URL`") {
		t.Fatal("expected DATABASE_URL row")
	}
	if !strings.Contains(content, "`WORKER_CONCURRENCY`") {
		t.Fatal("expected WORKER_CONCURRENCY row")
	}
}

func TestGenerateConfigurationMarkdown_MatchesCommittedDocumentation(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	docPath := filepath.Join(root, "docs", "configuration.md")

	committed, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}

	generated := []byte(GenerateConfigurationMarkdown())
	if string(committed) != string(generated) {
		t.Fatalf("configuration docs are out of date; run: go run ./cmd/configdoc -out %s", docPath)
	}
}

func testRelayConfig() RelayConfig {
	return RelayConfig{
		FilterGroups: map[string]FilterGroup{
			"default_v1": {Kinds: []int{0, 1, 3, 5, 6, 7, 10002}},
		},
		ActiveFilterGroup:            "default_v1",
		LiveBootstrapLookbackSeconds: 300,
		LiveResumeOverlapSeconds:     60,
	}
}
