package config

import "testing"

func TestValidateRelayConfig_RejectsNonTLSInTLSMode(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"ws://localhost:8080"}
	cfg.Allowlist = []string{"ws://localhost:8080"}
	cfg.RequireTLS = true
	err := validateRelayConfig(cfg)
	if err == nil {
		t.Fatal("expected error for ws relay when TLS is required")
	}
}

func TestValidateRelayConfig_EnforcesAllowlist(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"wss://relay.example.com"}
	cfg.Allowlist = []string{"wss://different.example.com"}
	err := validateRelayConfig(cfg)
	if err == nil {
		t.Fatal("expected allowlist enforcement error")
	}
}

func TestValidateRelayConfig_AllowsWSInDevMode(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"ws://localhost:8080"}
	cfg.Allowlist = []string{"ws://localhost:8080"}
	cfg.RequireTLS = false
	err := validateRelayConfig(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRelayConfig_DefaultFilterGroupKindsArePinned(t *testing.T) {
	cfg := testRelayConfig()
	cfg.FilterGroups["default_v1"] = FilterGroup{Kinds: []int{1, 2}}
	err := validateRelayConfig(cfg)
	if err == nil {
		t.Fatal("expected filter kinds validation error")
	}
}

func TestValidateRelayConfig_ActiveFilterGroupMustExist(t *testing.T) {
	cfg := testRelayConfig()
	cfg.ActiveFilterGroup = "custom"
	err := validateRelayConfig(cfg)
	if err == nil {
		t.Fatal("expected active filter group missing error")
	}
}

func TestValidateBackfillConfig_DisallowsReplayMode(t *testing.T) {
	err := validateBackfillConfig(BackfillConfig{
		Enabled:        true,
		Mode:           "replay",
		PageLimit:      100,
		IdleTimeout:    2,
		EmptyPageMax:   1,
		ConnectTimeout: 2,
	})
	if err == nil {
		t.Fatal("expected replay mode validation error")
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
		IdleTimeout:    2,
		EmptyPageMax:   1,
		ConnectTimeout: 2,
	})
	if err == nil {
		t.Fatal("expected invalid range validation error")
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

func TestLoad_APIMaxBatchSizeDefaultsTo200(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_RELAY_URLS", "")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "")
	t.Setenv("INGESTOR_RELAY_DISABLED", "")
	t.Setenv("API_MAX_BATCH_SIZE", "")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")

	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.APIMaxBatchSize != 200 {
		t.Fatalf("unexpected default API max batch size: got %d want 200", cfg.APIMaxBatchSize)
	}
}

func TestLoad_APIMaxBatchSizeFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_RELAY_URLS", "")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "")
	t.Setenv("INGESTOR_RELAY_DISABLED", "")
	t.Setenv("API_MAX_BATCH_SIZE", "75")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")

	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.APIMaxBatchSize != 75 {
		t.Fatalf("unexpected env API max batch size: got %d want 75", cfg.APIMaxBatchSize)
	}
}

func TestLoad_PrimalWSDefaultsAndEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_RELAY_URLS", "")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "")
	t.Setenv("INGESTOR_RELAY_DISABLED", "")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "")

	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("load config defaults: %v", err)
	}
	if cfg.PrimalWSMaxSubscriptions != 200 {
		t.Fatalf("unexpected default ws max subscriptions: got %d want 200", cfg.PrimalWSMaxSubscriptions)
	}
	if cfg.PrimalWSRequestTimeout.String() != "10s" {
		t.Fatalf("unexpected default ws request timeout: got %s want 10s", cfg.PrimalWSRequestTimeout)
	}
	if cfg.PrimalWSMaxMessageBytes != 1<<20 {
		t.Fatalf("unexpected default ws max message bytes: got %d want %d", cfg.PrimalWSMaxMessageBytes, 1<<20)
	}
	if cfg.PrimalWSMaxReqPerMinute != 240 {
		t.Fatalf("unexpected default ws req per minute: got %d want 240", cfg.PrimalWSMaxReqPerMinute)
	}
	if cfg.PrimalWSDMCompatRateLimitRPM != 30 {
		t.Fatalf("unexpected default ws dm req per minute: got %d want 30", cfg.PrimalWSDMCompatRateLimitRPM)
	}
	if cfg.PrimalWSAllowAnyOrigin {
		t.Fatal("expected default ws allow any origin to be false")
	}

	t.Setenv("PRIMAL_WS_MAX_SUBSCRIPTIONS", "50")
	t.Setenv("PRIMAL_WS_REQUEST_TIMEOUT", "2s")
	t.Setenv("PRIMAL_WS_MAX_MESSAGE_BYTES", "2048")
	t.Setenv("PRIMAL_WS_MAX_REQ_PER_MINUTE", "33")
	t.Setenv("HTTP_DM_COMPAT_RATE_LIMIT_RPM", "11")
	t.Setenv("PRIMAL_WS_ALLOWED_ORIGINS", "https://app.primal.net,https://nostrmash.local")
	t.Setenv("PRIMAL_WS_ALLOW_ANY_ORIGIN", "true")
	cfg, err = Load("api")
	if err != nil {
		t.Fatalf("load config env: %v", err)
	}
	if cfg.PrimalWSMaxSubscriptions != 50 {
		t.Fatalf("unexpected env ws max subscriptions: got %d want 50", cfg.PrimalWSMaxSubscriptions)
	}
	if cfg.PrimalWSRequestTimeout.String() != "2s" {
		t.Fatalf("unexpected env ws request timeout: got %s want 2s", cfg.PrimalWSRequestTimeout)
	}
	if cfg.PrimalWSMaxMessageBytes != 2048 {
		t.Fatalf("unexpected env ws max message bytes: got %d want 2048", cfg.PrimalWSMaxMessageBytes)
	}
	if cfg.PrimalWSMaxReqPerMinute != 33 {
		t.Fatalf("unexpected env ws req per minute: got %d want 33", cfg.PrimalWSMaxReqPerMinute)
	}
	if cfg.PrimalWSDMCompatRateLimitRPM != 11 {
		t.Fatalf("unexpected env ws dm req per minute: got %d want 11", cfg.PrimalWSDMCompatRateLimitRPM)
	}
	if len(cfg.PrimalWSAllowedOrigins) != 2 {
		t.Fatalf("unexpected ws allowed origins length: got %d want 2", len(cfg.PrimalWSAllowedOrigins))
	}
	if !cfg.PrimalWSAllowAnyOrigin {
		t.Fatal("expected ws allow any origin to be true from env")
	}
}

func TestLoad_HTTPRateLimitDefaultsAndEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_RELAY_URLS", "")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "")
	t.Setenv("INGESTOR_RELAY_DISABLED", "")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("HTTP_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_RATE_LIMIT_BURST", "")
	t.Setenv("HTTP_SEARCH_RATE_LIMIT_RPM", "")
	t.Setenv("HTTP_BATCH_RATE_LIMIT_RPM", "")

	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("load config defaults: %v", err)
	}
	if cfg.HTTPRateLimitRPM != 240 || cfg.HTTPRateLimitBurst != 60 {
		t.Fatalf("unexpected default http limits rpm=%d burst=%d", cfg.HTTPRateLimitRPM, cfg.HTTPRateLimitBurst)
	}
	if cfg.HTTPSearchRateLimitRPM != 60 || cfg.HTTPBatchRateLimitRPM != 40 {
		t.Fatalf("unexpected default http override limits search=%d batch=%d", cfg.HTTPSearchRateLimitRPM, cfg.HTTPBatchRateLimitRPM)
	}

	t.Setenv("HTTP_RATE_LIMIT_RPM", "120")
	t.Setenv("HTTP_RATE_LIMIT_BURST", "30")
	t.Setenv("HTTP_SEARCH_RATE_LIMIT_RPM", "20")
	t.Setenv("HTTP_BATCH_RATE_LIMIT_RPM", "10")
	cfg, err = Load("api")
	if err != nil {
		t.Fatalf("load config env: %v", err)
	}
	if cfg.HTTPRateLimitRPM != 120 || cfg.HTTPRateLimitBurst != 30 {
		t.Fatalf("unexpected env http limits rpm=%d burst=%d", cfg.HTTPRateLimitRPM, cfg.HTTPRateLimitBurst)
	}
	if cfg.HTTPSearchRateLimitRPM != 20 || cfg.HTTPBatchRateLimitRPM != 10 {
		t.Fatalf("unexpected env http override limits search=%d batch=%d", cfg.HTTPSearchRateLimitRPM, cfg.HTTPBatchRateLimitRPM)
	}
}

func TestLoad_WorkerConcurrencyDefaultsAndEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_RELAY_URLS", "")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "")
	t.Setenv("INGESTOR_RELAY_DISABLED", "")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("WORKER_CONCURRENCY", "")

	cfg, err := Load("worker")
	if err != nil {
		t.Fatalf("load config defaults: %v", err)
	}
	if cfg.WorkerConcurrency != 4 {
		t.Fatalf("unexpected default worker concurrency: got %d want 4", cfg.WorkerConcurrency)
	}

	t.Setenv("WORKER_CONCURRENCY", "8")
	cfg, err = Load("worker")
	if err != nil {
		t.Fatalf("load config env: %v", err)
	}
	if cfg.WorkerConcurrency != 8 {
		t.Fatalf("unexpected env worker concurrency: got %d want 8", cfg.WorkerConcurrency)
	}
}

func TestValidateIngestorMode_ReplayRequiresFixturePath(t *testing.T) {
	err := validateIngestorMode("ingestor", "replay", ReplayConfig{}, testRelayConfig())
	if err == nil {
		t.Fatal("expected replay mode to require fixture path")
	}
}

func TestValidateIngestorMode_LiveAcceptsEmptyReplayConfig(t *testing.T) {
	cfg := testRelayConfig()
	cfg.URLs = []string{"wss://relay.example.com"}
	cfg.Allowlist = []string{"wss://relay.example.com"}
	err := validateIngestorMode("ingestor", "live", ReplayConfig{}, cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateIngestorMode_LiveRequiresRelayURLs(t *testing.T) {
	err := validateIngestorMode("ingestor", "live", ReplayConfig{}, testRelayConfig())
	if err == nil {
		t.Fatal("expected live mode to require relay URLs")
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

	cfg, err := Load("ingestor")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Relay.LiveBootstrapLookbackSeconds != 300 {
		t.Fatalf("unexpected bootstrap lookback: got %d want 300", cfg.Relay.LiveBootstrapLookbackSeconds)
	}
	if cfg.Relay.LiveResumeOverlapSeconds != 60 {
		t.Fatalf("unexpected overlap: got %d want 60", cfg.Relay.LiveResumeOverlapSeconds)
	}
}

func TestLoad_RejectsInvalidLiveResumeConfig(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("INGESTOR_MODE", "live")
	t.Setenv("INGESTOR_RELAY_URLS", "wss://relay.example.com")
	t.Setenv("INGESTOR_RELAY_ALLOWLIST", "wss://relay.example.com")
	t.Setenv("INGESTOR_FILTER_GROUP", "default_v1")
	t.Setenv("INGESTOR_LIVE_BOOTSTRAP_LOOKBACK_SECONDS", "abc")

	if _, err := Load("ingestor"); err == nil {
		t.Fatal("expected invalid bootstrap lookback to fail")
	}
}
