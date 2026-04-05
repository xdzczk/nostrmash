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
