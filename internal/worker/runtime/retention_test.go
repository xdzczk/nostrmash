package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
)

func invalidEventRetentionConfig() config.WorkerInvalidEventRetentionConfig {
	return config.WorkerInvalidEventRetentionConfig{
		Enabled:          true,
		MaxAge:           24 * time.Hour,
		RunInterval:      time.Minute,
		DeleteBatchLimit: 100,
	}
}

func TestRunInvalidEventsRetentionLoop_Disabled(t *testing.T) {
	log := &recordingLogger{}
	cfg := invalidEventRetentionConfig()
	cfg.Enabled = false
	RunInvalidEventsRetentionLoop(context.Background(), log, &fakeInvalidEventRetentionStore{}, cfg)
	if !log.sawInfo("invalid_events_retention_disabled") {
		t.Fatal("expected disabled info log")
	}
}

func TestRunInvalidEventsRetentionLoop_InvalidConfig(t *testing.T) {
	log := &recordingLogger{}
	cfg := invalidEventRetentionConfig()
	cfg.DeleteBatchLimit = 0
	RunInvalidEventsRetentionLoop(context.Background(), log, &fakeInvalidEventRetentionStore{}, cfg)
	if !log.sawError("invalid_events_retention_invalid_config") {
		t.Fatal("expected invalid-config error log")
	}
}

func TestRunInvalidEventsRetentionLoop_TicksThenExitsOnCancel(t *testing.T) {
	log := &recordingLogger{}
	cfg := invalidEventRetentionConfig()
	cfg.RunInterval = time.Millisecond
	cfg.PayloadTrim = config.WorkerInvalidEventPayloadTrimConfig{Enabled: true, MaxAge: time.Hour, BatchLimit: 50}
	store := &fakeInvalidEventRetentionStore{purged: 5, trimmed: 2}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	RunInvalidEventsRetentionLoop(ctx, log, store, cfg)

	if store.purgeSeen == 0 {
		t.Fatal("expected at least one purge tick")
	}
	if store.trimSeen == 0 {
		t.Fatal("expected at least one payload-trim tick when trim enabled")
	}
	if !log.sawInfo("invalid_events_retention_enabled") {
		t.Fatal("expected enabled info log")
	}
}
