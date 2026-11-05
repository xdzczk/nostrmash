package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/config"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestExpiredAndNIP05LoopsExitOnDisabledOrInvalidConfig(t *testing.T) {
	log := &recordingLogger{}
	RunExpiredEventRetentionLoop(context.Background(), log, nil, config.WorkerExpiredEventRetentionConfig{})
	RunExpiredEventRetentionLoop(context.Background(), log, &store.PostgresStore{}, config.WorkerExpiredEventRetentionConfig{
		Enabled: true,
	})
	if !log.sawInfo("expired_event_retention_disabled") || !log.sawError("expired_event_retention_invalid_config") {
		t.Fatalf("logs info=%v errs=%v", log.info, log.errs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	RunExpiredEventRetentionLoop(ctx, log, &store.PostgresStore{}, config.WorkerExpiredEventRetentionConfig{
		Enabled:          true,
		Grace:            time.Hour,
		RunInterval:      time.Hour,
		DeleteBatchLimit: 10,
	})
	if !log.sawInfo("expired_event_retention_enabled") {
		t.Fatal("expected enabled log before the cancelled loop returned")
	}

	nipLog := &recordingLogger{}
	RunNIP05VerificationLoop(context.Background(), nipLog, nil, config.WorkerNIP05Config{})
	if !nipLog.sawInfo("nip05_verification_disabled") {
		t.Fatalf("nip05 logs info=%v", nipLog.info)
	}
}
