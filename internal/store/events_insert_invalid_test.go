package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertInvalidEventWritesIsolatedQuarantineRecord(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	if err := Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	store := NewPostgresStore(pool)
	invalid := model.InvalidEvent{
		SourceRelay:  "wss://relay.bad",
		ErrorCode:    "signature_invalid",
		ErrorMessage: "signature does not verify",
		RawPayload:   json.RawMessage(`{"id":"bad","sig":"oops"}`),
		SeenAt:       time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC),
	}
	if err := store.InsertInvalidEvent(ctx, invalid); err != nil {
		t.Fatalf("insert invalid event: %v", err)
	}

	var (
		sourceRelay  string
		errorCode    string
		errorMessage string
		rawPayload   []byte
	)
	err := pool.QueryRow(ctx, `
		SELECT source_relay, error_code, error_message, raw_payload::text
		FROM invalid_events
		LIMIT 1
	`).Scan(&sourceRelay, &errorCode, &errorMessage, &rawPayload)
	if err != nil {
		t.Fatalf("query invalid event: %v", err)
	}
	if sourceRelay != invalid.SourceRelay || errorCode != invalid.ErrorCode || errorMessage != invalid.ErrorMessage {
		t.Fatalf("unexpected invalid event fields")
	}
	if !jsonEqual(rawPayload, invalid.RawPayload) {
		t.Fatalf("raw payload mismatch: got %s want %s", string(rawPayload), string(invalid.RawPayload))
	}
}
