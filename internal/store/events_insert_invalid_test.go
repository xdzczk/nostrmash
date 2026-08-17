package store

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

func TestInsertInvalidEventWritesIsolatedQuarantineRecord(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

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

func TestInsertInvalidEventDropsPayloadForSizeErrorCodes(t *testing.T) {
	ctx := context.Background()
	dbURL := testDatabaseURL(t)
	pool := setupSchemaPool(t, ctx, dbURL)
	mustMigrateAndSeedDerivations(t, ctx, pool, "test-v1")

	store := NewPostgresStore(pool)

	cases := []struct {
		name      string
		errorCode string
	}{
		{name: "content_too_large", errorCode: "content_too_large"},
		{name: "payload_too_large", errorCode: "payload_too_large"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			invalid := model.InvalidEvent{
				SourceRelay:  "wss://relay.oversized",
				ErrorCode:    tc.errorCode,
				ErrorMessage: "content exceeds 65536 bytes",
				RawPayload:   json.RawMessage(`{"id":"big","content":"` + strings.Repeat("x", 1000) + `"}`),
				SeenAt:       time.Date(2026, 4, 4, 12, 0, 0, 0, time.UTC),
			}
			if err := store.InsertInvalidEvent(ctx, invalid); err != nil {
				t.Fatalf("insert invalid event: %v", err)
			}

			var rawPayload []byte
			err := pool.QueryRow(ctx, `
				SELECT raw_payload::text
				FROM invalid_events
				WHERE source_relay = $1 AND error_code = $2
				ORDER BY id DESC
				LIMIT 1
			`, invalid.SourceRelay, invalid.ErrorCode).Scan(&rawPayload)
			if err != nil {
				t.Fatalf("query invalid event: %v", err)
			}
			if rawPayload != nil {
				t.Fatalf("expected raw_payload to be dropped for error_code %q, got %s", tc.errorCode, string(rawPayload))
			}
		})
	}
}
