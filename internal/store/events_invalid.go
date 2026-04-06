package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

// InsertInvalidEvent writes one invalid payload into quarantine storage.
// This intentionally uses an isolated write path from canonical ingest transactions.
func (s *PostgresStore) InsertInvalidEvent(ctx context.Context, invalid model.InvalidEvent) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	if strings.TrimSpace(invalid.ErrorCode) == "" {
		return fmt.Errorf("invalid event error_code is required")
	}
	if strings.TrimSpace(invalid.ErrorMessage) == "" {
		return fmt.Errorf("invalid event error_message is required")
	}

	seenAt := invalid.SeenAt
	if seenAt.IsZero() {
		seenAt = time.Now().UTC()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO invalid_events (source_relay, error_code, error_message, raw_payload, seen_at)
		VALUES ($1, $2, $3, $4, $5)
	`,
		invalid.SourceRelay,
		invalid.ErrorCode,
		invalid.ErrorMessage,
		json.RawMessage(invalid.RawPayload),
		seenAt,
	)
	if err != nil {
		return fmt.Errorf("insert invalid event: %w", err)
	}
	return nil
}

func (s *PostgresStore) eventExists(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS(SELECT 1 FROM events WHERE id = $1)
	`, eventID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check event existence: %w", err)
	}
	return exists, nil
}
