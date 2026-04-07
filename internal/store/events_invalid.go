package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
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

func (s *PostgresStore) PurgeInvalidEventsOlderThan(ctx context.Context, cutoff time.Time, limit int) (deleted int64, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveDBOperation("purge_invalid_events", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if cutoff.IsZero() {
		return 0, fmt.Errorf("cutoff is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	tag, execErr := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT id
			FROM invalid_events
			WHERE seen_at < $1
			ORDER BY seen_at ASC, id ASC
			LIMIT $2
		)
		DELETE FROM invalid_events ie
		USING candidates c
		WHERE ie.id = c.id
	`, cutoff.UTC(), limit)
	if execErr != nil {
		return 0, fmt.Errorf("purge invalid events: %w", execErr)
	}
	return tag.RowsAffected(), nil
}

func (s *PostgresStore) TrimInvalidEventPayloadsOlderThan(ctx context.Context, cutoff time.Time, limit int) (trimmed int64, err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveDBOperation("trim_invalid_event_payloads", dbResultFromErr(err), time.Since(started))
	}()
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	if cutoff.IsZero() {
		return 0, fmt.Errorf("cutoff is required")
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be > 0")
	}
	tag, execErr := s.pool.Exec(ctx, `
		WITH candidates AS (
			SELECT id
			FROM invalid_events
			WHERE seen_at < $1
			  AND raw_payload IS NOT NULL
			ORDER BY seen_at ASC, id ASC
			LIMIT $2
		)
		UPDATE invalid_events ie
		SET raw_payload = NULL
		FROM candidates c
		WHERE ie.id = c.id
	`, cutoff.UTC(), limit)
	if execErr != nil {
		return 0, fmt.Errorf("trim invalid event payloads: %w", execErr)
	}
	return tag.RowsAffected(), nil
}
