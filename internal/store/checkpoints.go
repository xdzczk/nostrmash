package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/model"
)

// GetIngestCheckpoint returns a checkpoint for relay/mode/filter_group, if present.
func (s *PostgresStore) GetIngestCheckpoint(
	ctx context.Context,
	relayURL string,
	mode string,
	filterGroup string,
) (*model.IngestCheckpoint, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	relayURL = strings.TrimSpace(relayURL)
	mode = strings.TrimSpace(mode)
	filterGroup = strings.TrimSpace(filterGroup)
	if relayURL == "" || mode == "" || filterGroup == "" {
		return nil, fmt.Errorf("relay url, mode, and filter group are required")
	}

	var checkpoint model.IngestCheckpoint
	err := s.pool.QueryRow(ctx, `
		SELECT relay_url, mode, filter_group, since, "until", cursor, eose_seen_at, status, updated_at
		FROM ingest_checkpoints
		WHERE relay_url = $1 AND mode = $2 AND filter_group = $3
	`,
		relayURL, mode, filterGroup,
	).Scan(
		&checkpoint.RelayURL,
		&checkpoint.Mode,
		&checkpoint.FilterGroup,
		&checkpoint.Since,
		&checkpoint.Until,
		&checkpoint.Cursor,
		&checkpoint.EOSESeenAt,
		&checkpoint.Status,
		&checkpoint.UpdatedAt,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("select checkpoint: %w", err)
	}
	return &checkpoint, nil
}

// UpsertIngestCheckpoint writes/updates checkpoint state atomically by key.
func (s *PostgresStore) UpsertIngestCheckpoint(ctx context.Context, checkpoint model.IngestCheckpoint) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	checkpoint.RelayURL = strings.TrimSpace(checkpoint.RelayURL)
	checkpoint.Mode = strings.TrimSpace(checkpoint.Mode)
	checkpoint.FilterGroup = strings.TrimSpace(checkpoint.FilterGroup)
	checkpoint.Status = strings.TrimSpace(checkpoint.Status)
	if checkpoint.RelayURL == "" || checkpoint.Mode == "" || checkpoint.FilterGroup == "" {
		return fmt.Errorf("relay url, mode, and filter group are required")
	}
	if checkpoint.Status == "" {
		return fmt.Errorf("checkpoint status is required")
	}
	updatedAt := checkpoint.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO ingest_checkpoints (
			relay_url, mode, filter_group, since, "until", cursor, eose_seen_at, status, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (relay_url, mode, filter_group) DO UPDATE
		SET since = EXCLUDED.since,
			"until" = EXCLUDED."until",
			cursor = EXCLUDED.cursor,
			eose_seen_at = EXCLUDED.eose_seen_at,
			status = EXCLUDED.status,
			updated_at = EXCLUDED.updated_at
	`,
		checkpoint.RelayURL,
		checkpoint.Mode,
		checkpoint.FilterGroup,
		checkpoint.Since,
		checkpoint.Until,
		checkpoint.Cursor,
		checkpoint.EOSESeenAt,
		checkpoint.Status,
		updatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert checkpoint: %w", err)
	}
	return nil
}
