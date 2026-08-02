package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
)

// ListRelayHealth returns the latest persisted checkpoint rows per relay/mode/filter_group.
func (s *PostgresStore) ListRelayHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	rows, err := s.pool.Query(ctx, `
		SELECT relay_url, mode, filter_group, "since", "until", cursor, eose_seen_at,
		       status, last_error, last_error_at, updated_at
		FROM ingest_checkpoints
		ORDER BY updated_at DESC, relay_url ASC
	`)
	if err != nil {
		if strings.Contains(err.Error(), `column "since" does not exist`) {
			rows, err = s.pool.Query(ctx, `
				SELECT relay_url, mode, filter_group, since_ts, until_ts, cursor_val, eose_seen_at,
				       status, NULL::text AS last_error, NULL::timestamptz AS last_error_at, updated_at
				FROM ingest_checkpoints
				ORDER BY updated_at DESC, relay_url ASC
			`)
		}
		if err != nil {
			return nil, fmt.Errorf("list relay health: %w", err)
		}
	}
	defer rows.Close()

	out := make([]model.IngestCheckpoint, 0)
	for rows.Next() {
		var row model.IngestCheckpoint
		if err := rows.Scan(
			&row.RelayURL,
			&row.Mode,
			&row.FilterGroup,
			&row.Since,
			&row.Until,
			&row.Cursor,
			&row.EOSESeenAt,
			&row.Status,
			&row.LastError,
			&row.LastErrorAt,
			&row.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan relay health row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read relay health rows: %w", err)
	}
	return out, nil
}
