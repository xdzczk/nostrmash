package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// StoragePressureState is the persisted, cross-process view of the storage
// governor's most recent decision.
type StoragePressureState struct {
	Level         int
	Ratio         float64
	DatabaseBytes int64
	CapacityBytes int64
	ComputedAt    time.Time
}

// UpsertStoragePressureState writes the governor's latest computed pressure
// level to the singleton storage_pressure_state row. Called by the worker
// governor loop.
func (s *PostgresStore) UpsertStoragePressureState(
	ctx context.Context,
	level int,
	ratio float64,
	databaseBytes int64,
	capacityBytes int64,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO storage_pressure_state (id, level, ratio, database_bytes, capacity_bytes, computed_at)
		VALUES (TRUE, $1, $2, $3, $4, now())
		ON CONFLICT (id) DO UPDATE
		SET level = EXCLUDED.level,
		    ratio = EXCLUDED.ratio,
		    database_bytes = EXCLUDED.database_bytes,
		    capacity_bytes = EXCLUDED.capacity_bytes,
		    computed_at = EXCLUDED.computed_at
	`, level, ratio, databaseBytes, capacityBytes)
	if err != nil {
		return fmt.Errorf("upsert storage pressure state: %w", err)
	}
	return nil
}

// GetStoragePressureState reads the singleton pressure row. When no row exists
// yet (governor has never run) it returns a zero-value state with level 0.
func (s *PostgresStore) GetStoragePressureState(ctx context.Context) (StoragePressureState, error) {
	if s == nil || s.pool == nil {
		return StoragePressureState{}, fmt.Errorf("store is not initialized")
	}
	var st StoragePressureState
	err := s.pool.QueryRow(ctx, `
		SELECT level, ratio, database_bytes, capacity_bytes, computed_at
		FROM storage_pressure_state
		WHERE id = TRUE
	`).Scan(&st.Level, &st.Ratio, &st.DatabaseBytes, &st.CapacityBytes, &st.ComputedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return StoragePressureState{}, nil
		}
		return StoragePressureState{}, fmt.Errorf("get storage pressure state: %w", err)
	}
	return st, nil
}

// GetDatabaseBytes returns the current pg_database_size for the active
// database. Used by the governor to compute the pressure ratio without
// pulling the full per-table storage snapshot.
func (s *PostgresStore) GetDatabaseBytes(ctx context.Context) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("store is not initialized")
	}
	var bytes int64
	if err := s.pool.QueryRow(ctx, `SELECT pg_database_size(current_database())`).Scan(&bytes); err != nil {
		return 0, fmt.Errorf("get database size: %w", err)
	}
	return bytes, nil
}
