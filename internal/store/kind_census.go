package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RecordKindCensus stores one count-only event and bumps the durable daily
// kind counter. A repeated event id does not count twice. The raw row is
// pruned later; kind_activity_daily is kept.
func (s *PostgresStore) RecordKindCensus(ctx context.Context, eventID, pubkey string, kind int, createdAt int64) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("store is not initialized")
	}
	inserted := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		day := time.Unix(createdAt, 0).UTC().Format("2006-01-02")
		tag, err := tx.Exec(ctx, `
			INSERT INTO kind_census_events (event_id, kind, pubkey, created_at, seen_day)
			VALUES ($1, $2, $3, $4, $5::date)
			ON CONFLICT (event_id) DO NOTHING
		`, eventID, kind, pubkey, createdAt, day)
		if err != nil {
			return fmt.Errorf("insert kind census event: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		inserted = true
		return bumpKindActivityTx(ctx, tx, pubkey, kind, day)
	})
	return inserted, err
}

func bumpKindActivityTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, day string) error {
	authorTag, err := tx.Exec(ctx, `
		INSERT INTO kind_author_daily (day, kind, pubkey)
		VALUES ($1::date, $2, $3)
		ON CONFLICT DO NOTHING
	`, day, kind, pubkey)
	if err != nil {
		return fmt.Errorf("insert kind author: %w", err)
	}
	unique := int64(0)
	if authorTag.RowsAffected() > 0 {
		unique = 1
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO kind_activity_daily (day, kind, event_count, unique_authors)
		VALUES ($1::date, $2, 1, $3)
		ON CONFLICT (day, kind) DO UPDATE
		SET event_count = kind_activity_daily.event_count + 1,
		    unique_authors = kind_activity_daily.unique_authors + EXCLUDED.unique_authors
	`, day, kind, unique)
	if err != nil {
		return fmt.Errorf("upsert kind activity: %w", err)
	}
	return nil
}
