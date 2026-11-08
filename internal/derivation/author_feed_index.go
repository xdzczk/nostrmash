package derivation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

const authorFeedIndexBatch = 2000

// syncAuthorFeedIndexTx records a kind-1/6 note on the follows-feed index, or
// removes it when the note is a reply or not a feed kind. include is false
// for replies: the feed hides those, and the partial page index cannot.
func syncAuthorFeedIndexTx(ctx context.Context, tx pgx.Tx, pubkey string, kind int, createdAt int64, eventID string, include bool) error {
	if !include {
		if _, err := tx.Exec(ctx, `DELETE FROM author_feed_events WHERE event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete author feed index row: %w", err)
		}
		return nil
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO author_feed_events (event_id, pubkey, kind, created_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) DO UPDATE
		SET pubkey = EXCLUDED.pubkey,
		    kind = EXCLUDED.kind,
		    created_at = EXCLUDED.created_at
	`, eventID, pubkey, kind, createdAt); err != nil {
		return fmt.Errorf("upsert author feed index row: %w", err)
	}
	return nil
}

// BackfillAuthorFeedIndex copies one batch of historical top-level kind 1,
// then kind 6, notes into author_feed_events. more is false once the cursor
// has walked both kinds. New notes are indexed at insert time, so the scan
// only has to catch rows that already existed.
func (h *Handlers) BackfillAuthorFeedIndex(ctx context.Context) (more bool, err error) {
	if h == nil || h.pool == nil {
		return false, fmt.Errorf("handlers are not initialized")
	}
	started := time.Now()
	defer func() {
		metrics.ObserveDBOperation("backfill_author_feed_index", dbResultLabel(err), time.Since(started))
	}()
	err = pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var locked bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, int64(814202)).Scan(&locked); err != nil {
			return fmt.Errorf("lock author feed backfill: %w", err)
		}
		if !locked {
			return ErrBackgroundBusy
		}
		if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '30s'`); err != nil {
			return fmt.Errorf("set statement_timeout: %w", err)
		}
		var complete bool
		var kind int
		var createdAt *int64
		var eventID string
		if err := tx.QueryRow(ctx, `
			SELECT complete, cursor_kind, cursor_created_at, cursor_event_id
			FROM author_feed_index_state
			WHERE id
			FOR UPDATE
		`).Scan(&complete, &kind, &createdAt, &eventID); err != nil {
			return fmt.Errorf("read author feed index state: %w", err)
		}
		if complete {
			return nil
		}
		advanced, nextCreated, nextID, err := copyAuthorFeedBatch(ctx, tx, kind, createdAt, eventID)
		if err != nil {
			return err
		}
		if advanced {
			more = true
			if _, err := tx.Exec(ctx, `
				UPDATE author_feed_index_state
				SET cursor_created_at = $1, cursor_event_id = $2
				WHERE id
			`, nextCreated, nextID); err != nil {
				return fmt.Errorf("advance author feed cursor: %w", err)
			}
			return nil
		}
		if kind == 1 {
			more = true
			if _, err := tx.Exec(ctx, `
				UPDATE author_feed_index_state
				SET cursor_kind = 6, cursor_created_at = NULL, cursor_event_id = ''
				WHERE id
			`); err != nil {
				return fmt.Errorf("switch author feed cursor to kind 6: %w", err)
			}
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE author_feed_index_state SET complete = true WHERE id`); err != nil {
			return fmt.Errorf("complete author feed index: %w", err)
		}
		return nil
	})
	return more, err
}

func copyAuthorFeedBatch(ctx context.Context, tx pgx.Tx, kind int, createdAt *int64, eventID string) (bool, int64, string, error) {
	replyFilter := ""
	if kind == 1 {
		replyFilter = `AND NOT EXISTS (SELECT 1 FROM thread_edges te WHERE te.child_event_id = e.id)`
	}
	query := fmt.Sprintf(`
		WITH batch AS (
			SELECT e.pubkey, e.kind, e.created_at, e.id
			FROM events e
			WHERE e.kind = $1
			  AND ($2::bigint IS NULL OR (e.created_at, e.id) < ($2::bigint, $3))
			  %s
			ORDER BY e.created_at DESC, e.id DESC
			LIMIT $4
		),
		ins AS (
			INSERT INTO author_feed_events (event_id, pubkey, kind, created_at)
			SELECT id, pubkey, kind, created_at FROM batch
			ON CONFLICT (event_id) DO UPDATE
			SET pubkey = EXCLUDED.pubkey,
			    kind = EXCLUDED.kind,
			    created_at = EXCLUDED.created_at
			RETURNING 1
		)
		SELECT created_at, id
		FROM batch
		WHERE EXISTS (SELECT 1 FROM ins)
		ORDER BY created_at ASC, id ASC
		LIMIT 1
	`, replyFilter)
	var nextCreated int64
	var nextID string
	err := tx.QueryRow(ctx, query, kind, createdAt, eventID, authorFeedIndexBatch).Scan(&nextCreated, &nextID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, 0, "", nil
		}
		return false, 0, "", fmt.Errorf("copy author feed batch: %w", err)
	}
	return true, nextCreated, nextID, nil
}
