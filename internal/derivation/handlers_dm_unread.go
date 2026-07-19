package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectDMUnreadCounts(ctx context.Context, eventID string) error {
	return h.projectDMUnreadCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectDMUnreadCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	var senderPubkey string
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, pubkey, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind, &senderPubkey, &createdAt); err != nil {
		return fmt.Errorf("load event for dm projection: %w", err)
	}
	if kind != 4 {
		return nil
	}

	rows, err := h.pool.Query(ctx, `
		SELECT value
		FROM event_tags
		WHERE event_id = $1
		  AND tag_name = 'p'
		  AND value_index = 0
	`, eventID)
	if err != nil {
		return fmt.Errorf("load dm recipients: %w", err)
	}
	defer rows.Close()
	receivers := make([]string, 0, 4)
	for rows.Next() {
		var receiver string
		if err := rows.Scan(&receiver); err != nil {
			return fmt.Errorf("scan dm recipient row: %w", err)
		}
		receiver = strings.TrimSpace(receiver)
		if receiver != "" {
			receivers = append(receivers, receiver)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read dm recipient rows: %w", err)
	}
	receivers = normalizeUniqueIDs(receivers)
	if len(receivers) == 0 {
		return nil
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationDMUnreadCounts,
		DMUnreadCountsVersion,
		"Track unread DM counters by receiver and sender",
		versionOverride,
	); err != nil {
		return err
	}

	for _, receiver := range receivers {
		if receiver == senderPubkey {
			continue
		}
		if err := h.recomputeDMUnreadPairAndAggregate(ctx, tx, receiver, senderPubkey); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dm projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) recomputeDMUnreadPairAndAggregate(ctx context.Context, tx pgx.Tx, receiver string, sender string) error {
	receiver = strings.TrimSpace(receiver)
	sender = strings.TrimSpace(sender)
	if receiver == "" || sender == "" {
		return nil
	}
	pairCount, pairLatestAt, pairLatestID, err := h.computeDMUnreadCounterTx(ctx, tx, receiver, sender)
	if err != nil {
		return err
	}
	if err := h.upsertDMUnreadCounterTx(ctx, tx, receiver, sender, pairCount, pairLatestAt, pairLatestID); err != nil {
		return err
	}
	aggregateCount, aggregateLatestAt, aggregateLatestID, err := h.computeDMUnreadCounterTx(ctx, tx, receiver, "")
	if err != nil {
		return err
	}
	return h.upsertDMUnreadCounterTx(ctx, tx, receiver, "", aggregateCount, aggregateLatestAt, aggregateLatestID)
}

func (h *Handlers) computeDMUnreadCounterTx(
	ctx context.Context,
	tx pgx.Tx,
	receiver string,
	sender string,
) (int64, int64, string, error) {
	var count int64
	var latestAt int64
	var latestEventID string
	err := tx.QueryRow(ctx, `
		WITH unread AS (
			SELECT e.id, e.created_at
			FROM events e
			INNER JOIN event_tags et
			        ON et.event_id = e.id
			       AND et.tag_name = 'p'
			       AND et.value_index = 0
			LEFT JOIN dm_read_cursors c
			       ON c.user_pubkey = $1
			      AND c.peer_pubkey = e.pubkey
			WHERE e.kind = 4
			  AND et.value = $1
			  AND e.pubkey <> $1
			  AND ($2 = '' OR e.pubkey = $2)
			  AND NOT EXISTS (
				SELECT 1
				FROM deletion_events d
				WHERE d.target_event_id = e.id
				  AND d.deleter_pubkey = e.pubkey
			  )
			  AND (
				c.user_pubkey IS NULL
				OR e.created_at > c.last_read_created_at
				OR (
					e.created_at = c.last_read_created_at
					AND e.id > c.last_read_event_id
				)
			  )
		)
		SELECT
			COALESCE((SELECT count(*) FROM unread), 0),
			COALESCE((SELECT created_at FROM unread ORDER BY created_at DESC, id DESC LIMIT 1), 0),
			COALESCE((SELECT id FROM unread ORDER BY created_at DESC, id DESC LIMIT 1), '')
	`, receiver, sender).Scan(&count, &latestAt, &latestEventID)
	if err != nil {
		return 0, 0, "", fmt.Errorf("compute dm unread counter: %w", err)
	}
	return count, latestAt, latestEventID, nil
}

func (h *Handlers) upsertDMUnreadCounterTx(
	ctx context.Context,
	tx pgx.Tx,
	receiver string,
	sender string,
	count int64,
	latestAt int64,
	latestEventID string,
) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO dm_unread_counts (
			receiver_pubkey, sender_pubkey, cnt, latest_at, latest_event_id, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (receiver_pubkey, sender_pubkey) DO UPDATE
		SET cnt = EXCLUDED.cnt,
		    latest_at = EXCLUDED.latest_at,
		    latest_event_id = EXCLUDED.latest_event_id,
		    updated_at = now()
	`, receiver, sender, count, latestAt, latestEventID); err != nil {
		return fmt.Errorf("upsert dm unread counter: %w", err)
	}
	return nil
}

func (h *Handlers) reconcileDMUnreadForDeletedTarget(ctx context.Context, tx pgx.Tx, targetEventID string) error {
	targetEventID = strings.TrimSpace(targetEventID)
	if targetEventID == "" {
		return nil
	}
	var kind int
	var sender string
	err := tx.QueryRow(ctx, `
		SELECT kind, pubkey
		FROM events
		WHERE id = $1
	`, targetEventID).Scan(&kind, &sender)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("load deleted target event: %w", err)
	}
	if kind != 4 {
		return nil
	}
	rows, err := tx.Query(ctx, `
		SELECT value
		FROM event_tags
		WHERE event_id = $1
		  AND tag_name = 'p'
		  AND value_index = 0
	`, targetEventID)
	if err != nil {
		return fmt.Errorf("load deleted dm recipients: %w", err)
	}
	defer rows.Close()
	receivers := make([]string, 0, 4)
	for rows.Next() {
		var receiver string
		if err := rows.Scan(&receiver); err != nil {
			return fmt.Errorf("scan deleted dm recipient: %w", err)
		}
		receiver = strings.TrimSpace(receiver)
		if receiver != "" {
			receivers = append(receivers, receiver)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read deleted dm recipient rows: %w", err)
	}
	receivers = normalizeUniqueIDs(receivers)
	for _, receiver := range receivers {
		if receiver == sender {
			continue
		}
		if err := h.recomputeDMUnreadPairAndAggregate(ctx, tx, receiver, sender); err != nil {
			return err
		}
	}
	return nil
}
