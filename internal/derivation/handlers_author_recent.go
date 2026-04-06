package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) ProjectAuthorRecentEvent(ctx context.Context, eventID string) error {
	return h.projectAuthorRecentEventWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectAuthorRecentEventWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var pubkey string
	var createdAt int64
	err := h.pool.QueryRow(ctx, `
		SELECT pubkey, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&pubkey, &createdAt)
	if err != nil {
		return fmt.Errorf("load event for author projection: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationAuthorRecentEvents,
		AuthorRecentEventsVersion,
		"Project author recent events ordered by created_at desc, id desc",
		versionOverride,
	)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO author_recent_events (
			author_pubkey, event_id, created_at, derivation_version
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (author_pubkey, event_id) DO UPDATE
		SET created_at = EXCLUDED.created_at,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`,
		pubkey,
		eventID,
		createdAt,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert author_recent_events: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit author projection tx: %w", err)
	}
	return nil
}
