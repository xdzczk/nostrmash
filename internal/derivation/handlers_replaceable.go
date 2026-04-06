package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) UpdateReplaceableState(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	type replaceableEvent struct {
		Pubkey    string
		Kind      int
		CreatedAt int64
		DTag      string
	}
	var event replaceableEvent
	err := h.pool.QueryRow(ctx, `
		SELECT e.pubkey, e.kind, e.created_at,
		       COALESCE((
		           SELECT et.value
		           FROM event_tags et
		           WHERE et.event_id = e.id
		             AND et.tag_name = 'd'
		             AND et.tag_index >= 0
		             AND et.value_index = 0
		           ORDER BY et.tag_index ASC
		           LIMIT 1
		       ), '')
		FROM events e
		WHERE e.id = $1
	`, eventID).Scan(&event.Pubkey, &event.Kind, &event.CreatedAt, &event.DTag)
	if err != nil {
		return fmt.Errorf("load event for replaceable derivation: %w", err)
	}
	if !isReplaceableKind(event.Kind) {
		return nil
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertDerivationVersion(ctx, tx, DerivationReplaceableState, ReplaceableStateVersion, "Track deterministic latest-wins replaceable event state"); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO replaceable_state (
			pubkey, kind, d_tag, event_id, created_at, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (pubkey, kind, d_tag) DO UPDATE
		SET event_id = EXCLUDED.event_id,
		    created_at = EXCLUDED.created_at,
		    derivation_version = EXCLUDED.derivation_version,
		    updated_at = now()
		WHERE EXCLUDED.created_at > replaceable_state.created_at
		   OR (
		       EXCLUDED.created_at = replaceable_state.created_at
		       AND EXCLUDED.event_id > replaceable_state.event_id
		   )
	`,
		event.Pubkey,
		event.Kind,
		event.DTag,
		eventID,
		event.CreatedAt,
		ReplaceableStateVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert replaceable state: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit replaceable derivation tx: %w", err)
	}
	return nil
}

func isReplaceableKind(kind int) bool {
	if kind == 0 || kind == 3 {
		return true
	}
	if kind >= 10000 && kind < 20000 {
		return true
	}
	return kind >= 30000 && kind < 40000
}
