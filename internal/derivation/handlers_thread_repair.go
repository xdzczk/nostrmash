package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) RepairUnresolvedReferences(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT source_event_id
		FROM unresolved_thread_references
		WHERE missing_event_id = $1
		ORDER BY source_event_id ASC
	`, eventID)
	if err != nil {
		return fmt.Errorf("query unresolved references for repair: %w", err)
	}
	defer rows.Close()

	sourceEventIDs := make([]string, 0)
	for rows.Next() {
		var sourceEventID string
		if err := rows.Scan(&sourceEventID); err != nil {
			return fmt.Errorf("scan unresolved reference source event: %w", err)
		}
		sourceEventIDs = append(sourceEventIDs, sourceEventID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read unresolved references for repair: %w", err)
	}
	rows.Close()
	for _, sourceEventID := range sourceEventIDs {
		if err := enqueueDerivationJobTx(ctx, tx, JobTypeUpdateThreadProjection, sourceEventID, "repair:"+eventID); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit repair unresolved references tx: %w", err)
	}
	return nil
}
