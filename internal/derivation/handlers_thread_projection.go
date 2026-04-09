package derivation

import (
	"context"
	"fmt"
	"strings"
)

func (h *Handlers) UpdateThreadProjection(ctx context.Context, eventID string) error {
	return h.updateThreadProjectionWithVersion(ctx, eventID, nil)
}

func (h *Handlers) updateThreadProjectionWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	var kind int
	var createdAt int64
	if err := h.pool.QueryRow(ctx, `
		SELECT kind, created_at
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind, &createdAt); err != nil {
		return fmt.Errorf("load event for thread projection: %w", err)
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationThreadProjection,
		ThreadProjectionVersion,
		"Project reply parent/root edges with unresolved reference tracking",
		versionOverride,
	)
	if err != nil {
		return err
	}
	summaryVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		DerivationThreadSummary,
		ThreadSummaryVersion,
		"Project root-level thread summary counters and velocity hints",
		versionOverride,
	)
	if err != nil {
		return err
	}

	rootsToRefresh, err := h.collectThreadSummaryRootsTx(ctx, tx, eventID, kind)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM unresolved_thread_references WHERE source_event_id = $1`, eventID); err != nil {
		return fmt.Errorf("delete unresolved thread references: %w", err)
	}

	if kind != 1 {
		if _, err := tx.Exec(ctx, `DELETE FROM thread_edges WHERE child_event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete non-thread edge: %w", err)
		}
		if err := h.refreshThreadSummariesTx(ctx, tx, rootsToRefresh, summaryVersion); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit non-thread projection tx: %w", err)
		}
		return nil
	}

	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	refs := deriveEventReferences(eventID, rawTags)
	parentEventID := firstReferenceByRelation(refs, "reply")
	rootEventID := firstReferenceByRelation(refs, "root")
	if parentEventID == "" {
		parentEventID = rootEventID
	}
	if rootEventID == "" {
		rootEventID = parentEventID
	}

	if parentEventID == "" {
		if _, err := tx.Exec(ctx, `DELETE FROM thread_edges WHERE child_event_id = $1`, eventID); err != nil {
			return fmt.Errorf("delete thread edge with no parent: %w", err)
		}
		if _, ok := rootsToRefresh[eventID]; !ok {
			rootsToRefresh[eventID] = struct{}{}
		}
		if err := h.refreshThreadSummariesTx(ctx, tx, rootsToRefresh, summaryVersion); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit thread projection tx with no parent: %w", err)
		}
		return nil
	}

	parentExists, err := eventExistsTx(ctx, tx, parentEventID)
	if err != nil {
		return err
	}
	parentMissing := !parentExists
	rootMissing := false
	if rootEventID != "" {
		if rootEventID == parentEventID {
			rootMissing = parentMissing
		} else {
			rootExists, existsErr := eventExistsTx(ctx, tx, rootEventID)
			if existsErr != nil {
				return existsErr
			}
			rootMissing = !rootExists
		}
	}

	if parentMissing {
		if _, err := tx.Exec(ctx, `
			INSERT INTO unresolved_thread_references (
				source_event_id, missing_event_id, relation, derivation_version
			)
			VALUES ($1, $2, 'reply', $3)
			ON CONFLICT (source_event_id, missing_event_id, relation) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    detected_at = now()
		`, eventID, parentEventID, writeVersion); err != nil {
			return fmt.Errorf("upsert unresolved reply reference: %w", err)
		}
	}
	if rootEventID != "" && rootMissing && rootEventID != parentEventID {
		if _, err := tx.Exec(ctx, `
			INSERT INTO unresolved_thread_references (
				source_event_id, missing_event_id, relation, derivation_version
			)
			VALUES ($1, $2, 'root', $3)
			ON CONFLICT (source_event_id, missing_event_id, relation) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    detected_at = now()
		`, eventID, rootEventID, writeVersion); err != nil {
			return fmt.Errorf("upsert unresolved root reference: %w", err)
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO thread_edges (
			child_event_id, child_created_at, parent_event_id, root_event_id, parent_missing, root_missing, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (child_event_id) DO UPDATE
		SET child_created_at = EXCLUDED.child_created_at,
		    parent_event_id = EXCLUDED.parent_event_id,
		    root_event_id = EXCLUDED.root_event_id,
		    parent_missing = EXCLUDED.parent_missing,
		    root_missing = EXCLUDED.root_missing,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`,
		eventID,
		createdAt,
		parentEventID,
		nullIfBlank(rootEventID),
		parentMissing,
		rootMissing,
		writeVersion,
	)
	if err != nil {
		return fmt.Errorf("upsert thread edge: %w", err)
	}
	if rootEventID != "" {
		rootsToRefresh[rootEventID] = struct{}{}
	}
	if err := h.refreshThreadSummariesTx(ctx, tx, rootsToRefresh, summaryVersion); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit thread projection tx: %w", err)
	}
	return nil
}
