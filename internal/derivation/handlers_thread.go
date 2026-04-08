package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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

func (h *Handlers) updateThreadSummaryWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	var kind int
	if err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind); err != nil {
		return fmt.Errorf("load event for thread summary projection: %w", err)
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	writeVersion, err := resolveDerivationWriteVersion(
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
	if err := h.refreshThreadSummariesTx(ctx, tx, rootsToRefresh, writeVersion); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit thread summary projection tx: %w", err)
	}
	return nil
}

func (h *Handlers) collectThreadSummaryRootsTx(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
	kind int,
) (map[string]struct{}, error) {
	roots := make(map[string]struct{}, 2)
	var edgeRootID *string
	var edgeParentID string
	err := tx.QueryRow(ctx, `
		SELECT root_event_id, parent_event_id
		FROM thread_edges
		WHERE child_event_id = $1
	`, eventID).Scan(&edgeRootID, &edgeParentID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("load existing thread edge for summary roots: %w", err)
		}
		if kind == 1 {
			roots[eventID] = struct{}{}
		}
		return roots, nil
	}
	if edgeRootID != nil {
		rootID := strings.TrimSpace(*edgeRootID)
		if rootID != "" {
			roots[rootID] = struct{}{}
		}
	}
	parentID := strings.TrimSpace(edgeParentID)
	if parentID != "" {
		roots[parentID] = struct{}{}
	}
	if len(roots) == 0 && kind == 1 {
		roots[eventID] = struct{}{}
	}
	return roots, nil
}

func (h *Handlers) refreshThreadSummariesTx(
	ctx context.Context,
	tx pgx.Tx,
	roots map[string]struct{},
	writeVersion int,
) error {
	for rootID := range roots {
		rootID = strings.TrimSpace(rootID)
		if rootID == "" {
			continue
		}
		if err := h.refreshThreadSummaryTx(ctx, tx, rootID, writeVersion); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handlers) refreshThreadSummaryTx(
	ctx context.Context,
	tx pgx.Tx,
	rootEventID string,
	writeVersion int,
) error {
	rootEventID = strings.TrimSpace(rootEventID)
	if rootEventID == "" {
		return nil
	}
	var rootCreatedAt int64
	var rootKind int
	err := tx.QueryRow(ctx, `
		SELECT created_at, kind
		FROM events
		WHERE id = $1
	`, rootEventID).Scan(&rootCreatedAt, &rootKind)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if _, deleteErr := tx.Exec(ctx, `
				DELETE FROM thread_summaries
				WHERE root_event_id = $1
			`, rootEventID); deleteErr != nil {
				return fmt.Errorf("delete thread summary for missing root: %w", deleteErr)
			}
			return nil
		}
		return fmt.Errorf("load root event for thread summary: %w", err)
	}
	if rootKind != 1 {
		if _, deleteErr := tx.Exec(ctx, `
			DELETE FROM thread_summaries
			WHERE root_event_id = $1
		`, rootEventID); deleteErr != nil {
			return fmt.Errorf("delete thread summary for non-note root: %w", deleteErr)
		}
		return nil
	}

	var replyCount int64
	var participantCount int
	var lastActivityAt int64
	var replies24h int64
	var replies7d int64
	if err := tx.QueryRow(ctx, `
		WITH descendant_events AS (
			SELECT e.pubkey, e.created_at, te.child_created_at
			FROM thread_edges te
			INNER JOIN events e ON e.id = te.child_event_id
			WHERE te.root_event_id = $1
		),
		thread_participants AS (
			SELECT pubkey, created_at
			FROM descendant_events
			UNION ALL
			SELECT e.pubkey, e.created_at
			FROM events e
			WHERE e.id = $1
		)
		SELECT
			COALESCE((SELECT COUNT(*) FROM descendant_events), 0) AS reply_count,
			COALESCE((SELECT COUNT(DISTINCT pubkey) FROM thread_participants), 1) AS participant_count,
			COALESCE((SELECT MAX(created_at) FROM thread_participants), $2) AS last_activity_at,
			COALESCE((
				SELECT COUNT(*)
				FROM descendant_events
				WHERE child_created_at >= extract(epoch FROM now())::bigint - (24 * 60 * 60)
			), 0) AS replies_24h,
			COALESCE((
				SELECT COUNT(*)
				FROM descendant_events
				WHERE child_created_at >= extract(epoch FROM now())::bigint - (7 * 24 * 60 * 60)
			), 0) AS replies_7d
	`, rootEventID, rootCreatedAt).Scan(
		&replyCount,
		&participantCount,
		&lastActivityAt,
		&replies24h,
		&replies7d,
	); err != nil {
		return fmt.Errorf("compute thread summary aggregates: %w", err)
	}

	var maxDepth int
	if err := tx.QueryRow(ctx, `
		WITH RECURSIVE thread_tree(event_id, depth, path) AS (
			SELECT $1::text, 0, ARRAY[$1::text]
			UNION ALL
			SELECT
				te.child_event_id,
				thread_tree.depth + 1,
				thread_tree.path || te.child_event_id
			FROM thread_tree
			INNER JOIN thread_edges te ON te.parent_event_id = thread_tree.event_id
			WHERE te.root_event_id = $1
			  AND NOT te.child_event_id = ANY(thread_tree.path)
			  AND thread_tree.depth < 200
		)
		SELECT COALESCE(MAX(depth), 0)
		FROM thread_tree
	`, rootEventID).Scan(&maxDepth); err != nil {
		return fmt.Errorf("compute thread summary max depth: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO thread_summaries (
			root_event_id,
			reply_count,
			participant_count,
			max_depth,
			last_activity_at,
			replies_24h,
			replies_7d,
			derivation_version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (root_event_id) DO UPDATE
		SET reply_count = EXCLUDED.reply_count,
		    participant_count = EXCLUDED.participant_count,
		    max_depth = EXCLUDED.max_depth,
		    last_activity_at = EXCLUDED.last_activity_at,
		    replies_24h = EXCLUDED.replies_24h,
		    replies_7d = EXCLUDED.replies_7d,
		    derivation_version = EXCLUDED.derivation_version,
		    projected_at = now()
	`,
		rootEventID,
		replyCount,
		participantCount,
		maxDepth,
		lastActivityAt,
		replies24h,
		replies7d,
		writeVersion,
	); err != nil {
		return fmt.Errorf("upsert thread summary: %w", err)
	}
	return nil
}

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

	if err := upsertDerivationVersion(ctx, tx, DerivationThreadProjection, ThreadProjectionVersion, "Repair unresolved thread references when missing events arrive"); err != nil {
		return err
	}

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
