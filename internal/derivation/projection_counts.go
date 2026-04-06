package derivation

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) ProjectReplyCounts(ctx context.Context, eventID string) error {
	return h.projectReplyCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReplyCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationReplyCounts,
		ReplyCountsVersion,
		"Project eventually-consistent reply counts from relation=reply references",
		"reply_count_contributions",
		"reply_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 1 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				if ref.Relation != "reply" {
					continue
				}
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

func (h *Handlers) ProjectReactionCounts(ctx context.Context, eventID string) error {
	return h.projectReactionCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectReactionCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationReactionCounts,
		ReactionCountsVersion,
		"Project eventually-consistent reaction counts from kind=7 e references",
		"reaction_count_contributions",
		"reaction_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 7 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

func (h *Handlers) ProjectRepostCounts(ctx context.Context, eventID string) error {
	return h.projectRepostCountsWithVersion(ctx, eventID, nil)
}

func (h *Handlers) projectRepostCountsWithVersion(ctx context.Context, eventID string, versionOverride *int) error {
	return h.projectCounts(
		ctx,
		eventID,
		DerivationRepostCounts,
		RepostCountsVersion,
		"Project eventually-consistent repost counts from kind=6 e references",
		"repost_count_contributions",
		"repost_counts",
		func(kind int, refs []derivedReference) []string {
			if kind != 6 {
				return nil
			}
			ids := make([]string, 0, len(refs))
			for _, ref := range refs {
				ids = append(ids, ref.Referenced)
			}
			return ids
		},
		versionOverride,
	)
}

func (h *Handlers) projectCounts(
	ctx context.Context,
	eventID string,
	derivationName string,
	derivationVersion int,
	derivationDescription string,
	contributionTable string,
	countsTable string,
	projector func(kind int, refs []derivedReference) []string,
	versionOverride *int,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	if projector == nil {
		return fmt.Errorf("projector is required")
	}

	var kind int
	err := h.pool.QueryRow(ctx, `
		SELECT kind
		FROM events
		WHERE id = $1
	`, eventID).Scan(&kind)
	if err != nil {
		return fmt.Errorf("load event kind for %s: %w", derivationName, err)
	}

	rawTags, err := h.loadEventTags(ctx, eventID)
	if err != nil {
		return err
	}
	references := deriveEventReferences(eventID, rawTags)
	referencedIDs := normalizeUniqueIDs(projector(kind, references))

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	writeVersion, err := resolveDerivationWriteVersion(
		ctx,
		tx,
		derivationName,
		derivationVersion,
		derivationDescription,
		versionOverride,
	)
	if err != nil {
		return err
	}

	existing, err := readExistingContributions(ctx, tx, contributionTable, eventID)
	if err != nil {
		return err
	}

	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE source_event_id = $1`, contributionTable), eventID); err != nil {
		return fmt.Errorf("delete prior contributions in %s: %w", contributionTable, err)
	}

	for _, targetEventID := range referencedIDs {
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				source_event_id, target_event_id, derivation_version
			)
			VALUES ($1, $2, $3)
			ON CONFLICT (source_event_id, target_event_id) DO UPDATE
			SET derivation_version = EXCLUDED.derivation_version,
			    projected_at = now()
		`, contributionTable), eventID, targetEventID, writeVersion)
		if err != nil {
			return fmt.Errorf("insert contribution into %s: %w", contributionTable, err)
		}
	}

	affectedTargets := make(map[string]struct{}, len(existing)+len(referencedIDs))
	for _, targetEventID := range existing {
		affectedTargets[targetEventID] = struct{}{}
	}
	for _, targetEventID := range referencedIDs {
		affectedTargets[targetEventID] = struct{}{}
	}

	for targetEventID := range affectedTargets {
		var count int64
		if err := tx.QueryRow(ctx, fmt.Sprintf(`
			SELECT COUNT(*)
			FROM %s
			WHERE target_event_id = $1
		`, contributionTable), targetEventID).Scan(&count); err != nil {
			return fmt.Errorf("read aggregate from %s: %w", contributionTable, err)
		}
		if count == 0 {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE event_id = $1`, countsTable), targetEventID); err != nil {
				return fmt.Errorf("delete zero row from %s: %w", countsTable, err)
			}
			continue
		}
		_, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (
				event_id, count, derivation_version
			)
			VALUES ($1, $2, $3)
			ON CONFLICT (event_id) DO UPDATE
			SET count = EXCLUDED.count,
			    derivation_version = EXCLUDED.derivation_version,
			    updated_at = now()
		`, countsTable), targetEventID, count, writeVersion)
		if err != nil {
			return fmt.Errorf("upsert row in %s: %w", countsTable, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s tx: %w", derivationName, err)
	}
	return nil
}

func readExistingContributions(ctx context.Context, tx pgx.Tx, table string, sourceEventID string) ([]string, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT target_event_id
		FROM %s
		WHERE source_event_id = $1
	`, table), sourceEventID)
	if err != nil {
		return nil, fmt.Errorf("load existing contributions from %s: %w", table, err)
	}
	defer rows.Close()

	out := make([]string, 0)
	for rows.Next() {
		var targetEventID string
		if err := rows.Scan(&targetEventID); err != nil {
			return nil, fmt.Errorf("scan contribution from %s: %w", table, err)
		}
		out = append(out, targetEventID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read contributions from %s: %w", table, err)
	}
	return out, nil
}
