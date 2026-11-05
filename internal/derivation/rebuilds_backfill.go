package derivation

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// rebuildCursorBatch keeps each scope read under the production statement
// timeout. The projection of one event is its own transaction.
const rebuildCursorBatch = 200

func (h *Handlers) rebuildEventAttributesWithVersion(ctx context.Context, versionOverride *int) error {
	return h.rebuildProjectAllKinds(ctx, DerivationEventAttributes, h.projectEventAttributesWithVersion, versionOverride)
}

func (h *Handlers) rebuildProfileFieldsWithVersion(ctx context.Context, versionOverride *int) error {
	return h.rebuildProjectKinds(ctx, DerivationProfileFields, []int{0}, h.projectProfileFieldsWithVersion, versionOverride)
}

func (h *Handlers) rebuildSurfacesWithVersion(ctx context.Context, versionOverride *int) error {
	return h.rebuildProjectKinds(ctx, DerivationSurfaces, []int{
		8, 9, 11, 1018, 1063, 1068, 1311, 1985, 9041,
		10000, 10006, 10007, 10009, 10012, 10015, 10030, 10063,
		30002, 30004, 30005, 30008, 30009, 30015, 30315,
		39000, 39001, 39002, 39003,
	}, h.projectSurfacesWithVersion, versionOverride)
}

func (h *Handlers) rebuildAuthorRecentEventsWithVersion(ctx context.Context, versionOverride *int) error {
	return h.rebuildProjectAllKinds(ctx, DerivationAuthorRecentEvents, h.projectAuthorRecentEventWithVersion, versionOverride)
}

func (h *Handlers) rebuildProjectAllKinds(
	ctx context.Context,
	derivationName string,
	project func(context.Context, string, *int) error,
	versionOverride *int,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	rows, err := h.pool.Query(ctx, `SELECT DISTINCT kind FROM events ORDER BY kind`)
	if err != nil {
		return fmt.Errorf("list event kinds: %w", err)
	}
	defer rows.Close()
	kinds := make([]int, 0)
	for rows.Next() {
		var kind int
		if err := rows.Scan(&kind); err != nil {
			return fmt.Errorf("scan event kind: %w", err)
		}
		kinds = append(kinds, kind)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read event kinds: %w", err)
	}
	return h.rebuildProjectKinds(ctx, derivationName, kinds, project, versionOverride)
}

func (h *Handlers) rebuildProjectKinds(
	ctx context.Context,
	derivationName string,
	kinds []int,
	project func(context.Context, string, *int) error,
	versionOverride *int,
) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}
	for _, kind := range kinds {
		var cursorAt int64
		cursorID := ""
		for {
			rows, err := h.pool.Query(ctx, `
				SELECT id, created_at
				FROM events
				WHERE kind = $1
				  AND (created_at, id) > ($2::bigint, $3::text)
				ORDER BY created_at ASC, id ASC
				LIMIT $4
			`, kind, cursorAt, cursorID, rebuildCursorBatch)
			if err != nil {
				return fmt.Errorf("list %s events for kind %d: %w", derivationName, kind, err)
			}
			type cursorRow struct {
				id        string
				createdAt int64
			}
			batch := make([]cursorRow, 0, rebuildCursorBatch)
			for rows.Next() {
				var row cursorRow
				if err := rows.Scan(&row.id, &row.createdAt); err != nil {
					rows.Close()
					return fmt.Errorf("scan %s event: %w", derivationName, err)
				}
				batch = append(batch, row)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return fmt.Errorf("read %s events: %w", derivationName, err)
			}
			if len(batch) == 0 {
				break
			}
			for _, row := range batch {
				if err := project(ctx, row.id, versionOverride); err != nil {
					if errors.Is(err, pgx.ErrNoRows) {
						continue
					}
					return fmt.Errorf("rebuild %s for event %s: %w", derivationName, row.id, err)
				}
			}
			last := batch[len(batch)-1]
			cursorAt = last.createdAt
			cursorID = last.id
			if len(batch) < rebuildCursorBatch {
				break
			}
		}
	}
	return nil
}
