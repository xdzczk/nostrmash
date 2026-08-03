package derivation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (h *Handlers) markRebuildRunRunning(ctx context.Context, runID int64) (ProjectionRebuildRun, error) {
	out := ProjectionRebuildRun{}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	row := tx.QueryRow(ctx, `
		SELECT
			id,
			derivation_name,
			target_version,
			scope_type,
			scope_event_id,
			scope_pubkey,
			scope_start_created_at,
			scope_end_created_at,
			status,
			job_id,
			attempts,
			started_at,
			finished_at,
			last_error
		FROM projection_rebuild_runs
		WHERE id = $1
		FOR UPDATE
	`, runID)
	run, err := scanProjectionRebuildRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, fmt.Errorf("projection rebuild run %d not found", runID)
		}
		return out, fmt.Errorf("load projection rebuild run: %w", err)
	}
	if run.Status == RebuildStatusSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return out, fmt.Errorf("commit tx: %w", err)
		}
		return run, nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE projection_rebuild_runs
		SET status = $1,
		    attempts = attempts + 1,
		    started_at = now(),
		    finished_at = NULL,
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RebuildStatusRunning, run.ID); err != nil {
		return out, fmt.Errorf("mark projection rebuild run running: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit tx: %w", err)
	}
	run.Status = RebuildStatusRunning
	run.Attempts++
	now := time.Now().UTC()
	run.StartedAt = &now
	run.FinishedAt = nil
	run.LastError = nil
	return run, nil
}

func (h *Handlers) markRebuildRunFailed(ctx context.Context, runID int64, lastError string) error {
	lastError = strings.TrimSpace(lastError)
	if lastError == "" {
		lastError = "projection rebuild failed"
	}
	_, err := h.pool.Exec(ctx, `
		UPDATE projection_rebuild_runs
		SET status = $1,
		    finished_at = now(),
		    last_error = $2,
		    updated_at = now()
		WHERE id = $3
	`, RebuildStatusFailed, lastError, runID)
	if err != nil {
		return fmt.Errorf("mark projection rebuild run failed: %w", err)
	}
	return nil
}

func (h *Handlers) markRebuildRunSucceeded(ctx context.Context, run ProjectionRebuildRun) error {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `
		UPDATE projection_rebuild_runs
		SET status = $1,
		    finished_at = now(),
		    last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RebuildStatusSucceeded, run.ID); err != nil {
		return fmt.Errorf("mark projection rebuild run succeeded: %w", err)
	}

	if run.Scope.Type == RebuildScopeFull {
		if _, err := tx.Exec(ctx, `
			UPDATE derivation_active_versions
			SET active_version = $1,
			    target_version = $1,
			    updated_at = now()
			WHERE derivation_name = $2
		`, run.TargetVersion, run.DerivationName); err != nil {
			return fmt.Errorf("activate derivation version for %s: %w", run.DerivationName, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (h *Handlers) applyScopeRebuild(ctx context.Context, run ProjectionRebuildRun, def projectionDefinition) error {
	version := run.TargetVersion
	if run.Scope.Type == RebuildScopeFull && def.rebuildFull != nil {
		return def.rebuildFull(ctx, &version)
	}
	if def.rebuildProject == nil {
		return fmt.Errorf("rebuild scope %q is not supported for derivation %q", run.Scope.Type, run.DerivationName)
	}
	eventIDs, err := h.scopeEventIDs(ctx, run.Scope)
	if err != nil {
		return err
	}
	for _, eventID := range eventIDs {
		if err := def.rebuildProject(ctx, eventID, &version); err != nil {
			// Full/range rebuilds race with retention: an id can disappear between
			// the scope scan and projection. Skip those rows instead of failing
			// the entire rebuild. Event-scoped rebuilds still surface the miss.
			if errors.Is(err, pgx.ErrNoRows) && run.Scope.Type != RebuildScopeEvent {
				continue
			}
			return fmt.Errorf("rebuild %s for event %s: %w", run.DerivationName, eventID, err)
		}
	}
	return nil
}
