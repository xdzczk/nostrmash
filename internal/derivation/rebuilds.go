package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type rebuildProjectionFunc func(context.Context, string, *int) error

type projectionDefinition struct {
	name           string
	compiled       int
	description    string
	rebuildProject rebuildProjectionFunc
}

func (h *Handlers) TriggerProjectionRebuild(ctx context.Context, params TriggerProjectionRebuildParams) (ProjectionRebuildRun, error) {
	out := ProjectionRebuildRun{}
	if h == nil || h.pool == nil {
		return out, fmt.Errorf("handlers are not initialized")
	}

	def, err := h.projectionDefinition(params.DerivationName)
	if err != nil {
		return out, err
	}
	scope, err := normalizeRebuildScope(params.Scope)
	if err != nil {
		return out, err
	}

	targetVersion := params.TargetVersion
	if targetVersion <= 0 {
		targetVersion = def.compiled
	}
	if targetVersion <= 0 {
		return out, fmt.Errorf("target version must be positive")
	}

	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertDerivationVersion(ctx, tx, def.name, targetVersion, def.description); err != nil {
		return out, err
	}

	var runID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO projection_rebuild_runs (
			derivation_name,
			target_version,
			scope_type,
			scope_event_id,
			scope_pubkey,
			scope_start_created_at,
			scope_end_created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`,
		def.name,
		targetVersion,
		scope.Type,
		nullIfBlank(scope.EventID),
		nullIfBlank(scope.Pubkey),
		scope.StartCreatedAt,
		scope.EndCreatedAt,
	).Scan(&runID)
	if err != nil {
		return out, fmt.Errorf("insert projection rebuild run: %w", err)
	}

	payload, err := json.Marshal(RebuildProjectionScopeJobPayload{RunID: runID})
	if err != nil {
		return out, fmt.Errorf("encode rebuild job payload: %w", err)
	}
	idempotencyKey := fmt.Sprintf("%s:%d", JobTypeRebuildProjectionScope, runID)
	var jobID int64
	err = tx.QueryRow(ctx, `
		INSERT INTO jobs (job_type, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, now())
		RETURNING id
	`,
		JobTypeRebuildProjectionScope,
		json.RawMessage(payload),
		idempotencyKey,
		5,
	).Scan(&jobID)
	if err != nil {
		return out, fmt.Errorf("enqueue rebuild job: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE projection_rebuild_runs
		SET job_id = $1, updated_at = now()
		WHERE id = $2
	`, jobID, runID); err != nil {
		return out, fmt.Errorf("link rebuild run to job: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit trigger projection rebuild tx: %w", err)
	}
	return h.GetProjectionRebuildRun(ctx, runID)
}

func (h *Handlers) GetProjectionRebuildRun(ctx context.Context, runID int64) (ProjectionRebuildRun, error) {
	out := ProjectionRebuildRun{}
	if h == nil || h.pool == nil {
		return out, fmt.Errorf("handlers are not initialized")
	}
	row := h.pool.QueryRow(ctx, `
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
	`, runID)
	run, err := scanProjectionRebuildRun(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return out, fmt.Errorf("projection rebuild run %d not found", runID)
		}
		return out, fmt.Errorf("get projection rebuild run: %w", err)
	}
	return run, nil
}

func (h *Handlers) ExecuteProjectionRebuildRun(ctx context.Context, runID int64) error {
	if h == nil || h.pool == nil {
		return fmt.Errorf("handlers are not initialized")
	}

	run, err := h.markRebuildRunRunning(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status == RebuildStatusSucceeded {
		return nil
	}

	def, err := h.projectionDefinition(run.DerivationName)
	if err != nil {
		return err
	}
	if err := h.applyScopeRebuild(ctx, run, def); err != nil {
		_ = h.markRebuildRunFailed(ctx, run.ID, err.Error())
		return err
	}
	return h.markRebuildRunSucceeded(ctx, run)
}
