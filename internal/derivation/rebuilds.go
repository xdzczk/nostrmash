package derivation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

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
	eventIDs, err := h.scopeEventIDs(ctx, run.Scope)
	if err != nil {
		return err
	}
	version := run.TargetVersion
	for _, eventID := range eventIDs {
		if err := def.rebuildProject(ctx, eventID, &version); err != nil {
			return fmt.Errorf("rebuild %s for event %s: %w", run.DerivationName, eventID, err)
		}
	}
	return nil
}

func (h *Handlers) scopeEventIDs(ctx context.Context, scope ProjectionRebuildScope) ([]string, error) {
	switch scope.Type {
	case RebuildScopeEvent:
		return []string{scope.EventID}, nil
	case RebuildScopePubkey:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			WHERE pubkey = $1
			ORDER BY created_at ASC, id ASC
		`, scope.Pubkey)
	case RebuildScopeTimeRange:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			WHERE created_at >= $1
			  AND created_at <= $2
			ORDER BY created_at ASC, id ASC
		`, *scope.StartCreatedAt, *scope.EndCreatedAt)
	case RebuildScopeFull:
		return h.queryEventIDs(ctx, `
			SELECT id
			FROM events
			ORDER BY created_at ASC, id ASC
		`)
	default:
		return nil, fmt.Errorf("unsupported rebuild scope type %q", scope.Type)
	}
}

func (h *Handlers) queryEventIDs(ctx context.Context, query string, args ...any) ([]string, error) {
	rows, err := h.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query scope events: %w", err)
	}
	defer rows.Close()
	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan scope event id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scope events: %w", err)
	}
	return ids, nil
}

func (h *Handlers) projectionDefinition(derivationName string) (projectionDefinition, error) {
	normalized := strings.TrimSpace(derivationName)
	switch normalized {
	case DerivationProfilesLatest:
		return projectionDefinition{
			name:           DerivationProfilesLatest,
			compiled:       ProfilesLatestVersion,
			description:    "Project latest effective replaceable metadata (kind 0) per pubkey",
			rebuildProject: h.projectProfilesLatestWithVersion,
		}, nil
	case DerivationAuthorRecentEvents:
		return projectionDefinition{
			name:           DerivationAuthorRecentEvents,
			compiled:       AuthorRecentEventsVersion,
			description:    "Project author recent events ordered by created_at desc, id desc",
			rebuildProject: h.projectAuthorRecentEventWithVersion,
		}, nil
	case DerivationReplyCounts:
		return projectionDefinition{
			name:           DerivationReplyCounts,
			compiled:       ReplyCountsVersion,
			description:    "Project eventually-consistent reply counts from relation=reply references",
			rebuildProject: h.projectReplyCountsWithVersion,
		}, nil
	case DerivationReactionCounts:
		return projectionDefinition{
			name:           DerivationReactionCounts,
			compiled:       ReactionCountsVersion,
			description:    "Project eventually-consistent reaction counts from kind=7 e references",
			rebuildProject: h.projectReactionCountsWithVersion,
		}, nil
	case DerivationRepostCounts:
		return projectionDefinition{
			name:           DerivationRepostCounts,
			compiled:       RepostCountsVersion,
			description:    "Project eventually-consistent repost counts from kind=6 e references",
			rebuildProject: h.projectRepostCountsWithVersion,
		}, nil
	case DerivationReactionEvents:
		return projectionDefinition{
			name:           DerivationReactionEvents,
			compiled:       ReactionEventsVersion,
			description:    "Project reaction_events records from kind=7 references",
			rebuildProject: h.projectReactionEventsWithVersion,
		}, nil
	case DerivationRepostEvents:
		return projectionDefinition{
			name:           DerivationRepostEvents,
			compiled:       RepostEventsVersion,
			description:    "Project repost_events records from kind=6 references",
			rebuildProject: h.projectRepostEventsWithVersion,
		}, nil
	case DerivationDeletionEvents:
		return projectionDefinition{
			name:           DerivationDeletionEvents,
			compiled:       DeletionEventsVersion,
			description:    "Project deletion_events records from kind=5 references",
			rebuildProject: h.projectDeletionEventsWithVersion,
		}, nil
	case DerivationContactListsLatest:
		return projectionDefinition{
			name:        DerivationContactListsLatest,
			compiled:    ContactListsLatestVersion,
			description: "Project contact_lists_latest from kind=3 replaceables",
			rebuildProject: func(ctx context.Context, eventID string, version *int) error {
				return h.projectContactListsLatestWithVersion(ctx, eventID, version)
			},
		}, nil
	case DerivationRelayListsLatest:
		return projectionDefinition{
			name:        DerivationRelayListsLatest,
			compiled:    RelayListsLatestVersion,
			description: "Project relay_lists_latest from kind=10002 replaceables",
			rebuildProject: func(ctx context.Context, eventID string, version *int) error {
				return h.projectRelayListsLatestWithVersion(ctx, eventID, version)
			},
		}, nil
	case DerivationThreadProjection:
		return projectionDefinition{
			name:           DerivationThreadProjection,
			compiled:       ThreadProjectionVersion,
			description:    "Project reply parent/root edges with unresolved reference tracking",
			rebuildProject: h.updateThreadProjectionWithVersion,
		}, nil
	case DerivationDMUnreadCounts:
		return projectionDefinition{
			name:           DerivationDMUnreadCounts,
			compiled:       DMUnreadCountsVersion,
			description:    "Track unread DM counters by receiver and sender",
			rebuildProject: h.projectDMUnreadCountsWithVersion,
		}, nil
	case DerivationZapReceipts:
		return projectionDefinition{
			name:           DerivationZapReceipts,
			compiled:       ZapReceiptsVersion,
			description:    "Project zap receipts by sender, receiver, target event, and sats",
			rebuildProject: h.projectZapReceiptsWithVersion,
		}, nil
	default:
		return projectionDefinition{}, fmt.Errorf("projection rebuild is not supported for derivation %q", normalized)
	}
}

func normalizeRebuildScope(scope ProjectionRebuildScope) (ProjectionRebuildScope, error) {
	out := ProjectionRebuildScope{
		Type: strings.ToLower(strings.TrimSpace(scope.Type)),
	}
	switch out.Type {
	case RebuildScopeFull:
		return out, nil
	case RebuildScopeEvent, "event-scoped":
		out.Type = RebuildScopeEvent
		out.EventID = strings.TrimSpace(scope.EventID)
		if out.EventID == "" {
			return out, fmt.Errorf("event_id is required for event rebuild scope")
		}
		return out, nil
	case RebuildScopePubkey, "pubkey-scoped":
		out.Type = RebuildScopePubkey
		out.Pubkey = strings.TrimSpace(scope.Pubkey)
		if out.Pubkey == "" {
			return out, fmt.Errorf("pubkey is required for pubkey rebuild scope")
		}
		return out, nil
	case RebuildScopeTimeRange, "time-range":
		out.Type = RebuildScopeTimeRange
		if scope.StartCreatedAt == nil || scope.EndCreatedAt == nil {
			return out, fmt.Errorf("start_created_at and end_created_at are required for time_range rebuild scope")
		}
		if *scope.StartCreatedAt > *scope.EndCreatedAt {
			return out, fmt.Errorf("start_created_at must be <= end_created_at")
		}
		out.StartCreatedAt = scope.StartCreatedAt
		out.EndCreatedAt = scope.EndCreatedAt
		return out, nil
	default:
		return out, fmt.Errorf("unsupported rebuild scope type %q", scope.Type)
	}
}

type rebuildRunRowScanner interface {
	Scan(dest ...any) error
}

func scanProjectionRebuildRun(row rebuildRunRowScanner) (ProjectionRebuildRun, error) {
	out := ProjectionRebuildRun{}
	var scopeEventID *string
	var scopePubkey *string
	err := row.Scan(
		&out.ID,
		&out.DerivationName,
		&out.TargetVersion,
		&out.Scope.Type,
		&scopeEventID,
		&scopePubkey,
		&out.Scope.StartCreatedAt,
		&out.Scope.EndCreatedAt,
		&out.Status,
		&out.JobID,
		&out.Attempts,
		&out.StartedAt,
		&out.FinishedAt,
		&out.LastError,
	)
	if err != nil {
		return out, err
	}
	if scopeEventID != nil {
		out.Scope.EventID = *scopeEventID
	}
	if scopePubkey != nil {
		out.Scope.Pubkey = *scopePubkey
	}
	return out, nil
}
