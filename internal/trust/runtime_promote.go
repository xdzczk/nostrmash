package trust

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func (r *Runtime) executePromoteRun(ctx context.Context, runID int64, snapshotRef string) error {
	started := time.Now()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust promote tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run for promote: %w", err)
	}
	if status == RunStatusSucceeded || status == RunStatusFailed {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustPromoteRun, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhasePromote, "skipped", time.Since(started))
		return nil
	}

	if _, err := tx.Exec(ctx, `
		UPDATE trust_runs
		SET current_phase = $1,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RunPhasePromote, runID); err != nil {
		return fmt.Errorf("mark trust run promote phase: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM trust_scores_global`); err != nil {
		return fmt.Errorf("clear previous trust global scores: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO trust_scores_global (
			pubkey,
			score,
			rank,
			run_id,
			derivation_name,
			target_version,
			computed_at,
			created_at,
			updated_at
		)
		SELECT
			pubkey,
			score,
			rank,
			run_id,
			derivation_name,
			target_version,
			computed_at,
			now(),
			now()
		FROM trust_scores_global_stage
		WHERE run_id = $1
		ORDER BY rank ASC
	`, runID)
	if err != nil {
		return fmt.Errorf("publish staged trust global scores: %w", err)
	}
	metrics.AddTrustScoreRowsPublished(tag.RowsAffected())

	// Stage rows are only needed until promote publishes them into
	// trust_scores_global. Leaving them forever blew trust_scores_global_stage
	// to tens of GB across many runs. Clear this run's stage in the same
	// transaction. Pre-existing backlog from older runs should be reclaimed
	// with a one-time TRUNCATE/DELETE when no trust promote is in flight —
	// do not DELETE the whole history here (can be 100M+ rows under load).
	if _, err := tx.Exec(ctx, `
		DELETE FROM trust_scores_global_stage
		WHERE run_id = $1
	`, runID); err != nil {
		return fmt.Errorf("clear trust score staging rows after promote: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    redis_snapshot_ref = COALESCE(NULLIF($2, ''), redis_snapshot_ref),
		    phase_finished_at = now(),
		    finished_at = now(),
		    updated_at = now()
		WHERE id = $3
	`, RunStatusSucceeded, snapshotRef, runID)
	if err != nil {
		return fmt.Errorf("mark trust run succeeded in promote: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust promote tx: %w", err)
	}
	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustPromoteRun, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhasePromote, "success", time.Since(started))
	return nil
}
