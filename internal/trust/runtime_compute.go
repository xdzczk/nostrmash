package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func (r *Runtime) executeGlobalScoresRun(ctx context.Context, runID int64, snapshotRef string) (err error) {
	started := time.Now()

	// Keep the DB transaction short. Loading adjacency from Redis/Postgres and
	// iterative ranking can take minutes; holding a transaction open across
	// that work previously left COPY stuck and tripped idle-in-transaction /
	// stale-recovery paths in production.
	snapshotRef, skipped, err := r.claimComputePhase(ctx, runID, snapshotRef)
	if err != nil {
		return err
	}
	if skipped {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeGlobalScore, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhaseCompute, "skipped", time.Since(started))
		return nil
	}

	var adjacency map[string][]string
	var nodeSet map[string]struct{}
	if r.enableRedisSync && r.redis != nil && strings.TrimSpace(snapshotRef) != "" && snapshotRef != "postgres-only" {
		adjacency, nodeSet, err = r.loadAdjacencyFromRedis(ctx, runID, snapshotRef)
		if err != nil {
			return err
		}
	} else {
		adjacency, nodeSet, err = r.loadAdjacencyFromPostgres(ctx)
		if err != nil {
			return err
		}
	}

	ranked := computeIterativeGlobalRank(adjacency, nodeSet)
	followerEdges := int64(0)
	for _, neighbors := range adjacency {
		followerEdges += int64(len(neighbors))
	}

	if err := r.persistComputeResults(ctx, runID, snapshotRef, ranked, followerEdges); err != nil {
		return err
	}
	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeGlobalScore, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhaseCompute, "success", time.Since(started))
	return nil
}

func (r *Runtime) claimComputePhase(ctx context.Context, runID int64, snapshotRef string) (string, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin trust compute claim tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, fmt.Errorf("trust run %d not found", runID)
		}
		return "", false, fmt.Errorf("lock trust run: %w", err)
	}
	if currentStatus == RunStatusSucceeded || currentStatus == RunStatusFailed {
		return "", true, nil
	}

	if strings.TrimSpace(snapshotRef) == "" {
		err = tx.QueryRow(ctx, `SELECT COALESCE(redis_snapshot_ref, '') FROM trust_runs WHERE id = $1`, runID).Scan(&snapshotRef)
		if err != nil {
			return "", false, fmt.Errorf("load trust run redis snapshot ref: %w", err)
		}
	}

	if _, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET current_phase = $1,
		    phase_started_at = now(),
		    phase_finished_at = NULL,
		    phase_last_error = NULL,
		    updated_at = now()
		WHERE id = $2
	`, RunPhaseCompute, runID); err != nil {
		return "", false, fmt.Errorf("mark trust run compute phase: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("commit trust compute claim tx: %w", err)
	}
	return snapshotRef, false, nil
}

func (r *Runtime) persistComputeResults(
	ctx context.Context,
	runID int64,
	snapshotRef string,
	ranked []rankNode,
	followerEdges int64,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust compute persist tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"SET LOCAL statement_timeout = %d",
		trustEdgeScanStatementTimeout.Milliseconds(),
	)); err != nil {
		return fmt.Errorf("set trust compute statement_timeout: %w", err)
	}

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run for persist: %w", err)
	}
	if currentStatus == RunStatusSucceeded || currentStatus == RunStatusFailed {
		return nil
	}

	if _, err = tx.Exec(ctx, `DELETE FROM trust_scores_global_stage WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("clear previous trust score staging rows: %w", err)
	}

	if len(ranked) > 0 {
		rows := make([][]any, 0, len(ranked))
		for i, item := range ranked {
			rows = append(rows, []any{
				runID,
				item.Pubkey,
				item.Score,
				i + 1,
				derivation.DerivationTrustScoresGlobal,
				derivation.TrustScoresGlobalVersion,
			})
		}
		_, err = tx.CopyFrom(
			ctx,
			pgx.Identifier{"trust_scores_global_stage"},
			[]string{"run_id", "pubkey", "score", "rank", "derivation_name", "target_version"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return fmt.Errorf("write trust score staging rows by copy: %w", err)
		}
	}

	payload, err := json.Marshal(PromoteRunPayload{
		RunID:            runID,
		RedisSnapshotRef: snapshotRef,
	})
	if err != nil {
		return fmt.Errorf("encode trust promote payload: %w", err)
	}
	promoteJobID, err := enqueueTrustJobTx(
		ctx,
		tx,
		jobs.JobTypeTrustPromoteRun,
		payload,
		fmt.Sprintf("%s:run:%d:%s", jobs.JobTypeTrustPromoteRun, runID, snapshotRef),
	)
	if err != nil {
		return fmt.Errorf("enqueue trust promote job: %w", err)
	}

	_, err = tx.Exec(ctx, `
		UPDATE trust_runs
		SET status = $1,
		    input_follower_edges_count = $2,
		    score_rows_published = $3,
		    redis_snapshot_ref = NULLIF($4, ''),
		    phase_finished_at = now(),
		    promote_job_id = $5,
		    job_id = $5,
		    updated_at = now()
		WHERE id = $6
	`, RunStatusRunning, followerEdges, int64(len(ranked)), snapshotRef, promoteJobID, runID)
	if err != nil {
		return fmt.Errorf("mark trust run computed: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust compute persist tx: %w", err)
	}
	return nil
}
