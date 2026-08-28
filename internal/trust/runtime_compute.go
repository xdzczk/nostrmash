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

	teleport, err := r.globalTeleport(ctx, nodeSet)
	if err != nil {
		return err
	}

	var ranked []rankNode
	if r.enableInteractionGraph {
		if _, err := RefreshInteractionEdgeWeights(ctx, r.pool); err != nil {
			return err
		}
		interaction, err := loadInteractionWeights(ctx, r.pool)
		if err != nil {
			return err
		}
		weighted := mergeWeightedAdjacency(adjacencyToWeighted(adjacency), interaction, nodeSet)
		ranked = ComputePersonalizedRankWeighted(weighted, nodeSet, teleport, rankDamping)
	} else {
		ranked = ComputePersonalizedRank(adjacency, nodeSet, teleport, rankDamping)
	}
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

// globalTeleport picks the teleport vector for the global rank: uniform
// PageRank by default, or seed-anchored TrustRank when seed teleport is
// enabled. Seed mass is renormalized over ranked nodes inside the ranking
// core, which falls back to uniform teleport when no active seed is present
// in the graph, so an empty/dormant seed set can never fail the run.
func (r *Runtime) globalTeleport(ctx context.Context, nodeSet map[string]struct{}) (map[string]float64, error) {
	if !r.enableSeedTeleport {
		return uniformTeleport(nodeSet), nil
	}
	seeds, err := loadActiveSeeds(ctx, r.pool)
	if err != nil {
		return nil, err
	}
	teleport := make(map[string]float64, len(seeds))
	for seed := range seeds {
		teleport[seed] = 1
	}
	return teleport, nil
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

	// Prefer batched INSERT over CopyFrom. Production CopyFrom to
	// trust_scores_global_stage repeatedly stalled after ~192KiB with
	// pg_stat_progress_copy.tuples_processed stuck at 0 (even for 2k-row
	// batches), leaving the compute job hung until stale recovery.
	const insertBatch = 500
	for offset := 0; offset < len(ranked); offset += insertBatch {
		end := offset + insertBatch
		if end > len(ranked) {
			end = len(ranked)
		}
		args := make([]any, 0, (end-offset)*6)
		valueSQL := make([]string, 0, end-offset)
		for i := offset; i < end; i++ {
			item := ranked[i]
			base := len(args)
			valueSQL = append(valueSQL, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5, base+6,
			))
			args = append(args,
				runID,
				item.Pubkey,
				item.Score,
				i+1,
				derivation.DerivationTrustScoresGlobal,
				derivation.TrustScoresGlobalVersion,
			)
		}
		sql := `INSERT INTO trust_scores_global_stage (
			run_id, pubkey, score, rank, derivation_name, target_version
		) VALUES ` + strings.Join(valueSQL, ",")
		if _, err = tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("write trust score staging rows (offset %d): %w", offset, err)
		}
	}

	nextJobType := jobs.JobTypeTrustPromoteRun
	var nextPayload []byte
	if r.enableNeighborhoods {
		nextJobType = jobs.JobTypeTrustComputeNeighborhoods
		nextPayload, err = json.Marshal(ComputeNeighborhoodsPayload{
			RunID:            runID,
			RedisSnapshotRef: snapshotRef,
		})
		if err != nil {
			return fmt.Errorf("encode trust neighborhoods payload: %w", err)
		}
	} else {
		nextPayload, err = json.Marshal(PromoteRunPayload{
			RunID:            runID,
			RedisSnapshotRef: snapshotRef,
		})
		if err != nil {
			return fmt.Errorf("encode trust promote payload: %w", err)
		}
	}
	nextJobID, err := enqueueTrustJobTx(
		ctx,
		tx,
		nextJobType,
		nextPayload,
		fmt.Sprintf("%s:run:%d:%s", nextJobType, runID, snapshotRef),
	)
	if err != nil {
		return fmt.Errorf("enqueue trust %s job: %w", nextJobType, err)
	}

	if r.enableNeighborhoods {
		_, err = tx.Exec(ctx, `
			UPDATE trust_runs
			SET status = $1,
			    input_follower_edges_count = $2,
			    score_rows_published = $3,
			    redis_snapshot_ref = NULLIF($4, ''),
			    phase_finished_at = now(),
			    job_id = $5,
			    updated_at = now()
			WHERE id = $6
		`, RunStatusRunning, followerEdges, int64(len(ranked)), snapshotRef, nextJobID, runID)
	} else {
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
		`, RunStatusRunning, followerEdges, int64(len(ranked)), snapshotRef, nextJobID, runID)
	}
	if err != nil {
		return fmt.Errorf("mark trust run computed: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust compute persist tx: %w", err)
	}
	return nil
}
