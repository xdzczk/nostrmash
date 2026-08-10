package trust

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func (r *Runtime) executeNeighborhoodsRun(ctx context.Context, runID int64, snapshotRef string) (err error) {
	started := time.Now()
	if !r.enableNeighborhoods {
		return fmt.Errorf("trust neighborhood compute is disabled")
	}

	snapshotRef, skipped, err := r.claimNeighborhoodsPhase(ctx, runID, snapshotRef)
	if err != nil {
		return err
	}
	if skipped {
		metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeNeighborhoods, "skipped", time.Since(started))
		metrics.ObserveTrustPhaseDuration(RunPhaseNeighborhoods, "skipped", time.Since(started))
		return nil
	}

	var adjacency map[string][]string
	if r.enableRedisSync && r.redis != nil && strings.TrimSpace(snapshotRef) != "" && snapshotRef != "postgres-only" {
		adjacency, _, err = r.loadAdjacencyFromRedis(ctx, runID, snapshotRef)
		if err != nil {
			return err
		}
	} else {
		adjacency, _, err = r.loadAdjacencyFromPostgres(ctx)
		if err != nil {
			return err
		}
	}

	seeds, err := loadActiveSeeds(ctx, r.pool)
	if err != nil {
		return err
	}
	members := computeSeedNeighborhoods(
		adjacency,
		seeds,
		r.neighborhoodMaxHops,
		r.neighborhoodMaxMembers,
	)

	if err := r.writeNeighborhoodsToRedis(ctx, runID, snapshotRef, members); err != nil {
		return err
	}
	if err := r.persistNeighborhoodResults(ctx, runID, snapshotRef, members); err != nil {
		return err
	}

	countsBySeed := make(map[string]float64, len(seeds))
	for seed := range seeds {
		countsBySeed[seed] = 0
	}
	for _, member := range members {
		countsBySeed[member.SeedPubkey]++
	}
	for seed, count := range countsBySeed {
		metrics.SetTrustNeighborhoodMembers(seed, count)
	}

	metrics.ObserveWorkerJobExecution(jobs.JobTypeTrustComputeNeighborhoods, "success", time.Since(started))
	metrics.ObserveTrustPhaseDuration(RunPhaseNeighborhoods, "success", time.Since(started))
	return nil
}

func (r *Runtime) claimNeighborhoodsPhase(ctx context.Context, runID int64, snapshotRef string) (string, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin trust neighborhoods claim tx: %w", err)
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
	`, RunPhaseNeighborhoods, runID); err != nil {
		return "", false, fmt.Errorf("mark trust run neighborhoods phase: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("commit trust neighborhoods claim tx: %w", err)
	}
	return snapshotRef, false, nil
}

func (r *Runtime) persistNeighborhoodResults(
	ctx context.Context,
	runID int64,
	snapshotRef string,
	members []neighborhoodMember,
) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin trust neighborhoods persist tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(
		"SET LOCAL statement_timeout = %d",
		trustEdgeScanStatementTimeout.Milliseconds(),
	)); err != nil {
		return fmt.Errorf("set trust neighborhoods statement_timeout: %w", err)
	}

	var currentStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM trust_runs WHERE id = $1 FOR UPDATE`, runID).Scan(&currentStatus)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("trust run %d not found", runID)
		}
		return fmt.Errorf("lock trust run for neighborhoods persist: %w", err)
	}
	if currentStatus == RunStatusSucceeded || currentStatus == RunStatusFailed {
		return nil
	}

	if _, err = tx.Exec(ctx, `DELETE FROM trust_neighborhood_members_stage WHERE run_id = $1`, runID); err != nil {
		return fmt.Errorf("clear previous trust neighborhood staging rows: %w", err)
	}

	const insertBatch = 500
	for offset := 0; offset < len(members); offset += insertBatch {
		end := offset + insertBatch
		if end > len(members) {
			end = len(members)
		}
		args := make([]any, 0, (end-offset)*5)
		valueSQL := make([]string, 0, end-offset)
		for i := offset; i < end; i++ {
			item := members[i]
			base := len(args)
			valueSQL = append(valueSQL, fmt.Sprintf(
				"($%d,$%d,$%d,$%d,$%d)",
				base+1, base+2, base+3, base+4, base+5,
			))
			args = append(args, runID, item.SeedPubkey, item.MemberPubkey, item.Hops, item.Weight)
		}
		sql := `INSERT INTO trust_neighborhood_members_stage (
			run_id, seed_pubkey, member_pubkey, hops, weight
		) VALUES ` + strings.Join(valueSQL, ",")
		if _, err = tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("write trust neighborhood staging rows (offset %d): %w", offset, err)
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
		    redis_snapshot_ref = NULLIF($2, ''),
		    phase_finished_at = now(),
		    promote_job_id = $3,
		    job_id = $3,
		    updated_at = now()
		WHERE id = $4
	`, RunStatusRunning, snapshotRef, promoteJobID, runID)
	if err != nil {
		return fmt.Errorf("mark trust run neighborhoods computed: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit trust neighborhoods persist tx: %w", err)
	}
	return nil
}

func (r *Runtime) writeNeighborhoodsToRedis(
	ctx context.Context,
	runID int64,
	snapshotRef string,
	members []neighborhoodMember,
) error {
	if r.redis == nil || !r.enableRedisSync {
		return nil
	}
	snapshotRef = strings.TrimSpace(snapshotRef)
	if snapshotRef == "" || snapshotRef == "postgres-only" {
		return nil
	}
	if len(members) == 0 {
		return nil
	}

	keys := newRedisKeyspace(r.redisKeyPrefix)
	bySeed := make(map[string][]string)
	for _, member := range members {
		bySeed[member.SeedPubkey] = append(bySeed[member.SeedPubkey], member.MemberPubkey)
	}

	pipe := r.redis.TxPipeline()
	for seed, pubkeys := range bySeed {
		key := keys.runNeighborhoodKey(runID, snapshotRef, seed)
		args := make([]any, 0, len(pubkeys))
		for _, pubkey := range pubkeys {
			args = append(args, pubkey)
		}
		pipe.SAdd(ctx, key, args...)
		pipe.Expire(ctx, key, redisRunKeyTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("write trust neighborhood redis keys: %w", err)
	}
	return nil
}

func (r *Runtime) publishNeighborhoodMembersTx(ctx context.Context, tx pgx.Tx, runID int64) error {
	if _, err := tx.Exec(ctx, `DELETE FROM trust_neighborhood_members`); err != nil {
		return fmt.Errorf("clear previous trust neighborhood members: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO trust_neighborhood_members (
			seed_pubkey,
			member_pubkey,
			hops,
			weight,
			source_run_id,
			computed_at
		)
		SELECT
			seed_pubkey,
			member_pubkey,
			hops,
			weight,
			run_id,
			now()
		FROM trust_neighborhood_members_stage
		WHERE run_id = $1
	`, runID); err != nil {
		return fmt.Errorf("publish staged trust neighborhood members: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM trust_neighborhood_members_stage
		WHERE run_id = $1
	`, runID); err != nil {
		return fmt.Errorf("clear trust neighborhood staging rows after promote: %w", err)
	}
	return nil
}
