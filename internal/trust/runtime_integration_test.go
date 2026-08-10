package trust

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/testutil/dbtest"
	"github.com/xdzczk/nostrmash/internal/testutil/derivationbootstrap"
)

func TestRuntime_TriggerGlobalRunAndProcessLifecycleWithoutRedis(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntime(pool, false, true)
	queue := jobs.NewQueue(pool)

	seedFollowerEdge(t, ctx, pool, "evt-a", "alice", "bob")
	seedFollowerEdge(t, ctx, pool, "evt-b", "bob", "alice")

	run, err := runtime.TriggerGlobalRun(ctx)
	if err != nil {
		t.Fatalf("trigger global run: %v", err)
	}
	if run.Status != RunStatusPending {
		t.Fatalf("expected pending run after trigger, got %+v", run)
	}
	if run.SyncJobID == nil || run.JobID == nil || run.CurrentPhase == nil || *run.CurrentPhase != RunPhaseSync {
		t.Fatalf("expected sync job metadata after trigger, got %+v", run)
	}
	if *run.SyncJobID != *run.JobID {
		t.Fatalf("expected sync job id to be current job id, got %+v", run)
	}

	workerID := "trust-worker-test"
	syncJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustSyncGraphRedis)
	if run.SyncJobID == nil || *run.SyncJobID != syncJob.ID {
		t.Fatalf("expected claimed sync job to match run metadata, got job=%d run=%+v", syncJob.ID, run)
	}
	if err := runtime.ProcessJob(ctx, syncJob); err != nil {
		t.Fatalf("process sync job: %v", err)
	}
	if err := queue.CompleteJob(ctx, syncJob.ID, workerID); err != nil {
		t.Fatalf("complete sync job: %v", err)
	}
	assertStoredJobState(t, ctx, queue, syncJob.ID, jobs.StatusSucceeded, "")

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after sync: %v", err)
	}
	if run.Status != RunStatusRunning || run.ComputeJobID == nil || run.JobID == nil || *run.JobID != *run.ComputeJobID {
		t.Fatalf("expected compute job to be attached after sync, got %+v", run)
	}
	if run.StartedAt == nil || run.InputFollowerEdges != 2 {
		t.Fatalf("expected run to start and record input edges, got %+v", run)
	}
	if run.SyncJobID == nil || *run.SyncJobID != syncJob.ID {
		t.Fatalf("expected sync job id to be preserved after sync, got %+v", run)
	}

	computeJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustComputeGlobalScore)
	if *run.ComputeJobID != computeJob.ID || *run.JobID != computeJob.ID {
		t.Fatalf("expected claimed compute job to match run metadata, got job=%d run=%+v", computeJob.ID, run)
	}
	if err := runtime.ProcessJob(ctx, computeJob); err != nil {
		t.Fatalf("process compute job: %v", err)
	}
	if err := queue.CompleteJob(ctx, computeJob.ID, workerID); err != nil {
		t.Fatalf("complete compute job: %v", err)
	}
	assertStoredJobState(t, ctx, queue, computeJob.ID, jobs.StatusSucceeded, "")

	var stagedRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_scores_global_stage WHERE run_id = $1`, run.ID).Scan(&stagedRows); err != nil {
		t.Fatalf("count staged trust scores: %v", err)
	}
	if stagedRows != 2 {
		t.Fatalf("expected two staged trust score rows, got %d", stagedRows)
	}

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after compute: %v", err)
	}
	if run.PromoteJobID == nil || run.ScoreRowsPublished != 2 {
		t.Fatalf("expected promote job and published row count after compute, got %+v", run)
	}

	promoteJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustPromoteRun)
	if *run.PromoteJobID != promoteJob.ID || *run.JobID != promoteJob.ID {
		t.Fatalf("expected claimed promote job to match run metadata, got job=%d run=%+v", promoteJob.ID, run)
	}
	if err := runtime.ProcessJob(ctx, promoteJob); err != nil {
		t.Fatalf("process promote job: %v", err)
	}
	if err := queue.CompleteJob(ctx, promoteJob.ID, workerID); err != nil {
		t.Fatalf("complete promote job: %v", err)
	}
	assertStoredJobState(t, ctx, queue, promoteJob.ID, jobs.StatusSucceeded, "")

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after promote: %v", err)
	}
	if run.Status != RunStatusSucceeded || run.FinishedAt == nil {
		t.Fatalf("expected succeeded run after promote, got %+v", run)
	}

	var publishedRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_scores_global WHERE run_id = $1`, run.ID).Scan(&publishedRows); err != nil {
		t.Fatalf("count published trust scores: %v", err)
	}
	if publishedRows != 2 {
		t.Fatalf("expected two published trust score rows, got %d", publishedRows)
	}

	var latestRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_pubkeys_latest WHERE rank IS NOT NULL`).Scan(&latestRows); err != nil {
		t.Fatalf("count trust_pubkeys_latest rows: %v", err)
	}
	if latestRows != 2 {
		t.Fatalf("expected two trust_pubkeys_latest ranked rows after promote, got %d", latestRows)
	}

	var remainingStageRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_scores_global_stage WHERE run_id = $1`, run.ID).Scan(&remainingStageRows); err != nil {
		t.Fatalf("count staged trust scores after promote: %v", err)
	}
	if remainingStageRows != 0 {
		t.Fatalf("expected stage rows cleared after promote, got %d", remainingStageRows)
	}
}

func TestRuntime_NeighborhoodsPhasePublishesMembersWhenEnabled(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntime(pool, false, true).WithNeighborhoods(true, 100, 2)
	queue := jobs.NewQueue(pool)

	seedFollowerEdge(t, ctx, pool, "evt-a", "alice", "bob")
	seedFollowerEdge(t, ctx, pool, "evt-b", "bob", "carol")
	if _, err := pool.Exec(ctx, `INSERT INTO trust_seeds (pubkey, is_active) VALUES ('alice', true)`); err != nil {
		t.Fatalf("insert trust seed: %v", err)
	}

	run, err := runtime.TriggerGlobalRun(ctx)
	if err != nil {
		t.Fatalf("trigger global run: %v", err)
	}
	workerID := "trust-worker-neighborhoods"

	syncJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustSyncGraphRedis)
	if err := runtime.ProcessJob(ctx, syncJob); err != nil {
		t.Fatalf("process sync job: %v", err)
	}
	if err := queue.CompleteJob(ctx, syncJob.ID, workerID); err != nil {
		t.Fatalf("complete sync job: %v", err)
	}

	computeJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustComputeGlobalScore)
	if err := runtime.ProcessJob(ctx, computeJob); err != nil {
		t.Fatalf("process compute job: %v", err)
	}
	if err := queue.CompleteJob(ctx, computeJob.ID, workerID); err != nil {
		t.Fatalf("complete compute job: %v", err)
	}

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after compute: %v", err)
	}
	if run.PromoteJobID != nil {
		t.Fatalf("expected promote job to wait until neighborhoods finish, got %+v", run)
	}

	neighborhoodJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustComputeNeighborhoods)
	if err := runtime.ProcessJob(ctx, neighborhoodJob); err != nil {
		t.Fatalf("process neighborhoods job: %v", err)
	}
	if err := queue.CompleteJob(ctx, neighborhoodJob.ID, workerID); err != nil {
		t.Fatalf("complete neighborhoods job: %v", err)
	}

	var stagedNeighborhoods int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM trust_neighborhood_members_stage WHERE run_id = $1
	`, run.ID).Scan(&stagedNeighborhoods); err != nil {
		t.Fatalf("count staged neighborhoods: %v", err)
	}
	if stagedNeighborhoods < 3 {
		t.Fatalf("expected staged neighborhood members for alice→bob→carol, got %d", stagedNeighborhoods)
	}

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after neighborhoods: %v", err)
	}
	if run.PromoteJobID == nil {
		t.Fatalf("expected promote job after neighborhoods, got %+v", run)
	}

	promoteJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustPromoteRun)
	if err := runtime.ProcessJob(ctx, promoteJob); err != nil {
		t.Fatalf("process promote job: %v", err)
	}
	if err := queue.CompleteJob(ctx, promoteJob.ID, workerID); err != nil {
		t.Fatalf("complete promote job: %v", err)
	}

	var publishedNeighborhoods int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM trust_neighborhood_members WHERE seed_pubkey = 'alice'
	`).Scan(&publishedNeighborhoods); err != nil {
		t.Fatalf("count published neighborhoods: %v", err)
	}
	if publishedNeighborhoods < 3 {
		t.Fatalf("expected published neighborhood members, got %d", publishedNeighborhoods)
	}

	var remainingStage int
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM trust_neighborhood_members_stage WHERE run_id = $1
	`, run.ID).Scan(&remainingStage); err != nil {
		t.Fatalf("count neighborhood stage after promote: %v", err)
	}
	if remainingStage != 0 {
		t.Fatalf("expected neighborhood stage cleared after promote, got %d", remainingStage)
	}
}

func TestRuntime_ComputePhaseFailureAfterSuccessfulSyncMarksRunFailedWithoutPublishing(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	queue := jobs.NewQueue(pool)
	runtime := NewRuntime(pool, false, true)

	seedFollowerEdge(t, ctx, pool, "evt-a", "alice", "bob")

	run, err := runtime.TriggerGlobalRun(ctx)
	if err != nil {
		t.Fatalf("trigger global run: %v", err)
	}

	workerID := "trust-worker-test"
	syncJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustSyncGraphRedis)
	if err := runtime.ProcessJob(ctx, syncJob); err != nil {
		t.Fatalf("process sync job: %v", err)
	}
	if err := queue.CompleteJob(ctx, syncJob.ID, workerID); err != nil {
		t.Fatalf("complete sync job: %v", err)
	}
	assertStoredJobState(t, ctx, queue, syncJob.ID, jobs.StatusSucceeded, "")

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after sync: %v", err)
	}
	computeJob := claimSingleTrustJob(t, ctx, queue, workerID, jobs.JobTypeTrustComputeGlobalScore)
	if *run.ComputeJobID != computeJob.ID {
		t.Fatalf("expected compute job to match run metadata, got job=%d run=%+v", computeJob.ID, run)
	}

	// Force the compute-phase staging INSERT to fail. A CHECK constraint is
	// more reliable than DROP TABLE here because test pools use search_path
	// "<schema>,public"; if public still has the relation, DROP only removes
	// the schema-local copy and compute can keep writing.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE trust_scores_global_stage
		ADD CONSTRAINT trust_stage_force_fail CHECK (score < 0)
	`); err != nil {
		t.Fatalf("add failing check constraint to force compute failure: %v", err)
	}

	err = runtime.ProcessJob(ctx, computeJob)
	if err == nil || !strings.Contains(err.Error(), "write trust score staging rows") {
		t.Fatalf("expected compute-phase query failure, got %v", err)
	}
	failResult, failErr := queue.FailJob(ctx, computeJob.ID, workerID, err.Error(), time.Minute)
	if failErr != nil {
		t.Fatalf("fail compute job: %v", failErr)
	}
	if failResult.Status != jobs.StatusPending {
		t.Fatalf("expected failed compute job to be scheduled for retry, got %+v", failResult)
	}
	assertStoredJobStateContainsError(t, ctx, queue, computeJob.ID, jobs.StatusPending, "write trust score staging rows")

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after compute failure: %v", err)
	}
	if run.Status != RunStatusFailed || run.LastError == nil || run.PhaseLastError == nil {
		t.Fatalf("expected failed run after compute error, got %+v", run)
	}
	if run.CurrentPhase == nil || *run.CurrentPhase != RunPhaseCompute {
		t.Fatalf("expected compute phase to be recorded on failure, got %+v", run.CurrentPhase)
	}
	if !strings.Contains(*run.LastError, "write trust score staging rows") || !strings.Contains(*run.PhaseLastError, "write trust score staging rows") {
		t.Fatalf("expected compute failure details to be stored, got %+v", run)
	}

	var stagedRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_scores_global_stage WHERE run_id = $1`, run.ID).Scan(&stagedRows); err != nil {
		t.Fatalf("count staged trust scores after compute failure: %v", err)
	}
	if stagedRows != 0 {
		t.Fatalf("expected no staged trust scores after compute failure, got %d", stagedRows)
	}

	var publishedRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM trust_scores_global WHERE run_id = $1`, run.ID).Scan(&publishedRows); err != nil {
		t.Fatalf("count published trust scores after compute failure: %v", err)
	}
	if publishedRows != 0 {
		t.Fatalf("expected no published trust scores after compute failure, got %d", publishedRows)
	}

	var queuedPromoteJobs int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE job_type = $1`, jobs.JobTypeTrustPromoteRun).Scan(&queuedPromoteJobs); err != nil {
		t.Fatalf("count queued promote jobs after compute failure: %v", err)
	}
	if queuedPromoteJobs != 0 {
		t.Fatalf("expected no promote jobs after compute failure, got %d", queuedPromoteJobs)
	}
}

func TestRuntime_ProcessJobRejectsInvalidPayloadsAndUnsupportedTypes(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntime(pool, false, true)

	testCases := []struct {
		name    string
		job     jobs.Job
		wantErr string
	}{
		{
			name:    "sync malformed payload",
			job:     jobs.Job{JobType: jobs.JobTypeTrustSyncGraphRedis, Payload: json.RawMessage(`{`)},
			wantErr: "decode trust redis sync payload",
		},
		{
			name:    "sync missing run id",
			job:     jobs.Job{JobType: jobs.JobTypeTrustSyncGraphRedis, Payload: json.RawMessage(`{"run_id":0}`)},
			wantErr: "run_id is required in redis sync payload",
		},
		{
			name:    "compute malformed payload",
			job:     jobs.Job{JobType: jobs.JobTypeTrustComputeGlobalScore, Payload: json.RawMessage(`{`)},
			wantErr: "decode trust global score payload",
		},
		{
			name:    "promote missing run id",
			job:     jobs.Job{JobType: jobs.JobTypeTrustPromoteRun, Payload: json.RawMessage(`{"run_id":0}`)},
			wantErr: "run_id is required in promote payload",
		},
		{
			name:    "neighborhoods disabled",
			job:     jobs.Job{JobType: jobs.JobTypeTrustComputeNeighborhoods, Payload: json.RawMessage(`{"run_id":1}`)},
			wantErr: "trust neighborhood compute is disabled",
		},
		{
			name:    "unsupported job type",
			job:     jobs.Job{JobType: "trust_unknown", Payload: json.RawMessage(`{}`)},
			wantErr: `trust job type "trust_unknown" not implemented`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := runtime.ProcessJob(ctx, tc.job)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestRuntime_ProcessJobMarksRunFailedWhenSyncFails(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntimeWithRedis(pool, nil, "nostrmash", true, true)

	run, err := runtime.TriggerGlobalRun(ctx)
	if err != nil {
		t.Fatalf("trigger global run: %v", err)
	}

	err = runtime.ProcessJob(ctx, jobs.Job{
		JobType: jobs.JobTypeTrustSyncGraphRedis,
		Payload: json.RawMessage(`{"run_id":` + int64ToJSON(run.ID) + `}`),
	})
	if err == nil || !strings.Contains(err.Error(), "redis is not configured") {
		t.Fatalf("expected redis configuration error, got %v", err)
	}

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get failed run: %v", err)
	}
	if run.Status != RunStatusFailed || run.LastError == nil || run.PhaseLastError == nil {
		t.Fatalf("expected failed run with error details, got %+v", run)
	}
	if !strings.Contains(*run.LastError, "redis is not configured") || !strings.Contains(*run.PhaseLastError, "redis is not configured") {
		t.Fatalf("expected sync failure details to be stored, got last=%v phase=%v", run.LastError, run.PhaseLastError)
	}
	if run.CurrentPhase == nil || *run.CurrentPhase != RunPhaseSync {
		t.Fatalf("expected failed run to record sync phase, got %+v", run.CurrentPhase)
	}
}

func TestRuntime_ProcessJobSkipsTerminalRunsWithoutEnqueuingMoreWork(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntime(pool, false, true)

	runID := insertTrustRunForRuntimeTest(t, ctx, pool, RunStatusSucceeded)
	payload := json.RawMessage(`{"run_id":` + int64ToJSON(runID) + `}`)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- runtime.ProcessJob(ctx, jobs.Job{
				JobType: jobs.JobTypeTrustSyncGraphRedis,
				Payload: payload,
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("expected terminal run processing to skip cleanly, got %v", err)
		}
	}

	var jobCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs`).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs after terminal skip: %v", err)
	}
	if jobCount != 0 {
		t.Fatalf("expected terminal run skip to avoid enqueueing new jobs, got %d jobs", jobCount)
	}
}

func TestRuntime_ProcessJobConcurrentDuplicateSyncDeliveryEnqueuesAtMostOneComputeJob(t *testing.T) {
	ctx := context.Background()
	pool := setupTrustRuntimePool(t, ctx)
	runtime := NewRuntime(pool, false, true)
	queue := jobs.NewQueue(pool)

	seedFollowerEdge(t, ctx, pool, "evt-a", "alice", "bob")
	run, err := runtime.TriggerGlobalRun(ctx)
	if err != nil {
		t.Fatalf("trigger global run: %v", err)
	}

	syncJob := claimSingleTrustJob(t, ctx, queue, "trust-worker-test", jobs.JobTypeTrustSyncGraphRedis)
	start := make(chan struct{})
	errs := make(chan error, 2)

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- runtime.ProcessJob(ctx, syncJob)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	errorCount := 0
	for err := range errs {
		if err != nil {
			errorCount++
		}
	}
	if errorCount > 1 {
		t.Fatalf("expected at most one duplicate-delivery error, got %d", errorCount)
	}

	var computeJobCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE job_type = $1`, jobs.JobTypeTrustComputeGlobalScore).Scan(&computeJobCount); err != nil {
		t.Fatalf("count compute jobs after duplicate sync delivery: %v", err)
	}
	if computeJobCount > 1 {
		t.Fatalf("expected at most one compute job after duplicate sync delivery, got %d", computeJobCount)
	}

	var promoteJobCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM jobs WHERE job_type = $1`, jobs.JobTypeTrustPromoteRun).Scan(&promoteJobCount); err != nil {
		t.Fatalf("count promote jobs after duplicate sync delivery: %v", err)
	}
	if promoteJobCount != 0 {
		t.Fatalf("expected no promote jobs before compute phase, got %d", promoteJobCount)
	}

	run, err = runtime.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run after duplicate sync delivery: %v", err)
	}
	if run.Status == RunStatusSucceeded {
		t.Fatalf("expected duplicate sync delivery to stop short of a completed run, got %+v", run)
	}
}

func setupTrustRuntimePool(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	dbURL := dbtest.DatabaseURL(t, "trust runtime")
	pool := dbtest.SetupSchemaPool(t, ctx, dbURL, "trust_runtime")
	derivationbootstrap.MustMigrate(t, ctx, pool, "test-v1")
	return pool
}

func claimSingleTrustJob(t *testing.T, ctx context.Context, queue *jobs.Queue, workerID, wantJobType string) jobs.Job {
	t.Helper()
	claimed, err := queue.ClaimAvailableForPool(ctx, workerID, jobs.WorkerPoolTrust, 1)
	if err != nil {
		t.Fatalf("claim trust job: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed trust job, got %d", len(claimed))
	}
	if claimed[0].JobType != wantJobType {
		t.Fatalf("expected job type %q, got %+v", wantJobType, claimed[0])
	}
	return claimed[0]
}

func seedFollowerEdge(t *testing.T, ctx context.Context, pool *pgxpool.Pool, eventID, followerPubkey, followedPubkey string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := pool.Exec(ctx, `
		INSERT INTO events (
			id, pubkey, created_at, kind, sig, content, raw_json, first_seen_at, inserted_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
	`, eventID, followerPubkey, now.Unix(), 3, "sig-"+eventID, "", []byte(`{}`), now); err != nil {
		t.Fatalf("insert source event %s: %v", eventID, err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO follower_edges (
			followed_pubkey, follower_pubkey, source_event_id, contact_list_created_at, derivation_version
		)
		VALUES ($1, $2, $3, $4, $5)
	`, followedPubkey, followerPubkey, eventID, now.Unix(), derivation.TrustScoresGlobalVersion); err != nil {
		t.Fatalf("insert follower edge %s->%s: %v", followerPubkey, followedPubkey, err)
	}
}

func insertTrustRunForRuntimeTest(t *testing.T, ctx context.Context, pool *pgxpool.Pool, status string) int64 {
	t.Helper()
	var runID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO trust_runs (derivation_name, target_version, status)
		VALUES ($1, $2, $3)
		RETURNING id
	`, derivation.DerivationTrustScoresGlobal, derivation.TrustScoresGlobalVersion, status).Scan(&runID); err != nil {
		t.Fatalf("insert trust run: %v", err)
	}
	return runID
}

func int64ToJSON(v int64) string {
	return strconv.FormatInt(v, 10)
}

func assertStoredJobState(t *testing.T, ctx context.Context, queue *jobs.Queue, jobID int64, wantStatus string, wantLastError string) {
	t.Helper()
	stored, err := queue.GetJobByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get job %d: %v", jobID, err)
	}
	if stored.Status != wantStatus {
		t.Fatalf("expected job %d status %q, got %+v", jobID, wantStatus, stored)
	}
	if stored.LockedBy != nil || stored.LockedAt != nil {
		t.Fatalf("expected job %d locks to be cleared, got %+v", jobID, stored)
	}
	gotLastError := ""
	if stored.LastError != nil {
		gotLastError = *stored.LastError
	}
	if gotLastError != wantLastError {
		t.Fatalf("expected job %d last_error %q, got %q", jobID, wantLastError, gotLastError)
	}
}

func assertStoredJobStateContainsError(t *testing.T, ctx context.Context, queue *jobs.Queue, jobID int64, wantStatus string, wantLastErrorSubstring string) {
	t.Helper()
	stored, err := queue.GetJobByID(ctx, jobID)
	if err != nil {
		t.Fatalf("get job %d: %v", jobID, err)
	}
	if stored.Status != wantStatus {
		t.Fatalf("expected job %d status %q, got %+v", jobID, wantStatus, stored)
	}
	if stored.LockedBy != nil || stored.LockedAt != nil {
		t.Fatalf("expected job %d locks to be cleared, got %+v", jobID, stored)
	}
	if stored.LastError == nil || !strings.Contains(*stored.LastError, wantLastErrorSubstring) {
		t.Fatalf("expected job %d last_error containing %q, got %+v", jobID, wantLastErrorSubstring, stored.LastError)
	}
}
