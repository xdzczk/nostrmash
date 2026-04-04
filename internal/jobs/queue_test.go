package jobs

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestClaimAvailableConcurrentWorkersNoDoubleClaim(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	queue := NewQueue(pool)
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		_, err := queue.Enqueue(ctx, EnqueueParams{
			JobType:     "derive_profile",
			Payload:     []byte(`{"event_id":"abc"}`),
			RunAfter:    now,
			MaxAttempts: 3,
		})
		if err != nil {
			t.Fatalf("enqueue seed job %d: %v", i, err)
		}
	}

	type claimResult struct {
		jobs []Job
		err  error
	}
	results := make(chan claimResult, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup

	claim := func(workerID string) {
		defer wg.Done()
		<-start
		jobs, err := queue.ClaimAvailable(ctx, workerID, 2)
		results <- claimResult{jobs: jobs, err: err}
	}

	wg.Add(2)
	go claim("worker-a")
	go claim("worker-b")
	close(start)
	wg.Wait()
	close(results)

	claimedByID := make(map[int64]string)
	total := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("claim jobs: %v", result.err)
		}
		for _, job := range result.jobs {
			total++
			if prior, exists := claimedByID[job.ID]; exists {
				t.Fatalf("job %d double-claimed by %q and %q", job.ID, prior, derefString(job.LockedBy))
			}
			claimedByID[job.ID] = derefString(job.LockedBy)
		}
	}

	if total != 3 {
		t.Fatalf("expected 3 total claimed jobs, got %d", total)
	}

	remaining, err := queue.ClaimAvailable(ctx, "worker-c", 5)
	if err != nil {
		t.Fatalf("claim remaining jobs: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected 0 remaining claimable jobs, got %d", len(remaining))
	}
}

func TestFailJobRetriesThenDeadLettersAtMaxAttempts(t *testing.T) {
	ctx := context.Background()
	pool := setupSchemaPool(t, ctx, testDatabaseURL(t))
	if err := store.Migrate(ctx, pool, "test-v1"); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	queue := NewQueue(pool)
	job, err := queue.Enqueue(ctx, EnqueueParams{
		JobType:     "derive_profile",
		Payload:     []byte(`{"event_id":"abc"}`),
		MaxAttempts: 2,
		RunAfter:    time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("enqueue job: %v", err)
	}

	claimed, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("initial claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected one claimed job, got %d", len(claimed))
	}

	firstFailure, err := queue.FailJob(ctx, job.ID, "worker-a", "temporary failure", 20*time.Millisecond)
	if err != nil {
		t.Fatalf("first failure mark: %v", err)
	}
	if firstFailure.Status != StatusPending || firstFailure.Attempts != 1 {
		t.Fatalf("unexpected first failure result: %+v", firstFailure)
	}

	immediate, err := queue.ClaimAvailable(ctx, "worker-b", 1)
	if err != nil {
		t.Fatalf("immediate claim after retry schedule: %v", err)
	}
	if len(immediate) != 0 {
		t.Fatalf("expected no immediate retry claim, got %d jobs", len(immediate))
	}

	time.Sleep(30 * time.Millisecond)

	secondClaim, err := queue.ClaimAvailable(ctx, "worker-a", 1)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(secondClaim) != 1 {
		t.Fatalf("expected retried job to become claimable, got %d jobs", len(secondClaim))
	}

	secondFailure, err := queue.FailJob(ctx, job.ID, "worker-a", "permanent failure", 0)
	if err != nil {
		t.Fatalf("second failure mark: %v", err)
	}
	if secondFailure.Status != StatusDead {
		t.Fatalf("expected dead-letter status %q, got %q", StatusDead, secondFailure.Status)
	}
	if secondFailure.Attempts != 2 {
		t.Fatalf("expected attempts=2 after dead-letter, got %d", secondFailure.Attempts)
	}

	reclaimed, err := queue.ClaimAvailable(ctx, "worker-c", 1)
	if err != nil {
		t.Fatalf("claim after dead-letter: %v", err)
	}
	if len(reclaimed) != 0 {
		t.Fatalf("expected dead-lettered job to be unclaimable, got %d jobs", len(reclaimed))
	}

	stored, err := queue.GetJobByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("load dead-lettered job: %v", err)
	}
	if stored.Status != StatusDead {
		t.Fatalf("expected stored status %q, got %q", StatusDead, stored.Status)
	}
	if stored.LastError == nil || *stored.LastError != "permanent failure" {
		t.Fatalf("expected last_error to persist permanent failure, got %v", stored.LastError)
	}
}

func setupSchemaPool(t *testing.T, ctx context.Context, dbURL string) *pgxpool.Pool {
	t.Helper()

	adminPool, err := store.OpenPool(ctx, dbURL)
	if err != nil {
		t.Fatalf("open admin pool: %v", err)
	}

	schemaName := fmt.Sprintf("test_jobs_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schemaName}.Sanitize()
	if _, err := adminPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, quotedSchema)); err != nil {
		adminPool.Close()
		t.Fatalf("create schema %s: %v", schemaName, err)
	}

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		adminPool.Close()
		t.Fatalf("parse pool config: %v", err)
	}
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schemaName

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		adminPool.Close()
		t.Fatalf("open schema pool: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, quotedSchema))
		adminPool.Close()
	})

	return pool
}

func testDatabaseURL(t *testing.T) string {
	t.Helper()

	candidates := []string{
		os.Getenv("TEST_DATABASE_URL"),
		os.Getenv("DATABASE_URL"),
	}
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate) == "" {
			continue
		}
		return candidate
	}

	t.Skip("set TEST_DATABASE_URL or DATABASE_URL to run jobs integration tests")
	return ""
}

func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
