package replay

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/ingestor/live"
	"github.com/xdzczk/nostrmash/internal/jobs"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
)

// Result is the deterministic replay outcome.
type Result struct {
	EntriesReplayed int
	JobsProcessed   int
	IngestCounters  live.Counters
}

// Runner replays payload fixtures through canonical ingest then drains jobs in deterministic order.
type Runner struct {
	log       *slog.Logger
	processor *live.Processor
	queue     *jobs.Queue
	handlers  *derivation.Handlers
	workerID  string
}

func NewRunner(log *slog.Logger, pool *pgxpool.Pool, validateOpts nostr.Options) (*Runner, error) {
	if pool == nil {
		return nil, fmt.Errorf("pool is required")
	}
	if log == nil {
		log = slog.Default()
	}
	eventStore := store.NewPostgresStore(pool)
	processor, err := live.NewProcessor(log, eventStore, validateOpts)
	if err != nil {
		return nil, fmt.Errorf("new live processor: %w", err)
	}
	return &Runner{
		log:       log,
		processor: processor,
		queue:     jobs.NewQueue(pool),
		handlers:  derivation.NewHandlers(pool),
		workerID:  "replay-runner",
	}, nil
}

func (r *Runner) ReplayFixturePath(ctx context.Context, fixturePath string) (Result, error) {
	fixture, err := LoadFixture(fixturePath)
	if err != nil {
		return Result{}, err
	}
	return r.Replay(ctx, fixture)
}

func (r *Runner) Replay(ctx context.Context, fixture Fixture) (Result, error) {
	if r == nil || r.processor == nil || r.queue == nil || r.handlers == nil {
		return Result{}, fmt.Errorf("replay runner is not initialized")
	}
	if len(fixture.Entries) == 0 {
		return Result{}, fmt.Errorf("fixture has no entries")
	}

	for idx, entry := range fixture.Entries {
		if err := r.processor.Handle(ctx, entry.RelayURL, entry.Payload); err != nil {
			return Result{}, fmt.Errorf("replay entry %d: %w", idx, err)
		}
	}

	jobsProcessed, err := r.DrainJobs(ctx)
	if err != nil {
		return Result{}, err
	}

	snapshot := r.processor.Snapshot()
	r.log.Info(
		"replay_completed",
		"entries_replayed", len(fixture.Entries),
		"jobs_processed", jobsProcessed,
		"valid_total", snapshot.Valid,
		"duplicate_total", snapshot.Duplicate,
		"invalid_total", snapshot.Invalid,
	)
	return Result{
		EntriesReplayed: len(fixture.Entries),
		JobsProcessed:   jobsProcessed,
		IngestCounters:  snapshot,
	}, nil
}

// DrainJobs executes all available jobs in queue order with a single deterministic worker.
func (r *Runner) DrainJobs(ctx context.Context) (int, error) {
	processed := 0
	for {
		if ctx.Err() != nil {
			return processed, ctx.Err()
		}
		claimed, err := r.queue.ClaimAvailable(ctx, r.workerID, 1)
		if err != nil {
			return processed, fmt.Errorf("claim replay jobs: %w", err)
		}
		if len(claimed) == 0 {
			return processed, nil
		}

		job := claimed[0]
		err = derivation.ProcessJob(ctx, r.handlers, derivation.Job{
			JobType: job.JobType,
			Payload: job.Payload,
		})
		if err != nil {
			_, failErr := r.queue.FailJob(ctx, job.ID, r.workerID, err.Error(), 0*time.Second)
			if failErr != nil {
				return processed, fmt.Errorf("mark replay job failed id=%d: %w", job.ID, failErr)
			}
			return processed, fmt.Errorf("process replay job id=%d type=%s: %w", job.ID, strings.TrimSpace(job.JobType), err)
		}
		if err := r.queue.CompleteJob(ctx, job.ID, r.workerID); err != nil {
			return processed, fmt.Errorf("complete replay job id=%d: %w", job.ID, err)
		}
		processed++
	}
}
