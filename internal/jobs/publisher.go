package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

// CanonicalEventPublisher enqueues downstream jobs when a new canonical event is inserted.
type CanonicalEventPublisher interface {
	PublishCanonicalEventJobsTx(ctx context.Context, tx pgx.Tx, eventID string) error
}

type QueuePublisher struct {
	maxAttempts int
}

func NewQueuePublisher(maxAttempts int) QueuePublisher {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	return QueuePublisher{maxAttempts: maxAttempts}
}

func (p QueuePublisher) PublishCanonicalEventJobsTx(ctx context.Context, tx pgx.Tx, eventID string) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	// derive_event_bundle now invokes update_thread_projection and
	// repair_unresolved_references inline (see derivation.Handlers.DeriveEventBundle),
	// so a single composite job covers all per-event derivation work and keeps
	// the queue at one row per ingested event.
	return EnqueueEventJobTx(ctx, tx, JobTypeDeriveEventBundle, eventID, "", p.maxAttempts)
}

func EnqueueEventJobTx(
	ctx context.Context,
	tx pgx.Tx,
	jobType string,
	eventID string,
	idempotencySuffix string,
	maxAttempts int,
) (err error) {
	started := time.Now()
	defer func() {
		metrics.ObserveQueueOperation("enqueue_event_job_tx", queueResultFromErr(err), time.Since(started))
	}()
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	payload, err := json.Marshal(EventJobPayload{EventID: eventID})
	if err != nil {
		return fmt.Errorf("encode %s payload for event %s: %w", jobType, eventID, err)
	}
	idempotencyKey := fmt.Sprintf("%s:%s", jobType, eventID)
	if trimmed := strings.TrimSpace(idempotencySuffix); trimmed != "" {
		idempotencyKey = fmt.Sprintf("%s:%s", idempotencyKey, trimmed)
	}

	workerPool := WorkerPoolForJobType(jobType)
	if override, ok := WorkerPoolFromContext(ctx); ok && WorkerPoolForJobType(jobType) != WorkerPoolTrust {
		// Trust jobs are pinned to their dedicated pool regardless of caller context.
		workerPool = override
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO jobs (job_type, worker_pool, payload, idempotency_key, max_attempts, run_after)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
		DO NOTHING
	`,
		jobType,
		workerPool,
		json.RawMessage(payload),
		idempotencyKey,
		maxAttempts,
	)
	if err != nil {
		return fmt.Errorf("enqueue %s for event %s: %w", jobType, eventID, err)
	}
	return nil
}
