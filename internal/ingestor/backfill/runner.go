package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

// CheckpointStore persists per-relay backfill progress.
type CheckpointStore interface {
	GetIngestCheckpoint(
		ctx context.Context,
		relayURL string,
		mode string,
		filterGroup string,
	) (*model.IngestCheckpoint, error)
	UpsertIngestCheckpoint(ctx context.Context, checkpoint model.IngestCheckpoint) error
}

// MessageHandler persists one event payload from a relay.
type MessageHandler func(ctx context.Context, relayURL string, payload []byte) error

// PageRequest defines one relay history request.
type PageRequest struct {
	Kinds   []int
	Authors []string
	Since   *int64
	Until   *int64
	Limit   int
}

// PageResult is one relay history response.
type PageResult struct {
	Events   [][]byte
	EOSESeen bool
}

// PageFetcher fetches one page for a relay.
type PageFetcher interface {
	FetchPage(ctx context.Context, relayURL string, request PageRequest) (PageResult, error)
}

// Config defines backfill runner behavior.
type Config struct {
	Relays      []string
	FilterGroup string
	Kinds       []int

	Mode string

	Since *int64
	Until *int64

	PageLimit int

	// When relay does not send EOSE, complete after this many empty pages.
	EmptyPageMax int
}

// Runner executes resumable relay backfill and persists checkpoints.
type Runner struct {
	log       *slog.Logger
	cfg       Config
	store     CheckpointStore
	fetcher   PageFetcher
	onMessage MessageHandler
	nowFn     func() time.Time
}

func NewRunner(
	log *slog.Logger,
	cfg Config,
	store CheckpointStore,
	fetcher PageFetcher,
	onMessage MessageHandler,
) (*Runner, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	if fetcher == nil {
		return nil, fmt.Errorf("page fetcher is required")
	}
	if onMessage == nil {
		return nil, fmt.Errorf("message handler is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if strings.TrimSpace(cfg.Mode) == "" {
		cfg.Mode = model.ModeBackfill
	}
	if cfg.Mode != model.ModeBackfill {
		return nil, fmt.Errorf("mode %q is not implemented", cfg.Mode)
	}
	if cfg.PageLimit <= 0 {
		return nil, fmt.Errorf("page limit must be > 0")
	}
	if cfg.EmptyPageMax <= 0 {
		return nil, fmt.Errorf("empty page max must be > 0")
	}
	if strings.TrimSpace(cfg.FilterGroup) == "" {
		return nil, fmt.Errorf("filter group is required")
	}
	if len(cfg.Kinds) == 0 {
		return nil, fmt.Errorf("kinds are required")
	}
	if cfg.Since != nil && cfg.Until != nil && *cfg.Since > *cfg.Until {
		return nil, fmt.Errorf("since must be <= until")
	}

	return &Runner{
		log:       log,
		cfg:       cfg,
		store:     store,
		fetcher:   fetcher,
		onMessage: onMessage,
		nowFn:     time.Now,
	}, nil
}

// Run blocks until all configured relays complete or one fails.
func (r *Runner) Run(ctx context.Context) (err error) {
	ctx, span := traceutil.StartSpan(ctx, "ingest.backfill.run")
	defer func() { span.End(err) }()
	for _, relayURL := range r.cfg.Relays {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.runRelay(ctx, relayURL); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) runRelay(ctx context.Context, relayURL string) (err error) {
	ctx, span := traceutil.StartSpan(ctx, "ingest.backfill.run_relay", traceutil.KV("relay.url", relayURL))
	defer func() { span.End(err) }()
	checkpoint, err := r.ensureRunningCheckpoint(ctx, relayURL)
	if err != nil {
		return err
	}
	if checkpoint.Status == model.CheckpointCompleted {
		r.log.Info("backfill_relay_already_complete", "relay_url", relayURL)
		return nil
	}

	cursor := parseCursorInt64(checkpoint.Cursor)
	emptyPages := 0

	for {
		requestSince := r.nextSince(cursor)
		if r.cfg.Until != nil && requestSince != nil && *requestSince > *r.cfg.Until {
			return r.markCompleted(ctx, checkpoint, cursor)
		}

		fetchCtx, fetchSpan := traceutil.StartSpan(ctx, "ingest.backfill.fetch_page", traceutil.KV("relay.url", relayURL))
		page, err := r.fetcher.FetchPage(fetchCtx, relayURL, PageRequest{
			Kinds: r.cfg.Kinds,
			Since: requestSince,
			Until: r.cfg.Until,
			Limit: r.cfg.PageLimit,
		})
		fetchSpan.End(err)
		if err != nil {
			_ = r.markFailed(ctx, checkpoint, cursor)
			return fmt.Errorf("fetch page relay %s: %w", relayURL, err)
		}

		var maxCreatedAt *int64
		for _, payload := range page.Events {
			if err := r.onMessage(ctx, relayURL, payload); err != nil {
				_ = r.markFailed(ctx, checkpoint, cursor)
				return fmt.Errorf("persist page event relay %s: %w", relayURL, err)
			}
			createdAt, ok := extractCreatedAt(payload)
			if !ok {
				continue
			}
			if maxCreatedAt == nil || createdAt > *maxCreatedAt {
				v := createdAt
				maxCreatedAt = &v
			}
		}

		if maxCreatedAt != nil && (cursor == nil || *maxCreatedAt > *cursor) {
			cursor = maxCreatedAt
		}
		if page.EOSESeen {
			now := r.nowFn().UTC()
			checkpoint.EOSESeenAt = &now
		}
		if err := r.persistRunning(ctx, checkpoint, cursor); err != nil {
			return err
		}

		if r.cfg.Until != nil && cursor != nil && *cursor >= *r.cfg.Until {
			return r.markCompleted(ctx, checkpoint, cursor)
		}

		if page.EOSESeen {
			if len(page.Events) == 0 {
				return r.markCompleted(ctx, checkpoint, cursor)
			}
			emptyPages = 0
			continue
		}

		if len(page.Events) == 0 {
			emptyPages++
			if emptyPages >= r.cfg.EmptyPageMax {
				return r.markCompleted(ctx, checkpoint, cursor)
			}
			continue
		}

		emptyPages = 0
	}
}

func (r *Runner) ensureRunningCheckpoint(ctx context.Context, relayURL string) (*model.IngestCheckpoint, error) {
	existing, err := r.store.GetIngestCheckpoint(ctx, relayURL, r.cfg.Mode, r.cfg.FilterGroup)
	if err != nil {
		return nil, fmt.Errorf("load checkpoint relay %s: %w", relayURL, err)
	}
	if existing != nil {
		existing.Status = model.CheckpointRunning
		existing.UpdatedAt = r.nowFn().UTC()
		if err := r.store.UpsertIngestCheckpoint(ctx, *existing); err != nil {
			return nil, fmt.Errorf("set running checkpoint relay %s: %w", relayURL, err)
		}
		return existing, nil
	}

	created := model.IngestCheckpoint{
		RelayURL:    relayURL,
		Mode:        r.cfg.Mode,
		FilterGroup: r.cfg.FilterGroup,
		Since:       cloneInt64(r.cfg.Since),
		Until:       cloneInt64(r.cfg.Until),
		Status:      model.CheckpointRunning,
		UpdatedAt:   r.nowFn().UTC(),
	}
	if err := r.store.UpsertIngestCheckpoint(ctx, created); err != nil {
		return nil, fmt.Errorf("create checkpoint relay %s: %w", relayURL, err)
	}
	return &created, nil
}

func (r *Runner) persistRunning(ctx context.Context, checkpoint *model.IngestCheckpoint, cursor *int64) error {
	checkpoint.Cursor = formatCursor(cursor)
	checkpoint.Status = model.CheckpointRunning
	checkpoint.UpdatedAt = r.nowFn().UTC()
	if err := r.store.UpsertIngestCheckpoint(ctx, *checkpoint); err != nil {
		return fmt.Errorf("persist running checkpoint relay %s: %w", checkpoint.RelayURL, err)
	}
	return nil
}

func (r *Runner) markCompleted(ctx context.Context, checkpoint *model.IngestCheckpoint, cursor *int64) error {
	checkpoint.Cursor = formatCursor(cursor)
	checkpoint.Status = model.CheckpointCompleted
	checkpoint.UpdatedAt = r.nowFn().UTC()
	if err := r.store.UpsertIngestCheckpoint(ctx, *checkpoint); err != nil {
		return fmt.Errorf("persist completed checkpoint relay %s: %w", checkpoint.RelayURL, err)
	}
	r.log.Info(
		"backfill_relay_completed",
		"relay_url", checkpoint.RelayURL,
		"filter_group", checkpoint.FilterGroup,
		"cursor", checkpoint.Cursor,
	)
	return nil
}

func (r *Runner) markFailed(ctx context.Context, checkpoint *model.IngestCheckpoint, cursor *int64) error {
	checkpoint.Cursor = formatCursor(cursor)
	checkpoint.Status = model.CheckpointFailed
	checkpoint.UpdatedAt = r.nowFn().UTC()
	return r.store.UpsertIngestCheckpoint(ctx, *checkpoint)
}

func (r *Runner) nextSince(cursor *int64) *int64 {
	if cursor != nil {
		next := *cursor + 1
		return &next
	}
	return cloneInt64(r.cfg.Since)
}

func cloneInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func extractCreatedAt(payload []byte) (int64, bool) {
	var envelope struct {
		CreatedAt int64 `json:"created_at"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return 0, false
	}
	if envelope.CreatedAt < 0 {
		return 0, false
	}
	return envelope.CreatedAt, true
}

func parseCursorInt64(cursor *string) *int64 {
	if cursor == nil {
		return nil
	}
	v, err := strconv.ParseInt(strings.TrimSpace(*cursor), 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func formatCursor(cursor *int64) *string {
	if cursor == nil {
		return nil
	}
	v := strconv.FormatInt(*cursor, 10)
	return &v
}
