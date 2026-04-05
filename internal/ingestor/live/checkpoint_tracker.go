package live

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/ingestor/relay"
	"github.com/xdzczk/nostrmash/internal/model"
)

// CheckpointStore persists per-relay checkpoint state.
type CheckpointStore interface {
	GetIngestCheckpoint(
		ctx context.Context,
		relayURL string,
		mode string,
		filterGroup string,
	) (*model.IngestCheckpoint, error)
	UpsertIngestCheckpoint(ctx context.Context, checkpoint model.IngestCheckpoint) error
}

// CheckpointTracker batches live checkpoint progress and mirrors relay status changes.
type CheckpointTracker struct {
	log         *slog.Logger
	store       CheckpointStore
	filterGroup string
	flushEvery  time.Duration
	nowFn       func() time.Time

	mu      sync.Mutex
	byRelay map[string]*relayCheckpointState
}

type relayCheckpointState struct {
	checkpoint model.IngestCheckpoint
	lastFlush  time.Time
	dirty      bool
}

func NewCheckpointTracker(
	log *slog.Logger,
	store CheckpointStore,
	filterGroup string,
	flushEvery time.Duration,
) (*CheckpointTracker, error) {
	if store == nil {
		return nil, fmt.Errorf("checkpoint store is required")
	}
	filterGroup = strings.TrimSpace(filterGroup)
	if filterGroup == "" {
		return nil, fmt.Errorf("filter group is required")
	}
	if flushEvery <= 0 {
		flushEvery = 5 * time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	return &CheckpointTracker{
		log:         log,
		store:       store,
		filterGroup: filterGroup,
		flushEvery:  flushEvery,
		nowFn:       time.Now,
		byRelay:     make(map[string]*relayCheckpointState),
	}, nil
}

func (t *CheckpointTracker) MarkEventProcessed(
	ctx context.Context,
	relayURL string,
	eventID string,
	createdAt int64,
) error {
	relayURL = strings.TrimSpace(relayURL)
	if relayURL == "" {
		return fmt.Errorf("relay url is required")
	}
	eventID = strings.TrimSpace(eventID)
	if createdAt < 0 {
		createdAt = 0
	}
	now := t.nowFn().UTC()

	t.mu.Lock()
	state := t.ensureRelayStateLocked(relayURL)
	if state.checkpoint.Since == nil || createdAt > *state.checkpoint.Since {
		state.checkpoint.Since = ptrInt64(createdAt)
	}
	if eventID != "" {
		state.checkpoint.LastEventID = ptrString(eventID)
	}
	state.checkpoint.LastProgressAt = ptrTime(now)
	state.checkpoint.Status = model.CheckpointHealthy
	state.checkpoint.LastError = nil
	state.checkpoint.UpdatedAt = now
	state.dirty = true
	shouldFlush := state.lastFlush.IsZero() || now.Sub(state.lastFlush) >= t.flushEvery
	t.mu.Unlock()

	if !shouldFlush {
		return nil
	}
	return t.FlushRelay(ctx, relayURL)
}

func (t *CheckpointTracker) SetRelayStatus(
	ctx context.Context,
	relayURL string,
	state relay.State,
	lastError string,
) error {
	relayURL = strings.TrimSpace(relayURL)
	if relayURL == "" {
		return fmt.Errorf("relay url is required")
	}
	now := t.nowFn().UTC()

	t.mu.Lock()
	checkpointState := t.ensureRelayStateLocked(relayURL)
	checkpointState.checkpoint.Status = string(state)
	lastError = strings.TrimSpace(lastError)
	if lastError == "" {
		checkpointState.checkpoint.LastError = nil
	} else {
		checkpointState.checkpoint.LastError = ptrString(lastError)
	}
	checkpointState.checkpoint.UpdatedAt = now
	checkpointState.dirty = true
	t.mu.Unlock()

	return t.FlushRelay(ctx, relayURL)
}

func (t *CheckpointTracker) FlushRelay(ctx context.Context, relayURL string) error {
	relayURL = strings.TrimSpace(relayURL)
	if relayURL == "" {
		return fmt.Errorf("relay url is required")
	}

	t.mu.Lock()
	state := t.ensureRelayStateLocked(relayURL)
	if !state.dirty {
		t.mu.Unlock()
		return nil
	}
	snapshot := state.checkpoint
	t.mu.Unlock()

	if err := t.store.UpsertIngestCheckpoint(ctx, snapshot); err != nil {
		return fmt.Errorf("upsert live checkpoint relay %s: %w", relayURL, err)
	}

	t.mu.Lock()
	state = t.ensureRelayStateLocked(relayURL)
	state.lastFlush = t.nowFn().UTC()
	if !state.checkpoint.UpdatedAt.After(snapshot.UpdatedAt) {
		state.dirty = false
	}
	t.mu.Unlock()
	return nil
}

func (t *CheckpointTracker) FlushAll(ctx context.Context) error {
	t.mu.Lock()
	relays := make([]string, 0, len(t.byRelay))
	for relayURL := range t.byRelay {
		relays = append(relays, relayURL)
	}
	t.mu.Unlock()

	var firstErr error
	for _, relayURL := range relays {
		if err := t.FlushRelay(ctx, relayURL); err != nil {
			t.log.Warn("live_checkpoint_flush_failed", "relay_url", relayURL, "error", err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (t *CheckpointTracker) ensureRelayStateLocked(relayURL string) *relayCheckpointState {
	if state, ok := t.byRelay[relayURL]; ok {
		return state
	}

	base := model.IngestCheckpoint{
		RelayURL:    relayURL,
		Mode:        model.ModeLive,
		FilterGroup: t.filterGroup,
		Status:      model.CheckpointConnecting,
		UpdatedAt:   t.nowFn().UTC(),
	}
	if existing, err := t.store.GetIngestCheckpoint(
		context.Background(),
		relayURL,
		model.ModeLive,
		t.filterGroup,
	); err == nil && existing != nil {
		base = *existing
	}
	state := &relayCheckpointState{checkpoint: base}
	t.byRelay[relayURL] = state
	return state
}

func ptrString(v string) *string {
	c := v
	return &c
}

func ptrInt64(v int64) *int64 {
	c := v
	return &c
}

func ptrTime(v time.Time) *time.Time {
	c := v
	return &c
}
