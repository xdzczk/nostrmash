package live

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
)

// EventStore contains only the persistence methods needed by live ingest.
type EventStore interface {
	InsertCanonicalEventWithResult(
		ctx context.Context,
		event model.Event,
		tags [][]string,
		relayURL string,
		relaySeenAt time.Time,
	) (store.CanonicalInsertResult, error)
	InsertInvalidEvent(ctx context.Context, invalid model.InvalidEvent) error
}

// CheckpointWriter persists durable live checkpoint progress.
type CheckpointWriter interface {
	MarkEventProcessed(ctx context.Context, relayURL string, eventID string, createdAt int64) error
}

// Counters are cumulative ingest metrics for logging/observability.
type Counters struct {
	Valid     uint64
	Duplicate uint64
	Invalid   uint64
}

// Processor validates relay payloads and writes canonical/quarantine rows.
type Processor struct {
	log              *slog.Logger
	store            EventStore
	validateOpts     nostr.Options
	checkpointWriter CheckpointWriter
	validCount       atomic.Uint64
	dupeCount        atomic.Uint64
	invalidCount     atomic.Uint64
}

func NewProcessor(log *slog.Logger, store EventStore, validateOpts nostr.Options) (*Processor, error) {
	if store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Processor{
		log:          log,
		store:        store,
		validateOpts: validateOpts,
	}, nil
}

// SetCheckpointWriter wires an optional live checkpoint sink.
func (p *Processor) SetCheckpointWriter(writer CheckpointWriter) {
	if p == nil {
		return
	}
	p.checkpointWriter = writer
}

func (p *Processor) Handle(ctx context.Context, relayURL string, payload []byte) error {
	seenAt := time.Now().UTC()
	result := nostr.ParseAndValidate(payload, p.validateOpts)
	if result.Valid() {
		event := model.Event{
			ID:          result.Event.ID,
			Pubkey:      result.Event.Pubkey,
			CreatedAt:   result.Event.CreatedAt,
			Kind:        result.Event.Kind,
			Sig:         result.Event.Sig,
			Content:     result.Event.Content,
			RawJSON:     json.RawMessage(result.RawJSON),
			FirstSeenAt: seenAt,
			InsertedAt:  seenAt,
		}

		outcome, err := p.store.InsertCanonicalEventWithResult(
			ctx,
			event,
			result.Event.Tags,
			relayURL,
			seenAt,
		)
		if err != nil {
			return fmt.Errorf("store canonical event: %w", err)
		}
		if p.checkpointWriter != nil {
			if err := p.checkpointWriter.MarkEventProcessed(
				ctx,
				relayURL,
				event.ID,
				event.CreatedAt,
			); err != nil {
				return fmt.Errorf("persist live checkpoint: %w", err)
			}
		}

		if outcome.EventInserted {
			p.validCount.Add(1)
			metrics.IncIngestOutcome("valid")
			p.log.Debug("ingest_event_valid", "relay_url", relayURL, "event_id", event.ID, "kind", event.Kind)
			return nil
		}
		p.dupeCount.Add(1)
		metrics.IncIngestOutcome("duplicate")
		p.log.Debug("ingest_event_duplicate", "relay_url", relayURL, "event_id", event.ID, "kind", event.Kind)
		return nil
	}

	p.invalidCount.Add(1)
	metrics.IncIngestOutcome("invalid")
	invalid := model.InvalidEvent{
		SourceRelay:  relayURL,
		ErrorCode:    string(result.Err.Code),
		ErrorMessage: result.Err.Error(),
		RawPayload:   safeJSONPayload(result.RawJSON),
		SeenAt:       seenAt,
	}
	if err := p.store.InsertInvalidEvent(ctx, invalid); err != nil {
		return fmt.Errorf("store invalid event: %w", err)
	}
	p.log.Warn(
		"ingest_event_invalid",
		"relay_url", relayURL,
		"error_code", invalid.ErrorCode,
		"error_stage", string(result.Err.Stage),
		"error", invalid.ErrorMessage,
	)
	return nil
}

func (p *Processor) Snapshot() Counters {
	return Counters{
		Valid:     p.validCount.Load(),
		Duplicate: p.dupeCount.Load(),
		Invalid:   p.invalidCount.Load(),
	}
}

func safeJSONPayload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return json.RawMessage(raw)
}
