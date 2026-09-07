package live

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/traceutil"
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
	// InsertEventRelayProvenance records a relay sighting of an event that is
	// already canonical, without touching the events/event_tags rows. Used by
	// the dedup cache fast path so relay activity stats stay accurate.
	InsertEventRelayProvenance(
		ctx context.Context,
		eventID string,
		relayURL string,
		seenAt time.Time,
		pubkey string,
	) error
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
	Gated     uint64
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
	gatedCount       atomic.Uint64

	// Trust gate (optional). When trustedAuthors is nil the gate is disabled
	// and all valid events pass.
	gateMode       string
	trustedAuthors TrustedAuthors
	targetChecker  TargetExistenceChecker

	// blockedAuthors (optional) is consulted before the gate: any event from a
	// blocked author is dropped regardless of gate mode.
	blockedAuthors BlockedAuthors

	// observationSink (optional) records that a pubkey was seen at ingest
	// (including gated/blocked events). Must be cheap and non-blocking.
	observationSink ObservationSink

	// dedup (optional) short-circuits repeat sightings of already-persisted
	// events before signature verification and the canonical insert
	// transaction. Nil when disabled.
	dedup *dedupCache
}

// BlockedAuthors reports whether a pubkey is explicitly blocked. Satisfied by
// *runtime.TrustedAuthorSet.
type BlockedAuthors interface {
	Blocked(pubkey string) bool
}

// ObservationSink records cheap, counts-only observation accounting for a seen
// pubkey. Implementations must be non-blocking (in-memory buffering); the live
// hot path never performs a synchronous per-event DB write.
type ObservationSink interface {
	Observe(pubkey string)
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

// SetTrustGate enables the trust-bounded ingest gate. mode is "open" (shadow:
// record metrics, never reject) or "trusted_only" (enforce). When this is never
// called the gate stays disabled and all valid events pass.
func (p *Processor) SetTrustGate(mode string, trusted TrustedAuthors, checker TargetExistenceChecker) {
	if p == nil {
		return
	}
	p.gateMode = strings.ToLower(strings.TrimSpace(mode))
	p.trustedAuthors = trusted
	p.targetChecker = checker
}

// SetBlockedAuthors wires an optional blocked-author drop. Blocked authors have
// all events dropped before the trust gate, in any mode.
func (p *Processor) SetBlockedAuthors(blocked BlockedAuthors) {
	if p == nil {
		return
	}
	p.blockedAuthors = blocked
}

// SetObservationSink wires an optional, non-blocking observation accounting
// sink. When set, every valid event's author is recorded (including gated and
// blocked events) so account promotion can be signal-driven.
func (p *Processor) SetObservationSink(sink ObservationSink) {
	if p == nil {
		return
	}
	p.observationSink = sink
}

// SetDedupCache enables the in-memory duplicate short-circuit with the given
// capacity (entries). Size <= 0 disables it.
func (p *Processor) SetDedupCache(size int) {
	if p == nil {
		return
	}
	p.dedup = newDedupCache(size)
}

// dedupProbe is the minimal payload shape needed to consult the dedup cache
// before paying for full validation.
type dedupProbe struct {
	ID        string `json:"id"`
	CreatedAt int64  `json:"created_at"`
}

// handleCachedDuplicate short-circuits a payload whose event ID is already
// known to be canonical. Returns true when the payload was fully handled.
//
// The probe fields are unverified, but a cache hit proves an event with this
// ID already passed validation and was persisted — and the ID is the content
// hash, so whatever body this payload carries is irrelevant. Provenance is
// written with the pubkey cached from the verified first sighting, never with
// anything the incoming payload claims. The checkpoint advance trusts the
// probe's created_at the same way the gated-drop path trusts a relay's frames:
// a relay lying about its own stream can only skew its own resume window.
func (p *Processor) handleCachedDuplicate(ctx context.Context, relayURL string, payload []byte, seenAt time.Time) (bool, error) {
	if p.dedup == nil {
		return false, nil
	}
	var probe dedupProbe
	if err := json.Unmarshal(payload, &probe); err != nil || probe.ID == "" {
		return false, nil
	}
	pubkey, hit := p.dedup.Get(probe.ID)
	if !hit {
		return false, nil
	}
	if p.observationSink != nil {
		p.observationSink.Observe(pubkey)
	}
	if err := p.store.InsertEventRelayProvenance(ctx, probe.ID, relayURL, seenAt, pubkey); err != nil {
		return true, fmt.Errorf("store duplicate provenance: %w", err)
	}
	if p.checkpointWriter != nil {
		if err := p.checkpointWriter.MarkEventProcessed(ctx, relayURL, probe.ID, probe.CreatedAt); err != nil {
			return true, fmt.Errorf("persist live checkpoint (cached duplicate): %w", err)
		}
	}
	p.dupeCount.Add(1)
	metrics.IncIngestOutcome("duplicate_cached")
	p.log.Debug("ingest_event_duplicate_cached", "relay_url", relayURL, "event_id", probe.ID)
	return true, nil
}

func (p *Processor) Handle(ctx context.Context, relayURL string, payload []byte) (err error) {
	ctx, span := traceutil.StartSpan(ctx, "ingest.live.handle_event",
		traceutil.KV("relay.url", relayURL),
	)
	defer func() {
		span.End(err)
	}()
	seenAt := time.Now().UTC()
	if handled, dedupErr := p.handleCachedDuplicate(ctx, relayURL, payload, seenAt); handled {
		return dedupErr
	}
	result := nostr.ParseAndValidate(payload, p.validateOpts)
	if result.Valid() {
		if p.observationSink != nil {
			p.observationSink.Observe(result.Event.Pubkey)
		}
		if p.blockedAuthors != nil && p.blockedAuthors.Blocked(result.Event.Pubkey) {
			p.gatedCount.Add(1)
			metrics.IncIngestGateDecision(gateKindLabel(result.Event.Kind), gateDecisionRejectBlockedAuthor)
			metrics.IncIngestOutcome("gated")
			p.log.Debug(
				"ingest_event_blocked_author",
				"relay_url", relayURL,
				"event_id", result.Event.ID,
				"kind", result.Event.Kind,
			)
			if p.checkpointWriter != nil {
				if err := p.checkpointWriter.MarkEventProcessed(
					ctx,
					relayURL,
					result.Event.ID,
					result.Event.CreatedAt,
				); err != nil {
					return fmt.Errorf("persist live checkpoint (blocked): %w", err)
				}
			}
			return nil
		}
		if p.trustedAuthors != nil {
			decision := p.evaluateGate(ctx, result.Event.Kind, result.Event.Pubkey, result.Event.Tags)
			metrics.IncIngestGateDecision(decision.kindLabel, decision.decision)
			if !decision.accept {
				p.gatedCount.Add(1)
				metrics.IncIngestOutcome("gated")
				p.log.Debug(
					"ingest_event_gated",
					"relay_url", relayURL,
					"event_id", result.Event.ID,
					"kind", result.Event.Kind,
					"decision", decision.decision,
				)
				// Advance the resume checkpoint even for dropped events so a
				// restart does not re-fetch and re-drop the same span.
				if p.checkpointWriter != nil {
					if err := p.checkpointWriter.MarkEventProcessed(
						ctx,
						relayURL,
						result.Event.ID,
						result.Event.CreatedAt,
					); err != nil {
						return fmt.Errorf("persist live checkpoint (gated): %w", err)
					}
				}
				return nil
			}
		}
		event := model.Event{
			ID:          result.Event.ID,
			Pubkey:      result.Event.Pubkey,
			CreatedAt:   result.Event.CreatedAt,
			Kind:        result.Event.Kind,
			Sig:         result.Event.Sig,
			Content:     result.Event.Content,
			RawJSON:     result.RawJSON,
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

		// Cache only IDs that reached the store with a verified signature —
		// whether newly inserted or confirmed duplicates — so a hit can never
		// be poisoned by an unvalidated payload.
		p.dedup.Add(event.ID, event.Pubkey)

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
		Gated:     p.gatedCount.Load(),
	}
}

func safeJSONPayload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return raw
}
