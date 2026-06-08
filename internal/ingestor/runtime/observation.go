package runtime

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// observationStore persists batched observation counts. Satisfied by
// *store.PostgresStore.
type observationStore interface {
	BatchIncrementAccountObservations(ctx context.Context, deltas map[string]int64) error
}

// ObservationBuffer is the cheap, counts-only observation accumulator used by
// the live ingest hot path. Observe() only touches an in-memory map; the actual
// account_states UPSERT is batched out-of-band by the flush loop. This is the
// substitute for a raw event buffer: it records that a pubkey has been seen N
// times (including gated/blocked events) without retaining any payload.
type ObservationBuffer struct {
	mu      sync.Mutex
	counts  map[string]int64
	maxKeys int
}

// NewObservationBuffer returns an empty buffer. maxKeys caps the number of
// distinct pubkeys held between flushes (0 = unbounded); when exceeded, new
// pubkeys are dropped until the next flush to bound memory under a flood.
func NewObservationBuffer(maxKeys int) *ObservationBuffer {
	return &ObservationBuffer{
		counts:  make(map[string]int64),
		maxKeys: maxKeys,
	}
}

// Observe records one observation of pubkey. Non-blocking and cheap.
func (b *ObservationBuffer) Observe(pubkey string) {
	if b == nil {
		return
	}
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return
	}
	b.mu.Lock()
	if _, exists := b.counts[pubkey]; !exists && b.maxKeys > 0 && len(b.counts) >= b.maxKeys {
		b.mu.Unlock()
		return
	}
	b.counts[pubkey]++
	b.mu.Unlock()
}

// drain atomically swaps out and returns the accumulated counts.
func (b *ObservationBuffer) drain() map[string]int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.counts) == 0 {
		return nil
	}
	out := b.counts
	b.counts = make(map[string]int64)
	return out
}

// RunObservationFlushLoop periodically flushes the observation buffer to the
// store. A final flush runs on shutdown so in-flight observations are not lost.
func RunObservationFlushLoop(ctx context.Context, log *slog.Logger, s observationStore, buffer *ObservationBuffer, interval time.Duration) {
	if log == nil {
		log = slog.Default()
	}
	if s == nil || buffer == nil {
		return
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	log.Info("observation_flush_enabled", "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	flush := func() {
		deltas := buffer.drain()
		if len(deltas) == 0 {
			return
		}
		if err := s.BatchIncrementAccountObservations(ctx, deltas); err != nil {
			log.Error("observation_flush_failed", "pubkeys", len(deltas), "error", err)
			return
		}
		log.Debug("observation_flush_ok", "pubkeys", len(deltas))
	}
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			deltas := buffer.drain()
			if len(deltas) > 0 {
				if err := s.BatchIncrementAccountObservations(flushCtx, deltas); err != nil {
					log.Error("observation_final_flush_failed", "pubkeys", len(deltas), "error", err)
				}
			}
			cancel()
			return
		case <-ticker.C:
			flush()
		}
	}
}
