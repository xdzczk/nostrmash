package runtime

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestObservationBuffer_ObserveAccumulatesAndNormalizes(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(0)
	b.Observe("ABCD")
	b.Observe(" abcd ")
	b.Observe("abcd")
	b.Observe("ef01")

	got := b.drain()
	if len(got) != 2 {
		t.Fatalf("expected 2 distinct pubkeys, got %d (%v)", len(got), got)
	}
	if got["abcd"] != 3 {
		t.Fatalf("expected abcd count 3 (case/space folded), got %d", got["abcd"])
	}
	if got["ef01"] != 1 {
		t.Fatalf("expected ef01 count 1, got %d", got["ef01"])
	}
}

func TestObservationBuffer_DrainResetsAndIsEmptyAfter(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(0)
	b.Observe("aa")
	if first := b.drain(); len(first) != 1 {
		t.Fatalf("expected one entry on first drain, got %v", first)
	}
	if second := b.drain(); second != nil {
		t.Fatalf("expected nil on drain of empty buffer, got %v", second)
	}
}

func TestObservationBuffer_BlankInputsIgnored(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(0)
	b.Observe("")
	b.Observe("   ")
	if got := b.drain(); got != nil {
		t.Fatalf("blank observations should be ignored, got %v", got)
	}
}

func TestObservationBuffer_MaxKeysCapsNewPubkeysButCountsExisting(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(2)
	b.Observe("a")
	b.Observe("b")
	// New distinct key beyond the cap is dropped...
	b.Observe("c")
	// ...but existing keys keep counting.
	b.Observe("a")

	got := b.drain()
	if len(got) != 2 {
		t.Fatalf("expected cap of 2 distinct keys, got %d (%v)", len(got), got)
	}
	if _, ok := got["c"]; ok {
		t.Fatal("over-cap key c should have been dropped")
	}
	if got["a"] != 2 {
		t.Fatalf("expected existing key a to count to 2, got %d", got["a"])
	}
}

func TestObservationBuffer_NilSafe(t *testing.T) {
	t.Parallel()
	var b *ObservationBuffer
	b.Observe("x") // must not panic
}

func TestObservationBuffer_ConcurrentObserveIsRaceFree(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(0)
	const workers, perWorker = 8, 1000
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				b.Observe("hot")
			}
		}()
	}
	wg.Wait()
	got := b.drain()
	if got["hot"] != int64(workers*perWorker) {
		t.Fatalf("expected %d, got %d", workers*perWorker, got["hot"])
	}
}

type fakeObservationStore struct {
	mu     sync.Mutex
	deltas []map[string]int64
}

func (f *fakeObservationStore) BatchIncrementAccountObservations(_ context.Context, deltas map[string]int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(map[string]int64, len(deltas))
	for k, v := range deltas {
		cp[k] = v
	}
	f.deltas = append(f.deltas, cp)
	return nil
}

func (f *fakeObservationStore) total(pubkey string) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var sum int64
	for _, d := range f.deltas {
		sum += d[pubkey]
	}
	return sum
}

func TestRunObservationFlushLoop_FlushesOnShutdown(t *testing.T) {
	t.Parallel()
	b := NewObservationBuffer(0)
	b.Observe("alice")
	b.Observe("alice")
	b.Observe("bob")

	store := &fakeObservationStore{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		// Large interval so only the shutdown path flushes.
		RunObservationFlushLoop(ctx, nil, store, b, time.Hour)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("flush loop did not return after cancel")
	}

	if store.total("alice") != 2 {
		t.Fatalf("expected alice flushed total 2, got %d", store.total("alice"))
	}
	if store.total("bob") != 1 {
		t.Fatalf("expected bob flushed total 1, got %d", store.total("bob"))
	}
}
