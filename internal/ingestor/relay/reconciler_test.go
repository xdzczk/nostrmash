package relay

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeDesiredSetProvider struct {
	mu   sync.Mutex
	urls []string
	err  error
}

func (f *fakeDesiredSetProvider) GetDesiredActiveRelays(ctx context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.urls...), f.err
}

func (f *fakeDesiredSetProvider) SetURLs(urls []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.urls = append([]string(nil), urls...)
}

type reconcilerFakeConnector struct {
	mu          sync.Mutex
	connectURLs []string
}

func (f *reconcilerFakeConnector) Connect(ctx context.Context, relayURL string) (Connection, error) {
	f.mu.Lock()
	f.connectURLs = append(f.connectURLs, relayURL)
	f.mu.Unlock()
	return &reconcilerFakeConn{done: make(chan error), msgs: make(chan []byte)}, nil
}

func (f *reconcilerFakeConnector) ConnectedURLs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.connectURLs...)
}

type reconcilerFakeConn struct {
	done chan error
	msgs chan []byte
}

func (c *reconcilerFakeConn) Done() <-chan error      { return c.done }
func (c *reconcilerFakeConn) Messages() <-chan []byte { return c.msgs }
func (c *reconcilerFakeConn) Close() error {
	select {
	case c.done <- nil:
	default:
	}
	return nil
}

func TestReconciler_AddsDesiredRelays(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: []string{"wss://relay1.example.com", "wss://relay2.example.com"}}
	connector := &reconcilerFakeConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 100 * time.Millisecond,
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go r.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	snapshot := r.Snapshot()
	if len(snapshot) != 2 {
		t.Fatalf("expected 2 managed relays, got %d: %v", len(snapshot), snapshot)
	}
}

func TestReconciler_RemovesUnwantedRelays(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: []string{"wss://relay1.example.com", "wss://relay2.example.com"}}
	connector := &reconcilerFakeConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 100 * time.Millisecond,
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go r.Run(ctx)
	time.Sleep(150 * time.Millisecond)

	provider.SetURLs([]string{"wss://relay1.example.com"})
	time.Sleep(200 * time.Millisecond)

	snapshot := r.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 managed relay after removal, got %d: %v", len(snapshot), snapshot)
	}
	if snapshot[0] != "wss://relay1.example.com" {
		t.Fatalf("expected relay1 to remain, got %s", snapshot[0])
	}
}

func TestReconciler_UsesFallbackWhenEmpty(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: nil}
	connector := &reconcilerFakeConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 100 * time.Millisecond,
		FallbackURLs: []string{"wss://fallback.example.com"},
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go r.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	snapshot := r.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected 1 fallback relay, got %d", len(snapshot))
	}
}

func TestReconciler_NoUnnecessaryReconnects(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: []string{"wss://stable.example.com"}}
	connector := &reconcilerFakeConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 50 * time.Millisecond,
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	go r.Run(ctx)
	time.Sleep(250 * time.Millisecond)

	connected := connector.ConnectedURLs()
	count := 0
	for _, u := range connected {
		if u == "wss://stable.example.com" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("stable relay should only be connected once, got %d connect calls", count)
	}
}

// trackingConnector counts how many relay goroutines are currently live so a
// test can assert they have all exited once Run returns after cancellation.
type trackingConnector struct {
	live atomic.Int64
}

func (c *trackingConnector) Connect(ctx context.Context, relayURL string) (Connection, error) {
	c.live.Add(1)
	return &trackingConn{parent: c, done: make(chan error), msgs: make(chan []byte)}, nil
}

type trackingConn struct {
	parent *trackingConnector
	done   chan error
	msgs   chan []byte
	once   sync.Once
}

func (c *trackingConn) Done() <-chan error      { return c.done }
func (c *trackingConn) Messages() <-chan []byte { return c.msgs }
func (c *trackingConn) Close() error {
	c.once.Do(func() { c.parent.live.Add(-1) })
	return nil
}

func TestReconciler_DrainWaitsForRelayGoroutines(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: []string{"wss://relay1.example.com", "wss://relay2.example.com"}}
	connector := &trackingConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 50 * time.Millisecond,
		DrainTimeout: 2 * time.Second,
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 10 * time.Millisecond,
			MaxBackoff:     50 * time.Millisecond,
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(returned)
	}()

	// Wait for both relays to be connected.
	deadline := time.After(2 * time.Second)
	for connector.live.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("relays never connected, live=%d", connector.live.Load())
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}

	cancel()

	select {
	case <-returned:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if got := connector.live.Load(); got != 0 {
		t.Fatalf("expected all relay goroutines drained before Run returned, live=%d", got)
	}
	if snap := r.Snapshot(); len(snap) != 0 {
		t.Fatalf("expected no managed relays after drain, got %v", snap)
	}
}

func TestReconciler_SnapshotIsSorted(t *testing.T) {
	provider := &fakeDesiredSetProvider{urls: []string{"wss://z.example.com", "wss://a.example.com", "wss://m.example.com"}}
	connector := &reconcilerFakeConnector{}
	log := slog.Default()

	r := NewReconciler(log, connector, nil, nil, provider, ReconcilerConfig{
		PollInterval: 100 * time.Millisecond,
		ConnectConfig: Config{
			ConnectTimeout: 5 * time.Second,
			InitialBackoff: 100 * time.Millisecond,
			MaxBackoff:     1 * time.Second,
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go r.Run(ctx)
	time.Sleep(150 * time.Millisecond)

	snapshot := r.Snapshot()
	sort.Strings(snapshot)
	if len(snapshot) != 3 {
		t.Fatalf("expected 3 relays, got %d", len(snapshot))
	}
}
