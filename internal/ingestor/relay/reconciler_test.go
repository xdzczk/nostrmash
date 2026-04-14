package relay

import (
	"context"
	"log/slog"
	"sort"
	"sync"
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
