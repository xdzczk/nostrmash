package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestBoundedBackoffClampsToMax(t *testing.T) {
	b := newBoundedBackoff(10*time.Millisecond, 35*time.Millisecond)
	got := []time.Duration{
		b.Next(),
		b.Next(),
		b.Next(),
		b.Next(),
	}
	want := []time.Duration{
		10 * time.Millisecond,
		20 * time.Millisecond,
		35 * time.Millisecond,
		35 * time.Millisecond,
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step %d: got %s want %s", i, got[i], want[i])
		}
	}

	b.Reset()
	if next := b.Next(); next != 10*time.Millisecond {
		t.Fatalf("after reset: got %s want %s", next, 10*time.Millisecond)
	}
}

func TestManagerReconnectsWithBackoffAndBecomesHealthy(t *testing.T) {
	const relayURL = "wss://relay.example.com"

	conn := newFakeConnection()
	connector := &scriptedConnector{
		steps: []connectStep{
			{err: errors.New("dial failed")},
			{conn: conn},
		},
	}

	manager, err := NewManager(
		Config{
			Relays:         []string{relayURL},
			Allowlist:      []string{relayURL},
			ConnectTimeout: 100 * time.Millisecond,
			InitialBackoff: 25 * time.Millisecond,
			MaxBackoff:     75 * time.Millisecond,
			LagThreshold:   time.Second,
		},
		connector,
		nil,
		testLogger(),
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Start(ctx)

	waitForState(t, manager, relayURL, StateBackingOff, 500*time.Millisecond)
	waitForState(t, manager, relayURL, StateHealthy, time.Second)

	snapshot := manager.Snapshot()[relayURL]
	if snapshot.Attempts < 2 {
		t.Fatalf("expected at least 2 connection attempts, got %d", snapshot.Attempts)
	}
	if snapshot.Backoff != 0 {
		t.Fatalf("expected backoff reset after connect, got %s", snapshot.Backoff)
	}

	// Simulate connection loss and verify lifecycle enters reconnect backoff.
	conn.drop(errors.New("dropped"))
	waitForState(t, manager, relayURL, StateBackingOff, time.Second)
}

func TestManagerTransitionsToLaggingAndRecoversOnProgress(t *testing.T) {
	const relayURL = "wss://relay.example.com"

	connector := &scriptedConnector{
		steps: []connectStep{
			{conn: newFakeConnection()},
		},
	}
	manager, err := NewManager(
		Config{
			Relays:         []string{relayURL},
			Allowlist:      []string{relayURL},
			ConnectTimeout: 100 * time.Millisecond,
			InitialBackoff: 20 * time.Millisecond,
			MaxBackoff:     40 * time.Millisecond,
			LagThreshold:   60 * time.Millisecond,
		},
		connector,
		nil,
		testLogger(),
	)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go manager.Start(ctx)

	waitForState(t, manager, relayURL, StateHealthy, 500*time.Millisecond)
	waitForState(t, manager, relayURL, StateLagging, 2*time.Second)

	manager.MarkProgress(relayURL)
	waitForState(t, manager, relayURL, StateHealthy, 500*time.Millisecond)
}

func waitForState(t *testing.T, manager *Manager, relayURL string, want State, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		snapshot := manager.Snapshot()
		status, ok := snapshot[relayURL]
		if ok && status.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	snapshot := manager.Snapshot()
	t.Fatalf("timed out waiting for state %q; last state=%q", want, snapshot[relayURL].State)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

type connectStep struct {
	conn Connection
	err  error
}

type scriptedConnector struct {
	mu    sync.Mutex
	steps []connectStep
}

func (s *scriptedConnector) Connect(ctx context.Context, relayURL string) (Connection, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.steps) == 0 {
		return nil, errors.New("no scripted connector steps left")
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	return step.conn, step.err
}

type fakeConnection struct {
	done chan error
	msgs chan []byte
	once sync.Once
}

func newFakeConnection() *fakeConnection {
	return &fakeConnection{
		done: make(chan error, 1),
		msgs: make(chan []byte),
	}
}

func (c *fakeConnection) Done() <-chan error {
	return c.done
}

func (c *fakeConnection) Messages() <-chan []byte {
	return c.msgs
}

func (c *fakeConnection) Close() error {
	c.once.Do(func() {
		close(c.msgs)
		close(c.done)
	})
	return nil
}

func (c *fakeConnection) drop(err error) {
	c.once.Do(func() {
		close(c.msgs)
		c.done <- err
		close(c.done)
	})
}
