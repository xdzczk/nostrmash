package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// NostrConnector dials a relay, sends one live REQ, and streams EVENT payloads.
type NostrConnector struct {
	Log           *slog.Logger
	Kinds         []int
	FilterGroup   string
	SinceResolver SinceResolver
}

func (c NostrConnector) Connect(ctx context.Context, relayURL string) (Connection, error) {
	dialer := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment,
	}
	conn, resp, err := dialer.DialContext(ctx, relayURL, nil)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}

	subID := fmt.Sprintf("nm-live-%d", time.Now().UnixNano())
	since := time.Now().UTC().Unix()
	resumeStrategy := "bootstrap_lookback"
	var checkpointSince *int64
	var overlapSeconds int64
	var bootstrapLookbackSeconds int64
	if c.SinceResolver != nil {
		resolution, err := c.SinceResolver.ResolveSince(ctx, relayURL)
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("resolve relay since: %w", err)
		}
		since = resolution.Since
		resumeStrategy = resolution.Strategy
		checkpointSince = resolution.CheckpointSince
		overlapSeconds = resolution.OverlapSeconds
		bootstrapLookbackSeconds = resolution.BootstrapLookbackSeconds
	}
	req := []any{
		"REQ",
		subID,
		map[string]any{
			"kinds": c.Kinds,
			"since": since,
		},
	}
	reqRaw, err := json.Marshal(req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("marshal relay subscription: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, reqRaw); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write relay subscription: %w", err)
	}

	wsConn := newWSRelayConnection(conn, relayURL, c.Log)
	go wsConn.readLoop()

	if c.Log != nil {
		c.Log.Info(
			"relay_subscription_started",
			"relay_url", relayURL,
			"filter_group", c.FilterGroup,
			"kinds", c.Kinds,
			"since", since,
			"resume_strategy", resumeStrategy,
			"checkpoint_since", checkpointSince,
			"resume_overlap_seconds", overlapSeconds,
			"bootstrap_lookback_seconds", bootstrapLookbackSeconds,
			"subscription_id", subID,
		)
	}
	return wsConn, nil
}

type wsRelayConnection struct {
	log      *slog.Logger
	relayURL string
	conn     *websocket.Conn

	msgs chan []byte
	done chan error

	readCtx    context.Context
	cancelRead context.CancelFunc
	closeOnce  sync.Once
}

func newWSRelayConnection(conn *websocket.Conn, relayURL string, log *slog.Logger) *wsRelayConnection {
	readCtx, cancel := context.WithCancel(context.Background())
	wsConn := &wsRelayConnection{
		log:        log,
		relayURL:   relayURL,
		conn:       conn,
		msgs:       make(chan []byte, 128),
		done:       make(chan error, 1),
		readCtx:    readCtx,
		cancelRead: cancel,
	}
	go func() {
		<-readCtx.Done()
		_ = conn.Close()
	}()
	return wsConn
}

func (c *wsRelayConnection) Done() <-chan error {
	return c.done
}

func (c *wsRelayConnection) Messages() <-chan []byte {
	return c.msgs
}

func (c *wsRelayConnection) Close() error {
	c.closeWith(nil)
	return nil
}

func (c *wsRelayConnection) readLoop() {
	defer close(c.msgs)

	for {
		_, frame, err := c.conn.ReadMessage()
		if err != nil {
			c.closeWith(err)
			return
		}
		payload, ok := extractEventPayload(frame)
		if !ok {
			continue
		}
		select {
		case c.msgs <- payload:
		case <-c.readCtx.Done():
			c.closeWith(nil)
			return
		}
	}
}

func (c *wsRelayConnection) closeWith(err error) {
	c.closeOnce.Do(func() {
		c.cancelRead()
		_ = c.conn.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "closing"),
			time.Now().Add(time.Second),
		)
		_ = c.conn.Close()
		if closeErr := sanitizeCloseErr(err); closeErr != nil {
			c.done <- closeErr
		}
		close(c.done)
	})
}

func sanitizeCloseErr(err error) error {
	if err == nil {
		return nil
	}
	if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
		return nil
	}
	return err
}

func extractEventPayload(frame []byte) ([]byte, bool) {
	trimmed := bytes.TrimSpace(frame)
	if len(trimmed) == 0 {
		return nil, false
	}

	var envelope []json.RawMessage
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return nil, false
	}
	if len(envelope) == 0 {
		return nil, false
	}

	var envelopeType string
	if err := json.Unmarshal(envelope[0], &envelopeType); err != nil {
		return nil, false
	}
	if envelopeType != "EVENT" {
		return nil, false
	}
	if len(envelope) < 3 {
		return trimmed, true
	}
	return bytes.TrimSpace(envelope[2]), true
}
