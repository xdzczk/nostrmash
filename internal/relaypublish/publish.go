package relaypublish

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Result is one relay's reply to an EVENT publish.
type Result struct {
	Relay string
	Reply []any
}

// Client publishes Nostr events to relays and collects OK replies.
type Client struct {
	Timeout time.Duration
	Dialer  *websocket.Dialer

	mu    sync.Mutex
	conns map[string]*relayConn
}

type relayConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

// New returns a publisher with a 15 second budget.
func New() *Client {
	return &Client{
		Timeout: 15 * time.Second,
		Dialer:  websocket.DefaultDialer,
		conns:   map[string]*relayConn{},
	}
}

// Publish sends event to each relay and returns one result per relay.
func (c *Client) Publish(ctx context.Context, relays []string, event json.RawMessage) []Result {
	if c == nil {
		return nil
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out := make([]Result, len(relays))
	var wg sync.WaitGroup
	for i, relay := range relays {
		wg.Add(1)
		go func(i int, relay string) {
			defer wg.Done()
			out[i] = c.publishOne(ctx, relay, event)
		}(i, relay)
	}
	wg.Wait()
	return out
}

func (c *Client) publishOne(ctx context.Context, relay string, event json.RawMessage) Result {
	relay = strings.TrimSpace(relay)
	id := eventID(event)
	fail := Result{Relay: relay, Reply: []any{"OK", id, false, "publish failed"}}
	if relay == "" {
		return fail
	}
	slot, err := c.connection(ctx, relay)
	if err != nil {
		fail.Reply = []any{"OK", id, false, err.Error()}
		return fail
	}
	slot.mu.Lock()
	defer slot.mu.Unlock()
	conn := slot.conn
	payload, err := json.Marshal([]any{"EVENT", json.RawMessage(event)})
	if err != nil {
		return fail
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		c.drop(relay)
		fail.Reply = []any{"OK", id, false, err.Error()}
		return fail
	}
	for {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, raw, err := conn.ReadMessage()
		if err != nil {
			c.drop(relay)
			fail.Reply = []any{"OK", id, false, err.Error()}
			return fail
		}
		var frame []any
		if err := json.Unmarshal(raw, &frame); err != nil || len(frame) < 3 {
			continue
		}
		kind, _ := frame[0].(string)
		if !strings.EqualFold(kind, "OK") {
			continue
		}
		replyID, _ := frame[1].(string)
		if id != "" && replyID != "" && replyID != id {
			continue
		}
		return Result{Relay: relay, Reply: frame}
	}
}

func (c *Client) connection(ctx context.Context, relay string) (*relayConn, error) {
	c.mu.Lock()
	slot := c.conns[relay]
	c.mu.Unlock()
	if slot != nil && slot.conn != nil {
		return slot, nil
	}
	dialer := c.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, _, err := dialer.DialContext(ctx, relay, nil)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.conns[relay]; existing != nil && existing.conn != nil {
		_ = conn.Close()
		return existing, nil
	}
	slot = &relayConn{conn: conn}
	c.conns[relay] = slot
	return slot, nil
}

func (c *Client) drop(relay string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot := c.conns[relay]; slot != nil && slot.conn != nil {
		_ = slot.conn.Close()
		slot.conn = nil
		delete(c.conns, relay)
	}
}

func eventID(event json.RawMessage) string {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(event, &payload); err != nil {
		return ""
	}
	return payload.ID
}
