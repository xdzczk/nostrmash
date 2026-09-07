package api_primal

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/gorilla/websocket"
)

const (
	// wsWriteQueueSize is large enough for a bursty legitimate response
	// (thread_view can emit dozens of EVENT frames plus EOSE) without
	// blocking the request goroutine.
	wsWriteQueueSize = 128
	wsWriteDeadline  = 10 * time.Second
)

// wsWriteQueueWait is how long sendFrame parks when the queue is full
// before treating the client as too slow and disconnecting. A var so tests
// can shrink it without sleeping the production 5s.
var wsWriteQueueWait = 5 * time.Second

var (
	errSessionClosed     = errors.New("websocket session closed")
	errWriteBackpressure = errors.New("websocket write queue full")
)

func decodeFrame(payload []byte) ([]any, error) {
	var out []any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func encodeFrame(frame any) ([]byte, error) {
	return json.Marshal(frame)
}

func writeFrame(conn *websocket.Conn, frame any) error {
	raw, err := encodeFrame(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}
