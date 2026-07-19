package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// wsResult bundles the per-worker samples with the parallel request-name slice.
type wsResult struct {
	samples []sample
	names   []string
}

// runWSClient drives a single WS connection, issuing cache-protocol REQ frames
// in round-robin and measuring time-to-EOSE for each. It reconnects on
// transport failure and stops when ctx is cancelled. Samples recorded before
// warmupDone are discarded.
func runWSClient(
	ctx context.Context,
	wsURL string,
	reqs []wsRequest,
	f fixtures,
	timeout time.Duration,
	warmupDone <-chan struct{},
) wsResult {
	res := wsResult{}

	var conn *websocket.Conn
	defer func() {
		if conn != nil {
			_ = conn.Close()
		}
	}()

	dialer := websocket.Dialer{HandshakeTimeout: timeout}
	subCounter := 0
	i := 0
	for {
		if ctx.Err() != nil {
			return res
		}

		if conn == nil {
			start := time.Now()
			c, resp, err := dialer.DialContext(ctx, wsURL, nil)
			if resp != nil && resp.Body != nil {
				_ = resp.Body.Close()
			}
			if err != nil {
				if isClosed(warmupDone) {
					res.record("dial", sample{latency: time.Since(start), ok: false, class: "dial"})
				}
				if !sleepCtx(ctx, 100*time.Millisecond) {
					return res
				}
				continue
			}
			conn = c
		}

		req := reqs[i%len(reqs)]
		i++
		subCounter++
		subID := fmt.Sprintf("lt-%d", subCounter)

		s, transportOK := doWSRequest(conn, subID, req, f, timeout)
		if isClosed(warmupDone) {
			res.record(req.Name, s)
		}
		if !transportOK {
			_ = conn.Close()
			conn = nil
		}
	}
}

func (r *wsResult) record(name string, s sample) {
	r.samples = append(r.samples, s)
	r.names = append(r.names, name)
}

// doWSRequest sends one REQ frame and reads response frames until the matching
// EOSE. The bool return reports whether the connection is still usable.
func doWSRequest(conn *websocket.Conn, subID string, req wsRequest, f fixtures, timeout time.Duration) (sample, bool) {
	start := time.Now()
	frame := []any{"REQ", subID, map[string]any{"cache": []any{req.Verb, resolveParams(req.Params, f)}}}

	_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	if err := conn.WriteJSON(frame); err != nil {
		return sample{latency: time.Since(start), ok: false, class: "transport"}, false
	}

	deadline := time.Now().Add(timeout)
	_ = conn.SetReadDeadline(deadline)
	noticed := false
	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			class := "transport"
			if time.Now().After(deadline) {
				class = "timeout"
			}
			return sample{latency: time.Since(start), ok: false, class: class}, false
		}
		var decoded []json.RawMessage
		if err := json.Unmarshal(raw, &decoded); err != nil || len(decoded) < 2 {
			continue
		}
		var kind, frameSub string
		_ = json.Unmarshal(decoded[0], &kind)
		_ = json.Unmarshal(decoded[1], &frameSub)
		if frameSub != subID {
			continue
		}
		switch kind {
		case "NOTICE":
			noticed = true
		case "EOSE":
			if noticed {
				return sample{latency: time.Since(start), ok: false, class: "notice"}, true
			}
			return sample{latency: time.Since(start), ok: true, class: "eose"}, true
		}
	}
}

// isClosed reports whether ch has been closed, without blocking. It is used to
// gate sample recording so requests that start during warmup but complete in
// the measurement window are counted (their completion time is what matters).
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
