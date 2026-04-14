package relayprobe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/relayregistry"
)

// ProbeResult holds the outcome of a single relay probe.
type ProbeResult struct {
	ConnectOK        bool
	SubscribeOK      bool
	EOSEOK           bool
	ConnectLatencyMs float64
	EOSELatencyMs    float64
	Status           relayregistry.ProbeStatus
	ErrorCode        string
	ErrorText        string
	SampleYieldCount int
	SampleDupRatio   float64
}

// ProbeConfig controls probe behavior.
type ProbeConfig struct {
	ConnectTimeout time.Duration
	EOSETimeout    time.Duration
}

// Prober tests relay health by connecting, subscribing, and waiting for EOSE.
type Prober struct {
	cfg ProbeConfig
}

func NewProber(cfg ProbeConfig) *Prober {
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.EOSETimeout <= 0 {
		cfg.EOSETimeout = 15 * time.Second
	}
	return &Prober{cfg: cfg}
}

// Probe tests a relay's health: connect, subscribe with a small filter, wait for EOSE.
func (p *Prober) Probe(ctx context.Context, relayURL string) ProbeResult {
	result := ProbeResult{Status: relayregistry.ProbeStatusUnknownError}

	connectStart := time.Now()
	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: p.cfg.ConnectTimeout,
	}
	conn, _, err := dialer.DialContext(ctx, relayURL, nil)
	result.ConnectLatencyMs = float64(time.Since(connectStart).Milliseconds())
	if err != nil {
		result.Status = relayregistry.ProbeStatusConnectFailed
		result.ErrorCode = "connect_failed"
		result.ErrorText = truncateError(err)
		return result
	}
	defer conn.Close()
	result.ConnectOK = true

	subID := fmt.Sprintf("nm-probe-%d", time.Now().UnixNano())
	req := []any{
		"REQ", subID,
		map[string]any{
			"kinds": []int{0},
			"limit": 1,
		},
	}
	reqRaw, err := json.Marshal(req)
	if err != nil {
		result.Status = relayregistry.ProbeStatusProtocolError
		result.ErrorCode = "marshal_failed"
		result.ErrorText = truncateError(err)
		return result
	}
	if err := conn.WriteMessage(websocket.TextMessage, reqRaw); err != nil {
		result.Status = relayregistry.ProbeStatusSubscribeFailed
		result.ErrorCode = "subscribe_failed"
		result.ErrorText = truncateError(err)
		return result
	}
	result.SubscribeOK = true

	eoseStart := time.Now()
	eoseDeadline := time.Now().Add(p.cfg.EOSETimeout)
	if err := conn.SetReadDeadline(eoseDeadline); err != nil {
		result.Status = relayregistry.ProbeStatusProtocolError
		result.ErrorCode = "set_read_deadline_failed"
		result.ErrorText = truncateError(err)
		return result
	}

	var eventCount int
	for {
		_, frame, err := conn.ReadMessage()
		if err != nil {
			if time.Now().After(eoseDeadline) {
				result.Status = relayregistry.ProbeStatusEOSETimeout
				result.ErrorCode = "eose_timeout"
				result.ErrorText = "EOSE not received within timeout"
			} else {
				result.Status = relayregistry.ProbeStatusProtocolError
				result.ErrorCode = "read_failed"
				result.ErrorText = truncateError(err)
			}
			result.EOSELatencyMs = float64(time.Since(eoseStart).Milliseconds())
			result.SampleYieldCount = eventCount
			return result
		}

		msgType := classifyFrame(frame)
		switch msgType {
		case "EOSE":
			result.EOSEOK = true
			result.EOSELatencyMs = float64(time.Since(eoseStart).Milliseconds())
			result.SampleYieldCount = eventCount
			result.Status = relayregistry.ProbeStatusOK
			closeRelay(conn, subID)
			return result
		case "EVENT":
			eventCount++
		case "NOTICE":
			result.Status = relayregistry.ProbeStatusRateLimited
			result.ErrorCode = "notice"
			result.ErrorText = string(frame)
			result.EOSELatencyMs = float64(time.Since(eoseStart).Milliseconds())
			result.SampleYieldCount = eventCount
			closeRelay(conn, subID)
			return result
		case "CLOSED":
			result.Status = relayregistry.ProbeStatusSubscribeFailed
			result.ErrorCode = "subscription_closed"
			result.ErrorText = string(frame)
			result.EOSELatencyMs = float64(time.Since(eoseStart).Milliseconds())
			result.SampleYieldCount = eventCount
			return result
		}
	}
}

func classifyFrame(frame []byte) string {
	var envelope []json.RawMessage
	if err := json.Unmarshal(frame, &envelope); err != nil || len(envelope) == 0 {
		return "unknown"
	}
	var msgType string
	if err := json.Unmarshal(envelope[0], &msgType); err != nil {
		return "unknown"
	}
	return msgType
}

func closeRelay(conn *websocket.Conn, subID string) {
	closeMsg, _ := json.Marshal([]string{"CLOSE", subID})
	_ = conn.WriteMessage(websocket.TextMessage, closeMsg)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		time.Now().Add(time.Second),
	)
}

func truncateError(err error) string {
	s := err.Error()
	if len(s) > 200 {
		return s[:200]
	}
	return s
}
