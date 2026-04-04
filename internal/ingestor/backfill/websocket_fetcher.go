package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// WebsocketFetcher retrieves one backfill page per REQ.
type WebsocketFetcher struct {
	Log *slog.Logger

	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
}

func (f WebsocketFetcher) FetchPage(ctx context.Context, relayURL string, request PageRequest) (PageResult, error) {
	if request.Limit <= 0 {
		return PageResult{}, fmt.Errorf("page limit must be > 0")
	}
	if f.ConnectTimeout <= 0 {
		f.ConnectTimeout = 10 * time.Second
	}
	if f.IdleTimeout <= 0 {
		f.IdleTimeout = 3 * time.Second
	}

	connectCtx, cancelConnect := context.WithTimeout(ctx, f.ConnectTimeout)
	defer cancelConnect()

	dialer := websocket.Dialer{
		Proxy: http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(connectCtx, relayURL, nil)
	if err != nil {
		return PageResult{}, fmt.Errorf("dial websocket: %w", err)
	}
	defer conn.Close()

	subID := fmt.Sprintf("nm-backfill-%d", time.Now().UnixNano())
	filter := map[string]any{
		"kinds": request.Kinds,
		"limit": request.Limit,
	}
	if request.Since != nil {
		filter["since"] = *request.Since
	}
	if request.Until != nil {
		filter["until"] = *request.Until
	}
	req := []any{"REQ", subID, filter}
	reqRaw, err := json.Marshal(req)
	if err != nil {
		return PageResult{}, fmt.Errorf("marshal backfill req: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, reqRaw); err != nil {
		return PageResult{}, fmt.Errorf("write backfill req: %w", err)
	}

	result := PageResult{}
	idleTimer := time.NewTimer(f.IdleTimeout)
	defer idleTimer.Stop()
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(f.IdleTimeout)
	}

	defer func() {
		closeMsg, _ := json.Marshal([]any{"CLOSE", subID})
		_ = conn.WriteMessage(websocket.TextMessage, closeMsg)
	}()

	for {
		if err := ctx.Err(); err != nil {
			return PageResult{}, err
		}

		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, frame, err := conn.ReadMessage()
		if err != nil {
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				select {
				case <-idleTimer.C:
					return result, nil
				default:
				}
				continue
			}
			return PageResult{}, fmt.Errorf("read relay frame: %w", err)
		}
		resetIdle()

		envType, gotSubID, payload, ok := parseNostrEnvelope(frame)
		if !ok || gotSubID != subID {
			continue
		}
		switch envType {
		case "EVENT":
			result.Events = append(result.Events, payload)
		case "EOSE":
			result.EOSESeen = true
			return result, nil
		}
	}
}

func parseNostrEnvelope(frame []byte) (envType string, subID string, payload []byte, ok bool) {
	var env []json.RawMessage
	if err := json.Unmarshal(frame, &env); err != nil || len(env) < 2 {
		return "", "", nil, false
	}
	if err := json.Unmarshal(env[0], &envType); err != nil {
		return "", "", nil, false
	}
	if err := json.Unmarshal(env[1], &subID); err != nil {
		return "", "", nil, false
	}
	if envType == "EVENT" && len(env) >= 3 {
		return envType, subID, env[2], true
	}
	if envType == "EOSE" {
		return envType, subID, nil, true
	}
	return "", "", nil, false
}
