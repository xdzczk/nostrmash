package hydration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

// FetchFilter is a bounded Nostr REQ filter used by hydration. It supports the
// subset of filter fields hydration needs: authors, kinds, ids, #e tag refs,
// and a time/limit window.
type FetchFilter struct {
	Kinds   []int
	Authors []string
	IDs     []string
	ETags   []string
	Since   *int64
	Until   *int64
	Limit   int
}

// Fetcher retrieves raw event payloads from a single relay for one filter.
type Fetcher interface {
	Fetch(ctx context.Context, relayURL string, filter FetchFilter) ([][]byte, error)
}

// WebsocketFetcher is the default relay Fetcher. It opens one websocket per
// fetch, issues a single REQ, drains until EOSE or idle, then closes.
type WebsocketFetcher struct {
	ConnectTimeout time.Duration
	IdleTimeout    time.Duration
}

func (f WebsocketFetcher) Fetch(ctx context.Context, relayURL string, filter FetchFilter) ([][]byte, error) {
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	connectTimeout := f.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = 10 * time.Second
	}
	idleTimeout := f.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 4 * time.Second
	}

	connectCtx, cancelConnect := context.WithTimeout(ctx, connectTimeout)
	defer cancelConnect()

	dialer := websocket.Dialer{Proxy: http.ProxyFromEnvironment}
	conn, _, err := dialer.DialContext(connectCtx, relayURL, nil)
	if err != nil {
		return nil, fmt.Errorf("dial websocket: %w", err)
	}
	defer conn.Close()

	subID := fmt.Sprintf("nm-hydrate-%d", time.Now().UnixNano())
	req := []any{"REQ", subID, buildFilter(filter)}
	reqRaw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal hydrate req: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, reqRaw); err != nil {
		return nil, fmt.Errorf("write hydrate req: %w", err)
	}
	defer func() {
		closeMsg, _ := json.Marshal([]any{"CLOSE", subID})
		_ = conn.WriteMessage(websocket.TextMessage, closeMsg)
	}()

	var events [][]byte
	idleTimer := time.NewTimer(idleTimeout)
	defer idleTimer.Stop()
	resetIdle := func() {
		if !idleTimer.Stop() {
			select {
			case <-idleTimer.C:
			default:
			}
		}
		idleTimer.Reset(idleTimeout)
	}

	for {
		if err := ctx.Err(); err != nil {
			return events, err
		}
		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		_, frame, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err,
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseNoStatusReceived,
				websocket.CloseAbnormalClosure,
			) {
				return events, nil
			}
			if ne, ok := err.(interface{ Timeout() bool }); ok && ne.Timeout() {
				select {
				case <-idleTimer.C:
					return events, nil
				default:
				}
				continue
			}
			return events, fmt.Errorf("read relay frame: %w", err)
		}
		resetIdle()

		envType, gotSubID, payload, ok := parseEnvelope(frame)
		if !ok || gotSubID != subID {
			continue
		}
		switch envType {
		case "EVENT":
			events = append(events, payload)
			if len(events) >= filter.Limit {
				return events, nil
			}
		case "EOSE":
			return events, nil
		}
	}
}

func buildFilter(f FetchFilter) map[string]any {
	filter := map[string]any{"limit": f.Limit}
	if len(f.Kinds) > 0 {
		filter["kinds"] = f.Kinds
	}
	if len(f.Authors) > 0 {
		filter["authors"] = f.Authors
	}
	if len(f.IDs) > 0 {
		filter["ids"] = f.IDs
	}
	if len(f.ETags) > 0 {
		filter["#e"] = f.ETags
	}
	if f.Since != nil {
		filter["since"] = *f.Since
	}
	if f.Until != nil {
		filter["until"] = *f.Until
	}
	return filter
}

func parseEnvelope(frame []byte) (envType string, subID string, payload []byte, ok bool) {
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
