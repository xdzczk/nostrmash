package api_primal

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
)

func checkOrigin(r *http.Request, opts WSGatewayOptions) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		// Non-browser clients commonly omit Origin.
		return true
	}
	if opts.AllowAnyOrigin {
		return true
	}
	if len(opts.AllowedOrigins) == 0 {
		return false
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Scheme == "" || parsedOrigin.Host == "" {
		return false
	}
	normalizedOrigin := parsedOrigin.Scheme + "://" + parsedOrigin.Host
	for _, allowed := range opts.AllowedOrigins {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if strings.EqualFold(allowed, normalizedOrigin) {
			return true
		}
	}
	return false
}

func (g WSGateway) runWS(conn *websocket.Conn, r *http.Request) {
	defer conn.Close()
	metrics.IncPrimalWSConnection()
	defer metrics.DecPrimalWSConnection()
	conn.SetReadLimit(g.opts.MaxMessageBytes)
	remoteAddr := conn.RemoteAddr().String()
	g.log.Info("compat_ws_connected", "remote_addr", remoteAddr)
	defer g.log.Info("compat_ws_disconnected", "remote_addr", remoteAddr)

	var mu sync.Mutex
	var writeMu sync.Mutex
	subscriptions := make(map[string]struct{})
	dmLiveSubscriptions := make(map[string]dmLiveSubscription)
	windowStarted := time.Now().UTC()
	reqInWindow := 0
	dmReqInWindow := 0
	done := make(chan struct{})
	defer close(done)

	sendFrame := func(frame any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeFrame(conn, frame)
	}

	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := failure.FromPanic(recovered)
				class := failure.ClassifyError(err)
				g.log.Error("compat_ws_live_counts_panic_recovered", "failure_class", class.Class, "failure_reason", class.Reason, "error", err)
			}
		}()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mu.Lock()
				liveSubs := make([]dmLiveSubscription, 0, len(dmLiveSubscriptions))
				for _, sub := range dmLiveSubscriptions {
					liveSubs = append(liveSubs, sub)
				}
				mu.Unlock()
				for _, sub := range liveSubs {
					frame, err := g.resolveDMCountFrame(r.Context(), sub)
					if err != nil {
						continue
					}
					if err := sendFrame(frame); err != nil {
						return
					}
				}
			}
		}
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		msg, err := decodeFrame(payload)
		if err != nil {
			_ = sendFrame([]any{"NOTICE", "", "invalid json frame"})
			continue
		}
		if len(msg) < 2 {
			_ = sendFrame([]any{"NOTICE", "", "malformed message"})
			continue
		}
		kind, _ := msg[0].(string)
		subID, _ := msg[1].(string)
		metrics.IncPrimalWSFrame(strings.ToLower(strings.TrimSpace(kind)))
		switch kind {
		case "REQ":
			if len(msg) < 3 {
				metrics.ObservePrimalWSRequest("request", "invalid_request", 0)
				_ = sendFrame([]any{"NOTICE", subID, "missing filter"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			now := time.Now().UTC()
			if now.Sub(windowStarted) >= time.Minute {
				windowStarted = now
				reqInWindow = 0
				dmReqInWindow = 0
			}
			reqInWindow++
			if reqInWindow > g.opts.MaxReqPerMinute {
				metrics.ObservePrimalWSRequest("request", "rate_limited", 0)
				_ = sendFrame([]any{"NOTICE", subID, "rate limit exceeded"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}

			if isDirectMessagesRequest(msg[2:]) {
				dmReqInWindow++
				if dmReqInWindow > g.opts.MaxDMReqPerMinute {
					metrics.ObservePrimalWSRequest("get_directmsgs", "rate_limited", 0)
					_ = sendFrame([]any{"NOTICE", subID, "dm rate limit exceeded"})
					_ = sendFrame([]any{"EOSE", subID})
					continue
				}
			}

			mu.Lock()
			if len(subscriptions) >= g.opts.MaxSubscriptions {
				mu.Unlock()
				metrics.ObservePrimalWSRequest("request", "subscription_limit", 0)
				_ = sendFrame([]any{"NOTICE", subID, "too many subscriptions"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			subscriptions[subID] = struct{}{}
			mu.Unlock()
			if liveSub, ok := parseDMLiveSubscription(subID, msg[2:]); ok && hasOnlyDMLiveFilters(msg[2:]) {
				mu.Lock()
				dmLiveSubscriptions[subID] = liveSub
				mu.Unlock()
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			ctx, cancel := context.WithTimeout(r.Context(), g.opts.RequestTimeout)
			frames := g.handleRequestFilters(ctx, subID, remoteAddr, msg[2:])
			cancel()
			for _, frame := range frames {
				if err := sendFrame(frame); err != nil {
					return
				}
			}
			_ = sendFrame([]any{"EOSE", subID})
			if liveSub, ok := parseDMLiveSubscription(subID, msg[2:]); ok {
				mu.Lock()
				dmLiveSubscriptions[subID] = liveSub
				mu.Unlock()
				continue
			}
			mu.Lock()
			delete(subscriptions, subID)
			delete(dmLiveSubscriptions, subID)
			mu.Unlock()
		case "CLOSE":
			mu.Lock()
			delete(subscriptions, subID)
			delete(dmLiveSubscriptions, subID)
			mu.Unlock()
			g.log.Info("compat_ws_close", "sub_id", subID, "remote_addr", remoteAddr)
		default:
			metrics.ObservePrimalWSRequest("request", "unsupported_frame", 0)
			_ = sendFrame([]any{"NOTICE", subID, "unsupported frame type"})
		}
	}
}
