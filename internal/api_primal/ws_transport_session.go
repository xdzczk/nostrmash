package api_primal

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/failure"
)

type wsConnSession struct {
	gateway    WSGateway
	conn       *websocket.Conn
	requestCtx context.Context
	remoteAddr string

	mu                  sync.Mutex
	writeMu             sync.Mutex
	subscriptions       map[string]struct{}
	dmLiveSubscriptions map[string]dmLiveSubscription
	windowStarted       time.Time
	reqInWindow         int
	dmReqInWindow       int
	done                chan struct{}
	closeOnce           sync.Once
}

func newWSConnSession(requestCtx context.Context, g WSGateway, conn *websocket.Conn, remoteAddr string) *wsConnSession {
	return &wsConnSession{
		gateway:             g,
		conn:                conn,
		requestCtx:          requestCtx,
		remoteAddr:          remoteAddr,
		subscriptions:       make(map[string]struct{}),
		dmLiveSubscriptions: make(map[string]dmLiveSubscription),
		windowStarted:       time.Now().UTC(),
		done:                make(chan struct{}),
	}
}

func (s *wsConnSession) run() {
	defer close(s.done)
	s.startDMLiveCountLoop()
	for {
		_, payload, err := s.conn.ReadMessage()
		if err != nil {
			return
		}
		msg, err := decodeFrame(payload)
		if err != nil {
			_ = s.sendFrame([]any{"NOTICE", "", "invalid json frame"})
			continue
		}
		if len(msg) < 2 {
			_ = s.sendFrame([]any{"NOTICE", "", "malformed message"})
			continue
		}
		kind, _ := msg[0].(string)
		subID, _ := msg[1].(string)
		metrics.IncPrimalWSFrame(strings.ToLower(strings.TrimSpace(kind)))
		handler, ok := wsFrameHandlers[kind]
		if !ok {
			metrics.ObservePrimalWSRequest("request", "unsupported_frame", 0)
			_ = s.sendFrame([]any{"NOTICE", subID, "unsupported frame type"})
			continue
		}
		if err := handler(s, subID, msg[2:]); err != nil {
			return
		}
	}
}

func (s *wsConnSession) sendFrame(frame any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := writeFrame(s.conn, frame); err != nil {
		// A failed write means the connection is broken. Tear it down so the
		// read loop unblocks and the session ends promptly, rather than
		// silently continuing to operate on a dead connection.
		s.teardown()
		return err
	}
	return nil
}

// teardown closes the underlying connection exactly once. Closing unblocks the
// blocking ReadMessage in run(), which then returns and drives full cleanup.
func (s *wsConnSession) teardown() {
	s.closeOnce.Do(func() {
		_ = s.conn.Close()
	})
}

func (s *wsConnSession) handleREQFrame(subID string, filters []any) error {
	if len(filters) == 0 {
		metrics.ObservePrimalWSRequest("request", "invalid_request", 0)
		_ = s.sendFrame([]any{"NOTICE", subID, "missing filter"})
		_ = s.sendFrame([]any{"EOSE", subID})
		return nil
	}
	if !s.allowRequest(filters, subID) {
		return nil
	}
	if !s.reserveSubscription(subID) {
		return nil
	}

	if liveSub, ok := parseDMLiveSubscription(subID, filters); ok && hasOnlyDMLiveFilters(filters) {
		s.addLiveSubscription(liveSub)
		_ = s.sendFrame([]any{"EOSE", subID})
		return nil
	}

	ctx, cancel := context.WithTimeout(s.requestCtx, s.gateway.opts.RequestTimeout)
	frames := s.gateway.handleRequestFilters(ctx, subID, s.remoteAddr, filters)
	cancel()
	for _, frame := range frames {
		if err := s.sendFrame(frame); err != nil {
			return err
		}
	}
	_ = s.sendFrame([]any{"EOSE", subID})
	if liveSub, ok := parseDMLiveSubscription(subID, filters); ok {
		s.addLiveSubscription(liveSub)
		return nil
	}
	s.dropSubscription(subID)
	return nil
}

func (s *wsConnSession) handleCLOSEFrame(subID string) {
	s.dropSubscription(subID)
	s.gateway.log.Info("compat_ws_close", "sub_id", subID, "remote_addr", s.remoteAddr)
}

func (s *wsConnSession) allowRequest(filters []any, subID string) bool {
	now := time.Now().UTC()
	if now.Sub(s.windowStarted) >= time.Minute {
		s.windowStarted = now
		s.reqInWindow = 0
		s.dmReqInWindow = 0
	}
	s.reqInWindow++
	if s.reqInWindow > s.gateway.opts.MaxReqPerMinute {
		metrics.ObservePrimalWSRequest("request", "rate_limited", 0)
		_ = s.sendFrame([]any{"NOTICE", subID, "rate limit exceeded"})
		_ = s.sendFrame([]any{"EOSE", subID})
		return false
	}
	if isDirectMessagesRequest(filters) {
		s.dmReqInWindow++
		if s.dmReqInWindow > s.gateway.opts.MaxDMReqPerMinute {
			metrics.ObservePrimalWSRequest("get_directmsgs", "rate_limited", 0)
			_ = s.sendFrame([]any{"NOTICE", subID, "dm rate limit exceeded"})
			_ = s.sendFrame([]any{"EOSE", subID})
			return false
		}
	}
	return true
}

func (s *wsConnSession) reserveSubscription(subID string) bool {
	s.mu.Lock()
	if len(s.subscriptions) >= s.gateway.opts.MaxSubscriptions {
		s.mu.Unlock()
		metrics.ObservePrimalWSRequest("request", "subscription_limit", 0)
		_ = s.sendFrame([]any{"NOTICE", subID, "too many subscriptions"})
		_ = s.sendFrame([]any{"EOSE", subID})
		return false
	}
	s.subscriptions[subID] = struct{}{}
	s.mu.Unlock()
	return true
}

func (s *wsConnSession) addLiveSubscription(liveSub dmLiveSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dmLiveSubscriptions[liveSub.SubID] = liveSub
}

func (s *wsConnSession) dropSubscription(subID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subID)
	delete(s.dmLiveSubscriptions, subID)
}

func (s *wsConnSession) startDMLiveCountLoop() {
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				err := failure.FromPanic(recovered)
				class := failure.ClassifyError(err)
				s.gateway.log.Error("compat_ws_live_counts_panic_recovered", "failure_class", class.Class, "failure_reason", class.Reason, "error", err)
			}
		}()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-s.done:
				return
			case <-ticker.C:
				for _, sub := range s.liveSubscriptionsSnapshot() {
					frame, err := s.gateway.resolveDMCountFrame(s.requestCtx, sub)
					if err != nil {
						continue
					}
					if err := s.sendFrame(frame); err != nil {
						return
					}
				}
			}
		}
	}()
}

func (s *wsConnSession) liveSubscriptionsSnapshot() []dmLiveSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	liveSubs := make([]dmLiveSubscription, 0, len(s.dmLiveSubscriptions))
	for _, sub := range s.dmLiveSubscriptions {
		liveSubs = append(liveSubs, sub)
	}
	return liveSubs
}
