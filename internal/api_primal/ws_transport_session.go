package api_primal

import (
	"context"
	"errors"
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
	subscriptions       map[string]struct{}
	dmLiveSubscriptions map[string]dmLiveSubscription
	notifySubscriptions map[string]notifyLiveSubscription
	compression         bool
	windowStarted       time.Time
	reqInWindow         int
	dmReqInWindow       int
	broadcastInWindow   int
	done                chan struct{}
	writeQueue          chan wsQueuedFrame
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
		notifySubscriptions: make(map[string]notifyLiveSubscription),
		windowStarted:       time.Now().UTC(),
		done:                make(chan struct{}),
		writeQueue:          make(chan wsQueuedFrame, wsWriteQueueSize),
	}
}

func (s *wsConnSession) run() {
	defer close(s.done)
	go s.writePump()
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
	raw, err := encodeFrame(frame)
	if err != nil {
		return err
	}
	return s.enqueue(wsQueuedFrame{payload: raw})
}

func (s *wsConnSession) sendBinary(payload []byte) error {
	return s.enqueue(wsQueuedFrame{binary: true, payload: payload})
}

func (s *wsConnSession) enqueue(item wsQueuedFrame) error {
	select {
	case <-s.done:
		return errSessionClosed
	default:
	}
	select {
	case s.writeQueue <- item:
		return nil
	case <-s.done:
		return errSessionClosed
	default:
	}
	// Queue is full: wait briefly so a legitimate burst (a large thread_view)
	// still lands, then disconnect a client that cannot keep up.
	timer := time.NewTimer(wsWriteQueueWait)
	defer timer.Stop()
	select {
	case s.writeQueue <- item:
		return nil
	case <-timer.C:
		s.teardown()
		return errWriteBackpressure
	case <-s.done:
		return errSessionClosed
	}
}

// writePump is the only goroutine that writes to the socket. Concurrent
// WriteMessage calls are undefined in gorilla/websocket; a slow client
// previously blocked the request goroutine (and the DM live-count loop)
// on the write mutex for the duration of the TCP send.
func (s *wsConnSession) writePump() {
	defer s.teardown()
	for {
		select {
		case item := <-s.writeQueue:
			_ = s.conn.SetWriteDeadline(time.Now().Add(wsWriteDeadline))
			messageType := websocket.TextMessage
			if item.binary {
				messageType = websocket.BinaryMessage
			}
			if err := s.conn.WriteMessage(messageType, item.payload); err != nil {
				return
			}
		case <-s.done:
			return
		}
	}
}

// teardown closes the underlying connection exactly once. Closing unblocks the
// blocking ReadMessage in run(), which then returns and drives full cleanup.
func (s *wsConnSession) teardown() {
	s.closeOnce.Do(func() {
		if s.conn != nil {
			_ = s.conn.Close()
		}
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

	if s.gateway.opts.Dialect == WSDialectPrimalV1 && filtersUseKindOrAuthor(filters) {
		_ = s.sendFrame([]any{"NOTICE", subID, "kinds or authors filter is not supported"})
		s.teardown()
		return errors.New("kinds or authors filter is not supported")
	}

	cacheName, kwargs := cacheCallFromFilters(filters)
	if s.gateway.opts.Dialect == WSDialectPrimalV1 && cacheName == "set_primal_protocol" {
		if strings.EqualFold(strings.TrimSpace(stringValue(kwargs["compression"])), "zlib") {
			s.mu.Lock()
			s.compression = true
			s.mu.Unlock()
		}
		_ = s.sendFrame([]any{"EOSE", subID})
		s.dropSubscription(subID)
		return nil
	}
	if s.gateway.opts.Dialect == WSDialectPrimalV1 && (cacheName == "notification_counts" || cacheName == "notification_counts_2") {
		pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
		if pubkey == "" {
			pubkey = strings.TrimSpace(stringValue(kwargs["user_pubkey"]))
		}
		if err := validatePubkeyHex(pubkey); err != nil {
			_ = s.sendFrame([]any{"NOTICE", subID, "error"})
			_ = s.sendFrame([]any{"EOSE", subID})
			s.dropSubscription(subID)
			return nil
		}
		version := 1
		if cacheName == "notification_counts_2" {
			version = 2
		}
		s.addNotifySubscription(notifyLiveSubscription{SubID: subID, Pubkey: strings.ToLower(pubkey), Version: version})
		_ = s.sendFrame([]any{"EOSE", subID})
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
	if err := s.sendResponseFrames(subID, frames); err != nil {
		return err
	}
	if liveSub, ok := parseDMLiveSubscription(subID, filters); ok {
		s.addLiveSubscription(liveSub)
		return nil
	}
	s.dropSubscription(subID)
	return nil
}

func (s *wsConnSession) sendResponseFrames(subID string, frames [][]any) error {
	if !s.compressionOn() {
		for _, frame := range frames {
			if err := s.sendFrame(frame); err != nil {
				return err
			}
		}
		return s.sendFrame([]any{"EOSE", subID})
	}
	events := make([]any, 0, len(frames))
	for _, frame := range frames {
		kind, _ := frame[0].(string)
		if kind == "EVENT" && len(frame) >= 3 {
			events = append(events, frame[2])
			continue
		}
		if err := s.sendFrame(frame); err != nil {
			return err
		}
	}
	if len(events) > 0 {
		payload, err := encodeZlibEvents(subID, events)
		if err != nil {
			return err
		}
		if err := s.sendBinary(payload); err != nil {
			return err
		}
	}
	return s.sendFrame([]any{"EOSE", subID})
}

func (s *wsConnSession) compressionOn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compression
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
		s.broadcastInWindow = 0
	}
	s.reqInWindow++
	if s.reqInWindow > s.gateway.opts.MaxReqPerMinute {
		metrics.ObservePrimalWSRequest("request", "rate_limited", 0)
		_ = s.sendFrame([]any{"NOTICE", subID, "rate limit exceeded"})
		_ = s.sendFrame([]any{"EOSE", subID})
		return false
	}
	if s.gateway.opts.Dialect == WSDialectPrimalV1 && isBroadcastRequest(filters) {
		s.broadcastInWindow++
		if s.broadcastInWindow > s.gateway.opts.MaxBroadcastPerMinute {
			metrics.ObservePrimalWSRequest("broadcast_events", "rate_limited", 0)
			_ = s.sendFrame([]any{"NOTICE", subID, "rate limit exceeded"})
			_ = s.sendFrame([]any{"EOSE", subID})
			return false
		}
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

func (s *wsConnSession) addNotifySubscription(sub notifyLiveSubscription) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notifySubscriptions[sub.SubID] = sub
}

func (s *wsConnSession) dropSubscription(subID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscriptions, subID)
	delete(s.dmLiveSubscriptions, subID)
	delete(s.notifySubscriptions, subID)
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
				for _, sub := range s.notifySubscriptionsSnapshot() {
					frame, ok := s.gateway.notificationCountFrame(s.requestCtx, sub)
					if !ok {
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

func (s *wsConnSession) notifySubscriptionsSnapshot() []notifyLiveSubscription {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]notifyLiveSubscription, 0, len(s.notifySubscriptions))
	for _, sub := range s.notifySubscriptions {
		out = append(out, sub)
	}
	return out
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
