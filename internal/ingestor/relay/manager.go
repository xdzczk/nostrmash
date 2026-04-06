package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/store/failure"
)

type relayRuntime struct {
	status Status
}

// MessageHandler consumes relay payloads delivered by a live connection.
type MessageHandler func(ctx context.Context, relayURL string, payload []byte) error

// Manager owns relay connection lifecycle state and reconnect loops.
type Manager struct {
	log        *slog.Logger
	connector  Connector
	cfg        Config
	handler    MessageHandler
	statusSink RelayStatusSink

	nowFn   func() time.Time
	sleepFn func(context.Context, time.Duration) bool

	mu     sync.RWMutex
	relays map[string]*relayRuntime
}

// NewManager validates relay config and creates a lifecycle manager.
func NewManager(cfg Config, connector Connector, handler MessageHandler, log *slog.Logger) (*Manager, error) {
	if connector == nil {
		return nil, fmt.Errorf("connector is required")
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 10 * time.Second
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 1 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.InitialBackoff > cfg.MaxBackoff {
		return nil, fmt.Errorf("initial backoff must be <= max backoff")
	}
	if err := validateRelayLifecycleConfig(cfg); err != nil {
		return nil, err
	}
	cfg.Relays = mustNormalizeURLs(cfg.Relays)
	cfg.Allowlist = mustNormalizeURLs(cfg.Allowlist)
	cfg.DisabledRelays = mustNormalizeURLs(cfg.DisabledRelays)

	nowFn := time.Now
	m := &Manager{
		log:        log,
		connector:  connector,
		cfg:        cfg,
		handler:    handler,
		statusSink: cfg.StatusSink,
		nowFn:      nowFn,
		sleepFn:    sleepContext,
		relays:     make(map[string]*relayRuntime, len(cfg.Relays)),
	}

	disabledSet := make(map[string]struct{}, len(cfg.DisabledRelays))
	for _, relayURL := range cfg.DisabledRelays {
		disabledSet[relayURL] = struct{}{}
	}
	for _, relayURL := range cfg.Relays {
		state := StateConnecting
		if _, disabled := disabledSet[relayURL]; disabled {
			state = StateDisabled
		}
		m.relays[relayURL] = &relayRuntime{
			status: Status{
				URL:   relayURL,
				State: state,
				Since: nowFn(),
			},
		}
	}

	return m, nil
}

// Start begins lifecycle loops for enabled relays and blocks until ctx is done.
func (m *Manager) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for relayURL, runtime := range m.relays {
		if runtime.status.State == StateDisabled {
			continue
		}
		wg.Add(1)
		go func(relayURL string) {
			defer wg.Done()
			m.runRelay(ctx, relayURL)
		}(relayURL)
	}

	<-ctx.Done()
	wg.Wait()
}

// Snapshot returns a copy of current relay lifecycle statuses.
func (m *Manager) Snapshot() map[string]Status {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]Status, len(m.relays))
	for relayURL, runtime := range m.relays {
		out[relayURL] = runtime.status
	}
	return out
}

// MarkProgress updates the relay's liveness timestamp and can recover lagging -> healthy.
func (m *Manager) MarkProgress(relayURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.relays[relayURL]
	if !ok {
		return
	}
	runtime.status.LastProgress = m.nowFn()
	if runtime.status.State == StateLagging {
		m.transitionLocked(runtime, StateHealthy, "progress")
	}
}

func (m *Manager) runRelay(ctx context.Context, relayURL string) {
	backoff := newBoundedBackoff(m.cfg.InitialBackoff, m.cfg.MaxBackoff)

	for {
		if err := ctx.Err(); err != nil {
			return
		}

		m.setConnecting(relayURL)
		m.reportStatus(ctx, relayURL, StateConnecting, "")

		connectCtx, cancel := context.WithTimeout(ctx, m.cfg.ConnectTimeout)
		conn, err := m.connector.Connect(connectCtx, relayURL)
		cancel()
		if err != nil {
			m.setState(relayURL, StateErrored, err.Error(), 0, "connect_failed")
			m.reportStatus(ctx, relayURL, StateErrored, err.Error())
			wait := backoff.Next()
			m.setState(relayURL, StateBackingOff, err.Error(), wait, "reconnect_backoff")
			m.reportStatus(ctx, relayURL, StateBackingOff, err.Error())
			if ok := m.sleepFn(ctx, wait); !ok {
				return
			}
			continue
		}

		backoff.Reset()
		m.setConnected(relayURL)
		m.reportStatus(ctx, relayURL, StateHealthy, "")
		if !m.monitorConnection(ctx, relayURL, conn, backoff) {
			return
		}
	}
}

func (m *Manager) monitorConnection(ctx context.Context, relayURL string, conn Connection, backoff *boundedBackoff) bool {
	defer conn.Close()

	var ticker *time.Ticker
	if m.cfg.LagThreshold > 0 {
		checkEvery := m.cfg.LagThreshold / 2
		if checkEvery < 50*time.Millisecond {
			checkEvery = 50 * time.Millisecond
		}
		ticker = time.NewTicker(checkEvery)
		defer ticker.Stop()
	}

	messagesCh := conn.Messages()

	for {
		select {
		case <-ctx.Done():
			return false
		case payload, ok := <-messagesCh:
			if !ok {
				messagesCh = nil
				continue
			}
			m.MarkProgress(relayURL)
			if m.handler != nil {
				if err := m.safeHandleRelayMessage(ctx, relayURL, payload); err != nil {
					m.log.Warn(
						"relay_message_handler_failed",
						"relay_url", relayURL,
						"error", err,
					)
				}
			}
		case err, ok := <-conn.Done():
			if !ok {
				err = errors.New("connection closed")
			}
			if err != nil {
				m.setState(relayURL, StateErrored, err.Error(), 0, "connection_lost")
				m.reportStatus(ctx, relayURL, StateErrored, err.Error())
			} else {
				m.setState(relayURL, StateErrored, "connection closed", 0, "connection_closed")
				m.reportStatus(ctx, relayURL, StateErrored, "connection closed")
			}
			wait := backoff.Next()
			m.setState(relayURL, StateBackingOff, "connection_lost", wait, "reconnect_backoff")
			m.reportStatus(ctx, relayURL, StateBackingOff, "connection_lost")
			if ok := m.sleepFn(ctx, wait); !ok {
				return false
			}
			return true
		case <-tickerC(ticker):
			m.checkLagging(relayURL)
		}
	}
}

func (m *Manager) safeHandleRelayMessage(ctx context.Context, relayURL string, payload []byte) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = failure.FromPanic(recovered)
			class := failure.ClassifyError(err)
			m.log.Error(
				"relay_message_handler_panic_recovered",
				"relay_url", relayURL,
				"failure_class", class.Class,
				"failure_reason", class.Reason,
				"error", err,
			)
		}
	}()
	return m.handler(ctx, relayURL, payload)
}

func (m *Manager) checkLagging(relayURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.relays[relayURL]
	if !ok {
		return
	}
	if runtime.status.State != StateHealthy {
		return
	}
	if runtime.status.LastProgress.IsZero() {
		return
	}
	if m.nowFn().Sub(runtime.status.LastProgress) > m.cfg.LagThreshold {
		m.transitionLocked(runtime, StateLagging, "lag_threshold_exceeded")
	}
}

func (m *Manager) setConnected(relayURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.relays[relayURL]
	if !ok {
		return
	}
	runtime.status.LastError = ""
	runtime.status.Backoff = 0
	runtime.status.LastProgress = m.nowFn()
	m.transitionLocked(runtime, StateHealthy, "connected")
}

func (m *Manager) setConnecting(relayURL string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.relays[relayURL]
	if !ok {
		return
	}
	runtime.status.Attempts++
	runtime.status.Backoff = 0
	m.transitionLocked(runtime, StateConnecting, "dial")
}

func (m *Manager) setState(relayURL string, state State, lastErr string, backoff time.Duration, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime, ok := m.relays[relayURL]
	if !ok {
		return
	}
	runtime.status.LastError = strings.TrimSpace(lastErr)
	runtime.status.Backoff = backoff
	m.transitionLocked(runtime, state, reason)
}

func (m *Manager) transitionLocked(runtime *relayRuntime, next State, reason string) {
	prev := runtime.status.State
	if prev == next {
		return
	}
	runtime.status.State = next
	runtime.status.Since = m.nowFn()

	m.log.Info(
		"relay_state_transition",
		"relay_url", runtime.status.URL,
		"from_state", prev,
		"to_state", next,
		"reason", reason,
		"attempts", runtime.status.Attempts,
		"backoff", runtime.status.Backoff.String(),
		"last_error", runtime.status.LastError,
	)
}

func validateRelayLifecycleConfig(cfg Config) error {
	allow := make(map[string]struct{}, len(cfg.Allowlist))
	for _, relayURL := range cfg.Allowlist {
		normalized, err := normalizeRelayURL(relayURL)
		if err != nil {
			return fmt.Errorf("invalid allowlisted relay %q: %w", relayURL, err)
		}
		allow[normalized] = struct{}{}
	}
	if len(cfg.Relays) > 0 && len(allow) == 0 {
		return fmt.Errorf("allowlist is required when relays are configured")
	}
	for _, relayURL := range cfg.Relays {
		normalized, err := normalizeRelayURL(relayURL)
		if err != nil {
			return fmt.Errorf("invalid relay %q: %w", relayURL, err)
		}
		if _, ok := allow[normalized]; !ok {
			return fmt.Errorf("relay %q is not in allowlist", relayURL)
		}
	}
	for _, relayURL := range cfg.DisabledRelays {
		normalized, err := normalizeRelayURL(relayURL)
		if err != nil {
			return fmt.Errorf("invalid disabled relay %q: %w", relayURL, err)
		}
		if _, ok := allow[normalized]; !ok {
			return fmt.Errorf("disabled relay %q is not in allowlist", relayURL)
		}
	}
	return nil
}

func mustNormalizeURLs(urls []string) []string {
	if len(urls) == 0 {
		return nil
	}
	out := make([]string, 0, len(urls))
	for _, raw := range urls {
		normalized, err := normalizeRelayURL(raw)
		if err != nil {
			continue
		}
		out = append(out, normalized)
	}
	return out
}

func normalizeRelayURL(raw string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return "", err
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("host is required")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return "", fmt.Errorf("scheme must be ws or wss")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("relay url must not contain user/query/fragment")
	}

	path := strings.TrimSpace(parsed.EscapedPath())
	if path == "/" {
		path = ""
	}
	return fmt.Sprintf("%s://%s%s", scheme, strings.ToLower(parsed.Host), path), nil
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func tickerC(t *time.Ticker) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

func (m *Manager) reportStatus(ctx context.Context, relayURL string, state State, lastError string) {
	if m.statusSink == nil {
		return
	}
	if err := m.statusSink.SetRelayStatus(ctx, relayURL, state, lastError); err != nil {
		m.log.Warn(
			"relay_status_persist_failed",
			"relay_url", relayURL,
			"state", state,
			"error", err,
		)
	}
}
