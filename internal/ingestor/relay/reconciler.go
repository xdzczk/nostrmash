package relay

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
)

// DesiredSetProvider returns the current desired active relay URLs.
type DesiredSetProvider interface {
	GetDesiredActiveRelays(ctx context.Context) ([]string, error)
}

// Reconciler dynamically adds and removes relay connections to match a desired set,
// without requiring full restart of the relay manager.
type Reconciler struct {
	log           *slog.Logger
	connector     Connector
	handler       MessageHandler
	statusSink    RelayStatusSink
	provider      DesiredSetProvider
	pollInterval  time.Duration
	drainTimeout  time.Duration
	connectConfig Config

	mu           sync.Mutex
	managers     map[string]*managedRelay
	fallbackURLs []string
}

type managedRelay struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// ReconcilerConfig controls reconciliation behavior.
type ReconcilerConfig struct {
	PollInterval  time.Duration
	DrainTimeout  time.Duration
	FallbackURLs  []string
	ConnectConfig Config
}

// NewReconciler creates a reconciler that polls a desired set provider and
// adjusts relay connections accordingly.
func NewReconciler(
	log *slog.Logger,
	connector Connector,
	handler MessageHandler,
	statusSink RelayStatusSink,
	provider DesiredSetProvider,
	cfg ReconcilerConfig,
) *Reconciler {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 30 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 10 * time.Second
	}
	return &Reconciler{
		log:           log,
		connector:     connector,
		handler:       handler,
		statusSink:    statusSink,
		provider:      provider,
		pollInterval:  cfg.PollInterval,
		drainTimeout:  cfg.DrainTimeout,
		connectConfig: cfg.ConnectConfig,
		managers:      make(map[string]*managedRelay),
		fallbackURLs:  cfg.FallbackURLs,
	}
}

// Run starts the reconciliation loop and blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) {
	r.reconcileOnce(ctx)

	ticker := time.NewTicker(r.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.drainAll()
			return
		case <-ticker.C:
			r.reconcileOnce(ctx)
		}
	}
}

func (r *Reconciler) reconcileOnce(ctx context.Context) {
	desired, err := r.provider.GetDesiredActiveRelays(ctx)
	if err != nil {
		r.log.Warn("relay_reconciler_fetch_desired_failed", "error", err)
		return
	}

	if len(desired) == 0 {
		if len(r.fallbackURLs) > 0 {
			r.log.Info("relay_reconciler_using_fallback", "fallback_count", len(r.fallbackURLs))
			desired = r.fallbackURLs
		} else {
			r.log.Warn("relay_reconciler_empty_desired_set")
			return
		}
	}

	desiredSet := make(map[string]struct{}, len(desired))
	for _, u := range desired {
		normalized, err := normalizeRelayURL(u)
		if err != nil {
			r.log.Warn("relay_reconciler_normalize_failed", "url", u, "error", err)
			continue
		}
		desiredSet[normalized] = struct{}{}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var added, removed int

	for url := range desiredSet {
		if _, exists := r.managers[url]; !exists {
			r.startRelayLocked(ctx, url)
			added++
			metrics.IncRelayReconcilerAction("add")
		}
	}

	for url, managed := range r.managers {
		if _, wanted := desiredSet[url]; !wanted {
			managed.cancel()
			delete(r.managers, url)
			removed++
			metrics.IncRelayReconcilerAction("remove")
		}
	}

	if added > 0 || removed > 0 {
		r.log.Info("relay_reconciler_applied",
			"added", added,
			"removed", removed,
			"active", len(r.managers),
		)
	}
}

func (r *Reconciler) startRelayLocked(parentCtx context.Context, relayURL string) {
	relayCtx, cancel := context.WithCancel(parentCtx)
	done := make(chan struct{})
	r.managers[relayURL] = &managedRelay{cancel: cancel, done: done}

	go func() {
		defer close(done)
		r.runSingleRelay(relayCtx, relayURL)
	}()
}

func (r *Reconciler) runSingleRelay(ctx context.Context, relayURL string) {
	backoff := newBoundedBackoff(r.connectConfig.InitialBackoff, r.connectConfig.MaxBackoff)

	for {
		if err := ctx.Err(); err != nil {
			r.reportStatus(ctx, relayURL, StateDisconnected, "reconciler_drain")
			return
		}

		r.reportStatus(ctx, relayURL, StateConnecting, "")

		connectCtx, cancel := context.WithTimeout(ctx, r.connectConfig.ConnectTimeout)
		conn, err := r.connector.Connect(connectCtx, relayURL)
		cancel()
		if err != nil {
			r.reportStatus(ctx, relayURL, StateErrored, err.Error())
			wait := backoff.Next()
			if ok := sleepContext(ctx, wait); !ok {
				return
			}
			continue
		}

		backoff.Reset()
		r.reportStatus(ctx, relayURL, StateHealthy, "")
		r.streamMessages(ctx, relayURL, conn, backoff)

		if ctx.Err() != nil {
			return
		}
	}
}

func (r *Reconciler) streamMessages(ctx context.Context, relayURL string, conn Connection, backoff *boundedBackoff) {
	defer conn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case payload, ok := <-conn.Messages():
			if !ok {
				r.reportStatus(ctx, relayURL, StateErrored, "connection_closed")
				wait := backoff.Next()
				sleepContext(ctx, wait)
				return
			}
			if r.handler != nil {
				if err := r.handler(ctx, relayURL, payload); err != nil {
					r.log.Warn("relay_reconciler_handler_failed",
						"relay_url", relayURL,
						"error", err,
					)
				}
			}
		case err, ok := <-conn.Done():
			if !ok || err != nil {
				errStr := "connection_closed"
				if err != nil {
					errStr = err.Error()
				}
				r.reportStatus(ctx, relayURL, StateErrored, errStr)
			}
			wait := backoff.Next()
			sleepContext(ctx, wait)
			return
		}
	}
}

func (r *Reconciler) reportStatus(ctx context.Context, relayURL string, state State, lastError string) {
	if r.statusSink == nil {
		return
	}
	if err := r.statusSink.SetRelayStatus(ctx, relayURL, state, lastError); err != nil {
		r.log.Warn("relay_reconciler_status_persist_failed",
			"relay_url", relayURL,
			"state", state,
			"error", err,
		)
	}
}

func (r *Reconciler) drainAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for url, managed := range r.managers {
		managed.cancel()
		delete(r.managers, url)
	}
}

// Snapshot returns currently managed relay URLs.
func (r *Reconciler) Snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.managers))
	for url := range r.managers {
		out = append(out, url)
	}
	return out
}
