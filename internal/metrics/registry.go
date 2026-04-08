package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registry          = prometheus.NewRegistry()
	registerMetricsMu sync.Mutex
	metricsRegistered bool

	apiRequestsTotal             *prometheus.CounterVec
	apiRequestDuration           *prometheus.HistogramVec
	dbOperationDuration          *prometheus.HistogramVec
	dbOperationErrorsTotal       *prometheus.CounterVec
	ingestEventsTotal            *prometheus.CounterVec
	ingestSnapshot               *prometheus.GaugeVec
	ingestRelayPriorityDecisions *prometheus.CounterVec

	primalWSConnections     prometheus.Gauge
	primalWSFramesTotal     *prometheus.CounterVec
	primalWSRequestsTotal   *prometheus.CounterVec
	primalWSRequestDuration *prometheus.HistogramVec

	ingestCheckpointFreshnessSeconds *prometheus.GaugeVec
	lookupLocalTotal                 *prometheus.CounterVec
	lookupFallbackTotal              *prometheus.CounterVec
	lookupFallbackLatency            *prometheus.HistogramVec
	publicResponseCacheLookupsTotal  *prometheus.CounterVec
)

func Handler() http.Handler {
	ensureRegistered()
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func ensureRegistered() {
	registerMetricsMu.Lock()
	defer registerMetricsMu.Unlock()
	if metricsRegistered {
		return
	}

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	registerCoreMetrics()
	registerQueueMetrics()
	registerWorkerMetrics()
	registerTrustMetrics()
	registerBuildMetrics()
	registerStorageMetrics()

	metricsRegistered = true
}

func registerCoreMetrics() {
	apiRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_api_requests_total",
			Help: "Total number of API requests.",
		},
		[]string{"method", "path_template", "status_code"},
	)
	apiRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_api_request_duration_seconds",
			Help:    "API request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path_template"},
	)
	dbOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_db_operation_duration_seconds",
			Help:    "Latency of critical DB operations by operation and result.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "result"},
	)
	dbOperationErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_db_operation_errors_total",
			Help: "Total critical DB operation errors by operation.",
		},
		[]string{"operation"},
	)
	ingestEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_ingest_events_total",
			Help: "Total ingest events by outcome.",
		},
		[]string{"outcome"},
	)
	ingestSnapshot = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_snapshot_total",
			Help: "Latest ingest snapshot counters.",
		},
		[]string{"outcome"},
	)
	ingestRelayPriorityDecisions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_ingest_relay_priority_decisions_total",
			Help: "Relay prioritization decisions by source outcome.",
		},
		[]string{"outcome"},
	)
	primalWSConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_primal_ws_connections",
			Help: "Current number of active Primal compatibility websocket connections.",
		},
	)
	primalWSFramesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_primal_ws_frames_total",
			Help: "Total number of inbound websocket frames by type.",
		},
		[]string{"frame_type"},
	)
	primalWSRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_primal_ws_requests_total",
			Help: "Total number of processed Primal websocket requests by kind and outcome.",
		},
		[]string{"request_kind", "outcome"},
	)
	primalWSRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_primal_ws_request_duration_seconds",
			Help:    "Latency of Primal websocket request handling by request kind.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"request_kind"},
	)
	ingestCheckpointFreshnessSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_checkpoint_freshness_seconds",
			Help: "Age in seconds since the latest durable ingest checkpoint update.",
		},
		[]string{"mode", "filter_group"},
	)
	lookupLocalTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_lookup_local_total",
			Help: "Local lookup outcomes for fallback-enabled surfaces.",
		},
		[]string{"surface", "result"},
	)
	lookupFallbackTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_lookup_fallback_total",
			Help: "Fallback lookup outcomes by entity and result.",
		},
		[]string{"entity", "result"},
	)
	lookupFallbackLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_lookup_fallback_latency_seconds",
			Help:    "Fallback lookup latency in seconds by entity.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"entity"},
	)
	publicResponseCacheLookupsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_public_response_cache_lookups_total",
			Help: "Public endpoint response cache lookups by family, endpoint, and result.",
		},
		[]string{"family", "endpoint", "result"},
	)

	registry.MustRegister(
		apiRequestsTotal,
		apiRequestDuration,
		dbOperationDuration,
		dbOperationErrorsTotal,
		ingestEventsTotal,
		ingestSnapshot,
		ingestRelayPriorityDecisions,
		primalWSConnections,
		primalWSFramesTotal,
		primalWSRequestsTotal,
		primalWSRequestDuration,
		ingestCheckpointFreshnessSeconds,
		lookupLocalTotal,
		lookupFallbackTotal,
		lookupFallbackLatency,
		publicResponseCacheLookupsTotal,
	)
}

func ObserveAPI(method, pathTemplate string, statusCode int, d time.Duration) {
	ensureRegistered()
	status := strconv.Itoa(statusCode)
	apiRequestsTotal.WithLabelValues(method, pathTemplate, status).Inc()
	apiRequestDuration.WithLabelValues(method, pathTemplate).Observe(d.Seconds())
}

func IncIngestOutcome(outcome string) {
	ensureRegistered()
	ingestEventsTotal.WithLabelValues(outcome).Inc()
}

func ObserveDBOperation(operation, result string, d time.Duration) {
	ensureRegistered()
	dbOperationDuration.WithLabelValues(operation, result).Observe(d.Seconds())
	if result == "error" {
		dbOperationErrorsTotal.WithLabelValues(operation).Inc()
	}
}

func SetIngestSnapshot(valid, duplicate, invalid uint64) {
	ensureRegistered()
	ingestSnapshot.WithLabelValues("valid").Set(float64(valid))
	ingestSnapshot.WithLabelValues("duplicate").Set(float64(duplicate))
	ingestSnapshot.WithLabelValues("invalid").Set(float64(invalid))
}

func IncIngestRelayPriorityDecision(outcome string) {
	ensureRegistered()
	ingestRelayPriorityDecisions.WithLabelValues(outcome).Inc()
}

func IncPrimalWSConnection() {
	ensureRegistered()
	primalWSConnections.Inc()
}

func DecPrimalWSConnection() {
	ensureRegistered()
	primalWSConnections.Dec()
}

func IncPrimalWSFrame(frameType string) {
	ensureRegistered()
	primalWSFramesTotal.WithLabelValues(frameType).Inc()
}

func ObservePrimalWSRequest(requestKind, outcome string, d time.Duration) {
	ensureRegistered()
	primalWSRequestsTotal.WithLabelValues(requestKind, outcome).Inc()
	primalWSRequestDuration.WithLabelValues(requestKind).Observe(d.Seconds())
}

func SetIngestCheckpointFreshness(mode, filterGroup string, seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	ingestCheckpointFreshnessSeconds.WithLabelValues(mode, filterGroup).Set(seconds)
}

func ObserveLookupLocal(surface string, hit bool) {
	ensureRegistered()
	result := "miss"
	if hit {
		result = "hit"
	}
	lookupLocalTotal.WithLabelValues(surface, result).Inc()
}

func IncLookupFallbackAttempt(entity string) {
	ensureRegistered()
	lookupFallbackTotal.WithLabelValues(entity, "attempt").Inc()
}

func IncLookupFallbackResult(entity, result string) {
	ensureRegistered()
	lookupFallbackTotal.WithLabelValues(entity, result).Inc()
}

func IncLookupFallbackSuccess(entity string) {
	ensureRegistered()
	lookupFallbackTotal.WithLabelValues(entity, "hit").Inc()
}

func IncLookupFallbackMiss(entity string) {
	ensureRegistered()
	lookupFallbackTotal.WithLabelValues(entity, "miss").Inc()
}

func IncLookupFallbackFailure(entity string) {
	ensureRegistered()
	lookupFallbackTotal.WithLabelValues(entity, "error").Inc()
}

func ObserveLookupFallbackLatency(entity string, d time.Duration) {
	ensureRegistered()
	lookupFallbackLatency.WithLabelValues(entity).Observe(d.Seconds())
}

func ObservePublicResponseCacheLookup(family, endpoint string, hit bool) {
	ensureRegistered()
	result := "miss"
	if hit {
		result = "hit"
	}
	publicResponseCacheLookupsTotal.WithLabelValues(family, endpoint, result).Inc()
}
