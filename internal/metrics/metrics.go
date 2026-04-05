package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	apiRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_api_requests_total",
			Help: "Total number of API requests.",
		},
		[]string{"method", "path", "status_code"},
	)
	apiRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_api_request_duration_seconds",
			Help:    "API request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
	ingestEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_ingest_events_total",
			Help: "Total ingest events by outcome.",
		},
		[]string{"outcome"},
	)
	ingestSnapshot = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_snapshot_total",
			Help: "Latest ingest snapshot counters.",
		},
		[]string{"outcome"},
	)
	workerJobsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_jobs_total",
			Help: "Worker job executions by type and outcome.",
		},
		[]string{"job_type", "outcome"},
	)
	primalWSConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_primal_ws_connections",
			Help: "Current number of active Primal compatibility websocket connections.",
		},
	)
	primalWSFramesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_primal_ws_frames_total",
			Help: "Total number of inbound websocket frames by type.",
		},
		[]string{"frame_type"},
	)
	primalWSRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_primal_ws_requests_total",
			Help: "Total number of processed Primal websocket requests by kind and outcome.",
		},
		[]string{"request_kind", "outcome"},
	)
	primalWSRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_primal_ws_request_duration_seconds",
			Help:    "Latency of Primal websocket request handling by request kind.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"request_kind"},
	)
)

func Handler() http.Handler {
	return promhttp.Handler()
}

func ObserveAPI(method, path string, statusCode int, d time.Duration) {
	status := strconv.Itoa(statusCode)
	apiRequestsTotal.WithLabelValues(method, path, status).Inc()
	apiRequestDuration.WithLabelValues(method, path).Observe(d.Seconds())
}

func IncIngestOutcome(outcome string) {
	ingestEventsTotal.WithLabelValues(outcome).Inc()
}

func SetIngestSnapshot(valid, duplicate, invalid uint64) {
	ingestSnapshot.WithLabelValues("valid").Set(float64(valid))
	ingestSnapshot.WithLabelValues("duplicate").Set(float64(duplicate))
	ingestSnapshot.WithLabelValues("invalid").Set(float64(invalid))
}

func IncWorkerJob(jobType, outcome string) {
	workerJobsTotal.WithLabelValues(jobType, outcome).Inc()
}

func IncPrimalWSConnection() {
	primalWSConnections.Inc()
}

func DecPrimalWSConnection() {
	primalWSConnections.Dec()
}

func IncPrimalWSFrame(frameType string) {
	primalWSFramesTotal.WithLabelValues(frameType).Inc()
}

func ObservePrimalWSRequest(requestKind, outcome string, d time.Duration) {
	primalWSRequestsTotal.WithLabelValues(requestKind, outcome).Inc()
	primalWSRequestDuration.WithLabelValues(requestKind).Observe(d.Seconds())
}
