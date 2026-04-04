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
