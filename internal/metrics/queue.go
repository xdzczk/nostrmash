package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	queueOperationDuration       *prometheus.HistogramVec
	queueOperationErrors         *prometheus.CounterVec
	workerQueueBacklogAgeSeconds prometheus.Gauge

	staleRecoveryRecoveredTotal    *prometheus.CounterVec
	staleRecoveryDeadLetteredTotal *prometheus.CounterVec
	staleRecoveryDuration          *prometheus.HistogramVec
)

func registerQueueMetrics() {
	queueOperationDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_queue_operation_duration_seconds",
			Help:    "Latency of queue/job operations by operation and result.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation", "result"},
	)
	queueOperationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_queue_operation_errors_total",
			Help: "Total queue/job operation errors by operation.",
		},
		[]string{"operation"},
	)
	workerQueueBacklogAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_worker_queue_backlog_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest pending worker queue job.",
		},
	)
	staleRecoveryRecoveredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_stale_recovery_recovered_total",
			Help: "Stale running jobs recovered back to pending by queue recovery loops.",
		},
		[]string{"worker_pool"},
	)
	staleRecoveryDeadLetteredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_stale_recovery_dead_lettered_total",
			Help: "Stale running jobs dead-lettered by queue recovery loops.",
		},
		[]string{"worker_pool"},
	)
	staleRecoveryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_worker_stale_recovery_duration_seconds",
			Help:    "Duration of stale running queue recovery operations.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"worker_pool", "result"},
	)

	registry.MustRegister(
		queueOperationDuration,
		queueOperationErrors,
		workerQueueBacklogAgeSeconds,
		staleRecoveryRecoveredTotal,
		staleRecoveryDeadLetteredTotal,
		staleRecoveryDuration,
	)
}

func ObserveQueueOperation(operation, result string, d time.Duration) {
	ensureRegistered()
	queueOperationDuration.WithLabelValues(operation, result).Observe(d.Seconds())
	if result == "error" {
		queueOperationErrors.WithLabelValues(operation).Inc()
	}
}

func SetWorkerQueueBacklogOldestPendingAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	workerQueueBacklogAgeSeconds.Set(seconds)
}

func AddStaleRecoveryRecovered(workerPool string, count int) {
	ensureRegistered()
	if count <= 0 {
		return
	}
	staleRecoveryRecoveredTotal.WithLabelValues(workerPool).Add(float64(count))
}

func AddStaleRecoveryDeadLettered(workerPool string, count int) {
	ensureRegistered()
	if count <= 0 {
		return
	}
	staleRecoveryDeadLetteredTotal.WithLabelValues(workerPool).Add(float64(count))
}

func ObserveStaleRecoveryDuration(workerPool, result string, d time.Duration) {
	ensureRegistered()
	staleRecoveryDuration.WithLabelValues(workerPool, result).Observe(d.Seconds())
}
