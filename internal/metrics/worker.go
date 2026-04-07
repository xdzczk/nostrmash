package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	workerJobExecDuration   *prometheus.HistogramVec
	workerJobsTotal         *prometheus.CounterVec
	rebuildActiveAgeSeconds prometheus.Gauge
	rebuildRunsActive       prometheus.Gauge
)

func registerWorkerMetrics() {
	workerJobExecDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_worker_job_execution_duration_seconds",
			Help:    "Worker job execution latency by job type and outcome.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"job_type", "outcome"},
	)
	workerJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_jobs_total",
			Help: "Worker job executions by type and outcome.",
		},
		[]string{"job_type", "outcome"},
	)
	rebuildActiveAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_rebuild_active_oldest_age_seconds",
			Help: "Age in seconds of the oldest active projection rebuild run.",
		},
	)
	rebuildRunsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_rebuild_runs_active",
			Help: "Current number of active projection rebuild runs.",
		},
	)

	registry.MustRegister(
		workerJobExecDuration,
		workerJobsTotal,
		rebuildActiveAgeSeconds,
		rebuildRunsActive,
	)
}

func ObserveWorkerJobExecution(jobType, outcome string, d time.Duration) {
	ensureRegistered()
	workerJobExecDuration.WithLabelValues(jobType, outcome).Observe(d.Seconds())
}

func IncWorkerJob(jobType, outcome string) {
	ensureRegistered()
	workerJobsTotal.WithLabelValues(jobType, outcome).Inc()
}

func SetRebuildActiveOldestAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	rebuildActiveAgeSeconds.Set(seconds)
}

func SetRebuildRunsActive(count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	rebuildRunsActive.Set(count)
}
