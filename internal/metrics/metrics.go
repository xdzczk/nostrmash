package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	registry          = prometheus.NewRegistry()
	registerMetricsMu sync.Mutex
	metricsRegistered bool

	apiRequestsTotal                 *prometheus.CounterVec
	apiRequestDuration               *prometheus.HistogramVec
	dbOperationDuration              *prometheus.HistogramVec
	dbOperationErrorsTotal           *prometheus.CounterVec
	queueOperationDuration           *prometheus.HistogramVec
	queueOperationErrors             *prometheus.CounterVec
	workerJobExecDuration            *prometheus.HistogramVec
	ingestEventsTotal                *prometheus.CounterVec
	ingestSnapshot                   *prometheus.GaugeVec
	workerJobsTotal                  *prometheus.CounterVec
	primalWSConnections              prometheus.Gauge
	primalWSFramesTotal              *prometheus.CounterVec
	primalWSRequestsTotal            *prometheus.CounterVec
	primalWSRequestDuration          *prometheus.HistogramVec
	ingestCheckpointFreshnessSeconds *prometheus.GaugeVec
	workerQueueBacklogAgeSeconds     prometheus.Gauge
	rebuildActiveAgeSeconds          prometheus.Gauge
	rebuildRunsActive                prometheus.Gauge
	buildInfo                        *prometheus.GaugeVec
	deploymentInfo                   *prometheus.GaugeVec

	dbPoolStatsMu        sync.Mutex
	dbPoolStatsCollector prometheus.Collector
)

func Handler() http.Handler {
	ensureRegistered()
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}

func RegisterDBPool(pool *pgxpool.Pool) {
	if pool == nil {
		return
	}
	ensureRegistered()

	dbPoolStatsMu.Lock()
	defer dbPoolStatsMu.Unlock()
	if dbPoolStatsCollector != nil {
		return
	}

	dbPoolStatsCollector = newDBPoolCollector(realDBPoolProvider{pool: pool})
	registry.MustRegister(dbPoolStatsCollector)
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
	workerJobExecDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_worker_job_execution_duration_seconds",
			Help:    "Worker job execution latency by job type and outcome.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"job_type", "outcome"},
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
	workerJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_jobs_total",
			Help: "Worker job executions by type and outcome.",
		},
		[]string{"job_type", "outcome"},
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
	workerQueueBacklogAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_worker_queue_backlog_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest pending worker queue job.",
		},
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
	buildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_build_info",
			Help: "Build metadata for the running binary.",
		},
		[]string{"binary_role", "version", "commit", "build_time"},
	)
	deploymentInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_deployment_info",
			Help: "Deployment identity metadata for the running binary.",
		},
		[]string{"binary_role", "service_name", "environment"},
	)

	registry.MustRegister(
		apiRequestsTotal,
		apiRequestDuration,
		dbOperationDuration,
		dbOperationErrorsTotal,
		queueOperationDuration,
		queueOperationErrors,
		workerJobExecDuration,
		ingestEventsTotal,
		ingestSnapshot,
		workerJobsTotal,
		primalWSConnections,
		primalWSFramesTotal,
		primalWSRequestsTotal,
		primalWSRequestDuration,
		ingestCheckpointFreshnessSeconds,
		workerQueueBacklogAgeSeconds,
		rebuildActiveAgeSeconds,
		rebuildRunsActive,
		buildInfo,
		deploymentInfo,
	)
	metricsRegistered = true
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

func ObserveQueueOperation(operation, result string, d time.Duration) {
	ensureRegistered()
	queueOperationDuration.WithLabelValues(operation, result).Observe(d.Seconds())
	if result == "error" {
		queueOperationErrors.WithLabelValues(operation).Inc()
	}
}

func ObserveWorkerJobExecution(jobType, outcome string, d time.Duration) {
	ensureRegistered()
	workerJobExecDuration.WithLabelValues(jobType, outcome).Observe(d.Seconds())
}

func RegisterBuildInfo(binaryRole, version, commit, buildTime string) {
	ensureRegistered()
	buildInfo.WithLabelValues(binaryRole, version, commit, buildTime).Set(1)
}

func RegisterDeploymentInfo(binaryRole, serviceName, environment string) {
	ensureRegistered()
	deploymentInfo.WithLabelValues(binaryRole, serviceName, environment).Set(1)
}

func SetIngestSnapshot(valid, duplicate, invalid uint64) {
	ensureRegistered()
	ingestSnapshot.WithLabelValues("valid").Set(float64(valid))
	ingestSnapshot.WithLabelValues("duplicate").Set(float64(duplicate))
	ingestSnapshot.WithLabelValues("invalid").Set(float64(invalid))
}

func IncWorkerJob(jobType, outcome string) {
	ensureRegistered()
	workerJobsTotal.WithLabelValues(jobType, outcome).Inc()
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

func SetWorkerQueueBacklogOldestPendingAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	workerQueueBacklogAgeSeconds.Set(seconds)
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

type dbPoolCollector struct {
	pool dbPoolStatsProvider

	openConnections         *prometheus.Desc
	inUseConnections        *prometheus.Desc
	idleConnections         *prometheus.Desc
	maxOpenConnections      *prometheus.Desc
	maxOpenUsageRatio       *prometheus.Desc
	acquireCountTotal       *prometheus.Desc
	acquireDurationSeconds  *prometheus.Desc
	waitCountTotal          *prometheus.Desc
	canceledAcquireTotal    *prometheus.Desc
	constructingConnections *prometheus.Desc
}

func newDBPoolCollector(pool dbPoolStatsProvider) *dbPoolCollector {
	return &dbPoolCollector{
		pool:                    pool,
		openConnections:         prometheus.NewDesc("nostrmash_db_pool_open_connections", "Current number of open DB pool connections.", nil, nil),
		inUseConnections:        prometheus.NewDesc("nostrmash_db_pool_in_use_connections", "Current number of DB pool connections in use.", nil, nil),
		idleConnections:         prometheus.NewDesc("nostrmash_db_pool_idle_connections", "Current number of idle DB pool connections.", nil, nil),
		maxOpenConnections:      prometheus.NewDesc("nostrmash_db_pool_max_open_connections", "Configured maximum number of DB pool connections.", nil, nil),
		maxOpenUsageRatio:       prometheus.NewDesc("nostrmash_db_pool_max_open_usage_ratio", "Current in-use to max-open DB pool connection ratio.", nil, nil),
		acquireCountTotal:       prometheus.NewDesc("nostrmash_db_pool_acquire_count_total", "Total DB pool acquires.", nil, nil),
		acquireDurationSeconds:  prometheus.NewDesc("nostrmash_db_pool_acquire_duration_seconds_total", "Total seconds spent acquiring DB pool connections.", nil, nil),
		waitCountTotal:          prometheus.NewDesc("nostrmash_db_pool_wait_count_total", "Total DB pool acquires that waited due to saturation.", nil, nil),
		canceledAcquireTotal:    prometheus.NewDesc("nostrmash_db_pool_canceled_acquire_count_total", "Total canceled DB pool acquires.", nil, nil),
		constructingConnections: prometheus.NewDesc("nostrmash_db_pool_constructing_connections", "Current number of DB pool connections being constructed.", nil, nil),
	}
}

func (c *dbPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.openConnections
	ch <- c.inUseConnections
	ch <- c.idleConnections
	ch <- c.maxOpenConnections
	ch <- c.maxOpenUsageRatio
	ch <- c.acquireCountTotal
	ch <- c.acquireDurationSeconds
	ch <- c.waitCountTotal
	ch <- c.canceledAcquireTotal
	ch <- c.constructingConnections
}

type dbPoolStatsProvider interface {
	Stat() dbPoolStat
}

type dbPoolStat interface {
	TotalConns() int32
	AcquiredConns() int32
	IdleConns() int32
	MaxConns() int32
	AcquireCount() int64
	AcquireDuration() time.Duration
	EmptyAcquireCount() int64
	CanceledAcquireCount() int64
	ConstructingConns() int32
}

type realDBPoolProvider struct {
	pool *pgxpool.Pool
}

func (p realDBPoolProvider) Stat() dbPoolStat {
	return realDBPoolStat{stat: p.pool.Stat()}
}

type realDBPoolStat struct {
	stat *pgxpool.Stat
}

func (s realDBPoolStat) TotalConns() int32 {
	return s.stat.TotalConns()
}

func (s realDBPoolStat) AcquiredConns() int32 {
	return s.stat.AcquiredConns()
}

func (s realDBPoolStat) IdleConns() int32 {
	return s.stat.IdleConns()
}

func (s realDBPoolStat) MaxConns() int32 {
	return s.stat.MaxConns()
}

func (s realDBPoolStat) AcquireCount() int64 {
	return s.stat.AcquireCount()
}

func (s realDBPoolStat) AcquireDuration() time.Duration {
	return s.stat.AcquireDuration()
}

func (s realDBPoolStat) EmptyAcquireCount() int64 {
	return s.stat.EmptyAcquireCount()
}

func (s realDBPoolStat) CanceledAcquireCount() int64 {
	return s.stat.CanceledAcquireCount()
}

func (s realDBPoolStat) ConstructingConns() int32 {
	return s.stat.ConstructingConns()
}

func (c *dbPoolCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.pool.Stat()
	open := float64(stats.TotalConns())
	inUse := float64(stats.AcquiredConns())
	idle := float64(stats.IdleConns())
	maxOpen := float64(stats.MaxConns())

	waitCount := stats.EmptyAcquireCount()
	acquireCount := stats.AcquireCount()
	acquireDurationSeconds := stats.AcquireDuration().Seconds()

	ratio := 0.0
	if maxOpen > 0 {
		ratio = inUse / maxOpen
	}

	ch <- prometheus.MustNewConstMetric(c.openConnections, prometheus.GaugeValue, open)
	ch <- prometheus.MustNewConstMetric(c.inUseConnections, prometheus.GaugeValue, inUse)
	ch <- prometheus.MustNewConstMetric(c.idleConnections, prometheus.GaugeValue, idle)
	ch <- prometheus.MustNewConstMetric(c.maxOpenConnections, prometheus.GaugeValue, maxOpen)
	ch <- prometheus.MustNewConstMetric(c.maxOpenUsageRatio, prometheus.GaugeValue, ratio)
	ch <- prometheus.MustNewConstMetric(c.acquireCountTotal, prometheus.CounterValue, float64(acquireCount))
	ch <- prometheus.MustNewConstMetric(c.acquireDurationSeconds, prometheus.CounterValue, acquireDurationSeconds)
	ch <- prometheus.MustNewConstMetric(c.waitCountTotal, prometheus.CounterValue, float64(waitCount))
	ch <- prometheus.MustNewConstMetric(c.canceledAcquireTotal, prometheus.CounterValue, float64(stats.CanceledAcquireCount()))
	ch <- prometheus.MustNewConstMetric(c.constructingConnections, prometheus.GaugeValue, float64(stats.ConstructingConns()))
}
