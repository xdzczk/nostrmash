package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	storageDatabaseBytes        prometheus.Gauge
	storageTableBytes           *prometheus.GaugeVec
	storageTableIndexBytes      *prometheus.GaugeVec
	storageTierBytes            *prometheus.GaugeVec
	storageTableRows            *prometheus.GaugeVec
	storagePressureRatio        prometheus.Gauge
	storagePressureLevel        prometheus.Gauge
	retentionPurgeRunsTotal     *prometheus.CounterVec
	retentionPurgedRowsTotal    *prometheus.CounterVec
	jobsRowsByStatusType        *prometheus.GaugeVec
	jobsOldestFinishedAgeByStat *prometheus.GaugeVec
)

func registerStorageMetrics() {
	storageDatabaseBytes = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_database_bytes",
			Help: "Current size in bytes of the active PostgreSQL database.",
		},
	)
	storageTableBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_table_bytes",
			Help: "Current size in bytes for tracked tables.",
		},
		[]string{"table"},
	)
	storageTableIndexBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_table_index_bytes",
			Help: "Current index size in bytes for tracked tables (pg_indexes_size).",
		},
		[]string{"table"},
	)
	storageTierBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_tier_bytes",
			Help: "Total bytes per storage tier (canonical/derived/operational). Bounded label set.",
		},
		[]string{"tier"},
	)
	storageTableRows = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_table_rows",
			Help: "Current row count for tracked tables.",
		},
		[]string{"table"},
	)
	storagePressureRatio = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_pressure_ratio",
			Help: "Database size as a fraction of the configured capacity budget (0..1+). 0 when capacity is unset.",
		},
	)
	storagePressureLevel = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_pressure_level",
			Help: "Storage pressure level: 0 normal, 1 warn, 2 aggressive, 3 disable_hydration, 4 pause_candidate.",
		},
	)
	retentionPurgeRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_retention_purge_runs_total",
			Help: "Retention purge runs by target and result.",
		},
		[]string{"target", "result"},
	)
	retentionPurgedRowsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_retention_purged_rows_total",
			Help: "Rows deleted by retention purges, by target table class.",
		},
		[]string{"target"},
	)
	jobsRowsByStatusType = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_jobs_rows",
			Help: "Current row count in the jobs queue by status and job_type. Cardinality is bounded by the known job-type enum (unknowns reported as job_type=\"other\").",
		},
		[]string{"status", "job_type"},
	)
	jobsOldestFinishedAgeByStat = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_jobs_oldest_finished_age_seconds",
			Help: "Age in seconds of the oldest terminal job by status. 0 when no terminal rows exist for the status.",
		},
		[]string{"status"},
	)

	registry.MustRegister(
		storageDatabaseBytes,
		storageTableBytes,
		storageTableIndexBytes,
		storageTierBytes,
		storageTableRows,
		storagePressureRatio,
		storagePressureLevel,
		retentionPurgeRunsTotal,
		retentionPurgedRowsTotal,
		jobsRowsByStatusType,
		jobsOldestFinishedAgeByStat,
	)
}

func SetStorageDatabaseBytes(bytes float64) {
	ensureRegistered()
	if bytes < 0 {
		bytes = 0
	}
	storageDatabaseBytes.Set(bytes)
}

func SetStorageTableBytes(table string, bytes float64) {
	ensureRegistered()
	if bytes < 0 {
		bytes = 0
	}
	storageTableBytes.WithLabelValues(table).Set(bytes)
}

func SetStorageTableIndexBytes(table string, bytes float64) {
	ensureRegistered()
	if bytes < 0 {
		bytes = 0
	}
	storageTableIndexBytes.WithLabelValues(table).Set(bytes)
}

func SetStorageTierBytes(tier string, bytes float64) {
	ensureRegistered()
	if bytes < 0 {
		bytes = 0
	}
	storageTierBytes.WithLabelValues(tier).Set(bytes)
}

func SetStorageTableRows(table string, rows float64) {
	ensureRegistered()
	if rows < 0 {
		rows = 0
	}
	storageTableRows.WithLabelValues(table).Set(rows)
}

// SetStoragePressure publishes the current pressure ratio (db_bytes/capacity)
// and discrete level. Level enum is bounded (0..4).
func SetStoragePressure(ratio float64, level int) {
	ensureRegistered()
	if ratio < 0 {
		ratio = 0
	}
	storagePressureRatio.Set(ratio)
	storagePressureLevel.Set(float64(level))
}

func IncRetentionPurgeRun(target, result string) {
	ensureRegistered()
	retentionPurgeRunsTotal.WithLabelValues(target, result).Inc()
}

func AddRetentionPurgedRows(target string, rows int64) {
	ensureRegistered()
	if rows <= 0 {
		return
	}
	retentionPurgedRowsTotal.WithLabelValues(target).Add(float64(rows))
}

// SetJobsRows publishes the current row count for one (status, job_type)
// bucket in the jobs queue. Callers are expected to keep the job_type label
// space bounded (use "other" for anything not in the known enum).
func SetJobsRows(status, jobType string, count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	jobsRowsByStatusType.WithLabelValues(status, jobType).Set(count)
}

// ResetJobsRows zeroes any previously published (status, job_type) buckets so
// that disappearing combinations (e.g. a job_type whose backlog drained) do
// not stick at their last value forever.
func ResetJobsRows() {
	ensureRegistered()
	jobsRowsByStatusType.Reset()
}

// SetJobsOldestFinishedAgeSeconds publishes the age in seconds of the oldest
// terminal (succeeded/dead) jobs row by status. Used to detect a stuck or
// disabled retention loop.
func SetJobsOldestFinishedAgeSeconds(status string, seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	jobsOldestFinishedAgeByStat.WithLabelValues(status).Set(seconds)
}
