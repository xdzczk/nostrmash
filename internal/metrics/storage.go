package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	storageDatabaseBytes     prometheus.Gauge
	storageTableBytes        *prometheus.GaugeVec
	storageTableRows         *prometheus.GaugeVec
	retentionPurgeRunsTotal  *prometheus.CounterVec
	retentionPurgedRowsTotal *prometheus.CounterVec
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
	storageTableRows = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_storage_table_rows",
			Help: "Current row count for tracked tables.",
		},
		[]string{"table"},
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

	registry.MustRegister(
		storageDatabaseBytes,
		storageTableBytes,
		storageTableRows,
		retentionPurgeRunsTotal,
		retentionPurgedRowsTotal,
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

func SetStorageTableRows(table string, rows float64) {
	ensureRegistered()
	if rows < 0 {
		rows = 0
	}
	storageTableRows.WithLabelValues(table).Set(rows)
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
