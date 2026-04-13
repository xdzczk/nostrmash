package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	meiliSyncTotal    *prometheus.CounterVec
	meiliSyncDuration *prometheus.HistogramVec
	meiliSearchTotal  *prometheus.CounterVec
	meiliSearchDur    *prometheus.HistogramVec
)

func registerMeiliMetrics() {
	meiliSyncTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_meili_sync_total",
			Help: "Total Meilisearch sync operations by source kind and outcome.",
		},
		[]string{"kind", "outcome"},
	)
	meiliSyncDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_meili_sync_duration_seconds",
			Help:    "Latency of Meilisearch sync operations by source kind.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"kind"},
	)
	meiliSearchTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_meili_search_total",
			Help: "Total Meilisearch search operations by index and outcome.",
		},
		[]string{"index", "outcome"},
	)
	meiliSearchDur = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_meili_search_duration_seconds",
			Help:    "Latency of Meilisearch search operations by index.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"index"},
	)
	registry.MustRegister(
		meiliSyncTotal,
		meiliSyncDuration,
		meiliSearchTotal,
		meiliSearchDur,
	)
}

func ObserveMeiliSync(kind, outcome string, d time.Duration) {
	ensureRegistered()
	meiliSyncTotal.WithLabelValues(kind, outcome).Inc()
	meiliSyncDuration.WithLabelValues(kind).Observe(d.Seconds())
}

func ObserveMeiliSearch(index, outcome string, d time.Duration) {
	ensureRegistered()
	meiliSearchTotal.WithLabelValues(index, outcome).Inc()
	meiliSearchDur.WithLabelValues(index).Observe(d.Seconds())
}
