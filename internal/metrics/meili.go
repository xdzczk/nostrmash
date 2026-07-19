package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	meiliSyncTotal              *prometheus.CounterVec
	meiliSyncDuration           *prometheus.HistogramVec
	meiliSearchTotal            *prometheus.CounterVec
	meiliSearchDur              *prometheus.HistogramVec
	meiliPendingBacklog         prometheus.Gauge
	meiliPendingOldestAgeSecond prometheus.Gauge
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
	meiliPendingBacklog = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_pending_meilisearch_syncs",
			Help: "Current depth of the pending_meilisearch_syncs queue awaiting indexing.",
		},
	)
	meiliPendingOldestAgeSecond = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_pending_meilisearch_sync_oldest_age_seconds",
			Help: "Age in seconds of the oldest pending Meilisearch sync (search index lag SLO signal).",
		},
	)
	registry.MustRegister(
		meiliSyncTotal,
		meiliSyncDuration,
		meiliSearchTotal,
		meiliSearchDur,
		meiliPendingBacklog,
		meiliPendingOldestAgeSecond,
	)
}

// SetMeilisearchSyncBacklog publishes the pending Meilisearch sync queue depth
// and the age of its oldest entry, driving the search-index-lag SLO alert.
func SetMeilisearchSyncBacklog(backlog int64, oldestAgeSeconds float64) {
	ensureRegistered()
	if backlog < 0 {
		backlog = 0
	}
	if oldestAgeSeconds < 0 {
		oldestAgeSeconds = 0
	}
	meiliPendingBacklog.Set(float64(backlog))
	meiliPendingOldestAgeSecond.Set(oldestAgeSeconds)
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
