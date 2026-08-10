package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	trustQueueBacklogAgeSeconds    prometheus.Gauge
	trustRunsActive                prometheus.Gauge
	trustRunActiveOldestAgeSeconds prometheus.Gauge
	trustSnapshotActiveAgeSeconds  prometheus.Gauge
	trustScoreRowsPublishedTotal   prometheus.Counter
	trustNeighborhoodMembers       *prometheus.GaugeVec
	trustPhaseDuration             *prometheus.HistogramVec
	trustFetchFrontierCount        *prometheus.GaugeVec
	trustFetchCyclesTotal          *prometheus.CounterVec
	trustFetchPubkeysTotal         *prometheus.CounterVec
	trustFetchPubkeysSelectedTotal prometheus.Counter
)

func registerTrustMetrics() {
	trustQueueBacklogAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_queue_backlog_oldest_pending_age_seconds",
			Help: "Age in seconds of the oldest pending trust queue job.",
		},
	)
	trustRunsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_runs_active",
			Help: "Current number of active trust runs.",
		},
	)
	trustRunActiveOldestAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_active_oldest_run_age_seconds",
			Help: "Age in seconds of the oldest active trust run.",
		},
	)
	trustSnapshotActiveAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_active_snapshot_age_seconds",
			Help: "Age in seconds since the most recently succeeded trust run finished.",
		},
	)
	trustScoreRowsPublishedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "nostrmash_trust_score_rows_published_total",
			Help: "Total trust score rows published during promote phases.",
		},
	)
	trustNeighborhoodMembers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_neighborhood_members_total",
			Help: "Seeded trust neighborhood member counts from the latest neighborhoods phase.",
		},
		[]string{"seed"},
	)
	trustPhaseDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_trust_phase_duration_seconds",
			Help:    "Trust run phase duration by phase and outcome.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"phase", "outcome"},
	)
	trustFetchFrontierCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_trust_fetch_frontier_count",
			Help: "Current trust pubkey frontier entries by state.",
		},
		[]string{"state"},
	)
	trustFetchCyclesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_trust_fetch_cycles_total",
			Help: "Trust fetch scheduler cycles by outcome.",
		},
		[]string{"outcome"},
	)
	trustFetchPubkeysTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_trust_fetch_pubkeys_total",
			Help: "Trust fetch pubkey outcomes.",
		},
		[]string{"outcome"},
	)
	trustFetchPubkeysSelectedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "nostrmash_trust_fetch_pubkeys_selected_total",
			Help: "Total number of pubkeys selected for trust fetch cycles.",
		},
	)

	registry.MustRegister(
		trustQueueBacklogAgeSeconds,
		trustRunsActive,
		trustRunActiveOldestAgeSeconds,
		trustSnapshotActiveAgeSeconds,
		trustScoreRowsPublishedTotal,
		trustNeighborhoodMembers,
		trustPhaseDuration,
		trustFetchFrontierCount,
		trustFetchCyclesTotal,
		trustFetchPubkeysTotal,
		trustFetchPubkeysSelectedTotal,
	)
}

func SetTrustQueueBacklogOldestPendingAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	trustQueueBacklogAgeSeconds.Set(seconds)
}

func SetTrustRunsActive(count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	trustRunsActive.Set(count)
}

func SetTrustActiveOldestRunAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	trustRunActiveOldestAgeSeconds.Set(seconds)
}

func SetTrustActiveSnapshotAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	trustSnapshotActiveAgeSeconds.Set(seconds)
}

func AddTrustScoreRowsPublished(rows int64) {
	ensureRegistered()
	if rows <= 0 {
		return
	}
	trustScoreRowsPublishedTotal.Add(float64(rows))
}

func SetTrustNeighborhoodMembers(seed string, count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	trustNeighborhoodMembers.WithLabelValues(seed).Set(count)
}

func ObserveTrustPhaseDuration(phase, outcome string, d time.Duration) {
	ensureRegistered()
	trustPhaseDuration.WithLabelValues(phase, outcome).Observe(d.Seconds())
}

func SetTrustFetchFrontierCount(state string, count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	trustFetchFrontierCount.WithLabelValues(state).Set(count)
}

func IncTrustFetchCycleOutcome(outcome string) {
	ensureRegistered()
	trustFetchCyclesTotal.WithLabelValues(outcome).Inc()
}

func IncTrustFetchPubkeyOutcome(outcome string) {
	ensureRegistered()
	trustFetchPubkeysTotal.WithLabelValues(outcome).Inc()
}

func AddTrustFetchPubkeysSelected(count float64) {
	ensureRegistered()
	if count <= 0 {
		return
	}
	trustFetchPubkeysSelectedTotal.Add(count)
}
