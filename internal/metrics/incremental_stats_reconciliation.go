package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	incrementalStatsReconciliationRunDuration  *prometheus.HistogramVec
	incrementalStatsReconciliationSampledTotal *prometheus.CounterVec
	incrementalStatsReconciliationMismatches   *prometheus.CounterVec
	incrementalStatsReconciliationHeals        *prometheus.CounterVec
)

func registerIncrementalStatsReconciliationMetrics() {
	incrementalStatsReconciliationRunDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_worker_incremental_stats_reconciliation_run_duration_seconds",
			Help:    "Duration of the incremental author-stats reconciliation pass by result.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"result"},
	)
	incrementalStatsReconciliationSampledTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_incremental_stats_reconciliation_sampled_total",
			Help: "Pubkeys full-recomputed and compared against incrementally-maintained values by the reconciliation loop.",
		},
		[]string{"result"},
	)
	incrementalStatsReconciliationMismatches = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_incremental_stats_reconciliation_mismatches_total",
			Help: "Fields where an incrementally-maintained author/profile stat diverged from a full recompute, by projection and field.",
		},
		[]string{"projection", "field"},
	)

	incrementalStatsReconciliationHeals = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_worker_incremental_stats_reconciliation_heals_total",
			Help: "Drifted pubkey projections rebuilt by the reconciliation loop after a detected mismatch, by heal action and result.",
		},
		[]string{"action", "result"},
	)

	registry.MustRegister(
		incrementalStatsReconciliationRunDuration,
		incrementalStatsReconciliationSampledTotal,
		incrementalStatsReconciliationMismatches,
		incrementalStatsReconciliationHeals,
	)
}

// ObserveIncrementalStatsReconciliationRun records duration and outcome of
// one reconciliation pass, plus how many pubkeys it sampled.
func ObserveIncrementalStatsReconciliationRun(result string, sampled int, d time.Duration) {
	ensureRegistered()
	incrementalStatsReconciliationRunDuration.WithLabelValues(result).Observe(d.Seconds())
	if sampled > 0 {
		incrementalStatsReconciliationSampledTotal.WithLabelValues(result).Add(float64(sampled))
	}
}

// IncIncrementalStatsReconciliationMismatch records one detected divergence
// between an incrementally-maintained value and a fresh full recompute.
func IncIncrementalStatsReconciliationMismatch(projection, field string) {
	ensureRegistered()
	incrementalStatsReconciliationMismatches.WithLabelValues(projection, field).Inc()
}

// IncIncrementalStatsReconciliationHeal records one attempted self-heal
// rebuild of a drifted projection ("ok" or "error").
func IncIncrementalStatsReconciliationHeal(action, result string) {
	ensureRegistered()
	incrementalStatsReconciliationHeals.WithLabelValues(action, result).Inc()
}
