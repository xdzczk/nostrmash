package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	hydrationRunsTotal        *prometheus.CounterVec
	hydrationDurationSeconds  prometheus.Histogram
	hydrationEventsFoundTotal prometheus.Counter
)

func registerHydrationMetrics() {
	hydrationRunsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_hydration_runs_total",
			Help: "On-demand account hydration runs by result. Bounded label set (success/partial/failed/skipped).",
		},
		[]string{"result"},
	)
	hydrationDurationSeconds = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "nostrmash_hydration_duration_seconds",
			Help:    "Duration of on-demand account hydration runs in seconds.",
			Buckets: []float64{0.5, 1, 2, 5, 10, 20, 30, 60, 120},
		},
	)
	hydrationEventsFoundTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "nostrmash_hydration_events_found_total",
			Help: "Total events fetched across all hydration runs.",
		},
	)
	registry.MustRegister(
		hydrationRunsTotal,
		hydrationDurationSeconds,
		hydrationEventsFoundTotal,
	)
}

// ObserveHydrationRun records the outcome and duration of a hydration run and
// the number of events it fetched.
func ObserveHydrationRun(result string, durationSeconds float64, eventsFound int) {
	ensureRegistered()
	hydrationRunsTotal.WithLabelValues(result).Inc()
	if durationSeconds > 0 {
		hydrationDurationSeconds.Observe(durationSeconds)
	}
	if eventsFound > 0 {
		hydrationEventsFoundTotal.Add(float64(eventsFound))
	}
}
