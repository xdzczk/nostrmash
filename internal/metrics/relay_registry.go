package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	relayRegistryByState     *prometheus.GaugeVec
	relayDesiredActiveCount  prometheus.Gauge
	relayDiscoveryCandidates *prometheus.CounterVec
	relayProbeResults        *prometheus.CounterVec
	relayProbeLatency        *prometheus.HistogramVec
	relayAdmissionChanges    *prometheus.CounterVec
	relayReconcilerActions   *prometheus.CounterVec
)

func registerRelayRegistryMetrics() {
	relayRegistryByState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_registry_by_state",
			Help: "Number of relays in the registry by admission state.",
		},
		[]string{"admission_state"},
	)
	relayDesiredActiveCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_desired_active_count",
			Help: "Number of relays in the current desired active set.",
		},
	)
	relayDiscoveryCandidates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_discovery_candidates_total",
			Help: "Discovery candidate writes per run by outcome (upserted, refreshed, failed).",
		},
		[]string{"outcome"},
	)
	relayProbeResults = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_probe_results_total",
			Help: "Relay probe results by status.",
		},
		[]string{"status"},
	)
	relayProbeLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "nostrmash_relay_probe_latency_seconds",
			Help:    "Relay probe connect latency in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"phase"},
	)
	relayAdmissionChanges = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_admission_changes_total",
			Help: "Relay admission state changes by direction.",
		},
		[]string{"direction"},
	)
	relayReconcilerActions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_relay_reconciler_actions_total",
			Help: "Reconciler relay add/remove actions.",
		},
		[]string{"action"},
	)

	registry.MustRegister(
		relayRegistryByState,
		relayDesiredActiveCount,
		relayDiscoveryCandidates,
		relayProbeResults,
		relayProbeLatency,
		relayAdmissionChanges,
		relayReconcilerActions,
	)
}

func SetRelayRegistryStateCount(state string, count float64) {
	ensureRegistered()
	relayRegistryByState.WithLabelValues(state).Set(count)
}

func SetRelayDesiredActiveCount(count float64) {
	ensureRegistered()
	relayDesiredActiveCount.Set(count)
}

func IncRelayDiscoveryCandidates(outcome string) {
	ensureRegistered()
	relayDiscoveryCandidates.WithLabelValues(outcome).Inc()
}

func IncRelayProbeResult(status string) {
	ensureRegistered()
	relayProbeResults.WithLabelValues(status).Inc()
}

func ObserveRelayProbeLatency(phase string, seconds float64) {
	ensureRegistered()
	relayProbeLatency.WithLabelValues(phase).Observe(seconds)
}

func IncRelayAdmissionChange(direction string) {
	ensureRegistered()
	relayAdmissionChanges.WithLabelValues(direction).Inc()
}

func IncRelayReconcilerAction(action string) {
	ensureRegistered()
	relayReconcilerActions.WithLabelValues(action).Inc()
}
