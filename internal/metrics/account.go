package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	accountStatesTotal           *prometheus.GaugeVec
	accountStateTransitionsTotal *prometheus.CounterVec
)

func registerAccountMetrics() {
	accountStatesTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "nostrmash_account_states_total",
			Help: "Number of accounts in each lifecycle state. Bounded label set (8 states).",
		},
		[]string{"state"},
	)
	accountStateTransitionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_account_state_transitions_total",
			Help: "Account lifecycle state transitions by destination state. Bounded label set.",
		},
		[]string{"to_state"},
	)
	registry.MustRegister(
		accountStatesTotal,
		accountStateTransitionsTotal,
	)
}

// SetAccountStateCount publishes the current count of accounts in a state.
func SetAccountStateCount(state string, count float64) {
	ensureRegistered()
	if count < 0 {
		count = 0
	}
	accountStatesTotal.WithLabelValues(state).Set(count)
}

// IncAccountStateTransition records one account lifecycle transition into the
// given destination state.
func IncAccountStateTransition(toState string) {
	ensureRegistered()
	accountStateTransitionsTotal.WithLabelValues(toState).Inc()
}
