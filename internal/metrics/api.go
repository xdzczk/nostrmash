package metrics

import "github.com/prometheus/client_golang/prometheus"

var apiPartialResponsesTotal *prometheus.CounterVec

func registerAPIMetrics() {
	apiPartialResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_api_partial_response_total",
			Help: "API responses degraded to partial results because an enrichment dependency failed, by surface and component.",
		},
		[]string{"surface", "component"},
	)
	registry.MustRegister(apiPartialResponsesTotal)
}

// IncAPIPartialResponse records that a response surface returned partial data
// because the named enrichment component failed for a non-not-found reason.
func IncAPIPartialResponse(surface, component string) {
	ensureRegistered()
	apiPartialResponsesTotal.WithLabelValues(surface, component).Inc()
}
