package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	authorMetadataDiscoveryCyclesTotal  *prometheus.CounterVec
	authorMetadataDiscoveryFetchedTotal prometheus.Counter
)

func registerAuthorMetadataMetrics() {
	authorMetadataDiscoveryCyclesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_author_metadata_discovery_cycles_total",
			Help: "Total author metadata discovery cycles by outcome.",
		},
		[]string{"outcome"},
	)
	authorMetadataDiscoveryFetchedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "nostrmash_author_metadata_discovery_fetched_total",
			Help: "Total author metadata successfully fetched from relays.",
		},
	)
	registry.MustRegister(
		authorMetadataDiscoveryCyclesTotal,
		authorMetadataDiscoveryFetchedTotal,
	)
}

func IncAuthorMetadataDiscoveryOutcome(outcome string) {
	ensureRegistered()
	authorMetadataDiscoveryCyclesTotal.WithLabelValues(outcome).Inc()
}

func AddAuthorMetadataDiscoveryFetched(count float64) {
	ensureRegistered()
	if count <= 0 {
		return
	}
	authorMetadataDiscoveryFetchedTotal.Add(count)
}
