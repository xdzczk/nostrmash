package metrics

import "github.com/prometheus/client_golang/prometheus"

// relayWindowSnapshotAgeSeconds mirrors the trust-snapshot staleness gauge
// (see trustSnapshotActiveAgeSeconds in trust.go) for the homepage's
// relay-window snapshots. Fed from the actual "home_window_24h" row via
// derivation.Handlers.RelayWindowSnapshotAge on every worker tick — success
// or failure — so it reflects true data staleness rather than the worker's
// own in-memory notion of "last successful run". Without this gauge, a
// stuck or silently-failing refresh loop has no signal beyond an old
// computed_at value on /api/v1/discovery/home that nobody is watching.
var relayWindowSnapshotAgeSeconds prometheus.Gauge

func registerDiscoveryMetrics() {
	relayWindowSnapshotAgeSeconds = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_relay_window_snapshot_age_seconds",
			Help: "Age in seconds since the homepage's home_window_24h relay-window snapshot was last computed. Drives the \"Updated Xd ago\" freshness label on / and /api/v1/discovery/home.",
		},
	)

	registry.MustRegister(relayWindowSnapshotAgeSeconds)
}

// SetRelayWindowSnapshotAge reports the current age of the homepage's
// relay-window snapshot. See RunRelayWindowSnapshotsLoop in
// internal/worker/runtime/background_loops.go for the call site.
func SetRelayWindowSnapshotAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	relayWindowSnapshotAgeSeconds.Set(seconds)
}
