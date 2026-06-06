package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	ingestGateDecisionsTotal *prometheus.CounterVec
	ingestTrustedSetSize     prometheus.Gauge
	ingestTrustedSetLoaded   prometheus.Gauge
	ingestTrustedSetAge      prometheus.Gauge
)

func registerIngestGateMetrics() {
	ingestGateDecisionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_ingest_gate_decisions_total",
			Help: "Trust-gate ingest decisions by event kind and decision. Labels are normalized to a fixed, bounded set.",
		},
		[]string{"kind", "decision"},
	)
	ingestTrustedSetSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_trusted_set_size",
			Help: "Number of trusted-author pubkeys currently held by the in-memory ingest gate set.",
		},
	)
	ingestTrustedSetLoaded = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_trusted_set_loaded",
			Help: "Whether the in-memory trusted-author set has ever loaded successfully (1) or never (0).",
		},
	)
	ingestTrustedSetAge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "nostrmash_ingest_trusted_set_age_seconds",
			Help: "Age in seconds since the in-memory trusted-author set last refreshed successfully.",
		},
	)

	registry.MustRegister(
		ingestGateDecisionsTotal,
		ingestTrustedSetSize,
		ingestTrustedSetLoaded,
		ingestTrustedSetAge,
	)
}

// IncIngestGateDecision records a gate decision. Callers MUST pass normalized,
// bounded label values (see internal/ingestor/live gate logic) to keep
// cardinality fixed.
func IncIngestGateDecision(kind, decision string) {
	ensureRegistered()
	ingestGateDecisionsTotal.WithLabelValues(kind, decision).Inc()
}

func SetIngestTrustedSetSize(size int) {
	ensureRegistered()
	if size < 0 {
		size = 0
	}
	ingestTrustedSetSize.Set(float64(size))
}

func SetIngestTrustedSetLoaded(loaded bool) {
	ensureRegistered()
	if loaded {
		ingestTrustedSetLoaded.Set(1)
		return
	}
	ingestTrustedSetLoaded.Set(0)
}

func SetIngestTrustedSetAge(seconds float64) {
	ensureRegistered()
	if seconds < 0 {
		seconds = 0
	}
	ingestTrustedSetAge.Set(seconds)
}
