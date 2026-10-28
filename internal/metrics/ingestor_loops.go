package metrics

import "github.com/prometheus/client_golang/prometheus"

var ingestorBackgroundLoopPanicsTotal *prometheus.CounterVec

func registerIngestorLoopMetrics() {
	ingestorBackgroundLoopPanicsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nostrmash_ingestor_background_loop_panics_total",
			Help: "Panics recovered in ingestor background loops. Live ingest keeps running.",
		},
		[]string{"loop"},
	)
	registry.MustRegister(ingestorBackgroundLoopPanicsTotal)
}

func IncIngestorBackgroundLoopPanic(loop string) {
	ensureRegistered()
	if loop == "" {
		loop = "unknown"
	}
	ingestorBackgroundLoopPanicsTotal.WithLabelValues(loop).Inc()
}
