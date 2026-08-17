package api

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
)

func recordDiscoveryDegrade(ctx context.Context, surface, component string, err error, reasons *[]string) {
	if err == nil {
		return
	}
	logging.WithRequestID(ctx, apiErrLog).Warn(
		"api_enrichment_degraded",
		"surface", surface,
		"component", component,
		"error", err,
	)
	metrics.IncAPIPartialResponse(surface, component)
	if reasons != nil {
		*reasons = append(*reasons, component)
	}
}
