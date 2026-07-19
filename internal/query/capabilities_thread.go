package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// threadSummaryCapability is readmodel-shaped; the Service maps to the query
// ThreadSummary DTO at the response edge via threadSummaryFromStore.
type threadSummaryCapability interface {
	GetThreadSummary(ctx context.Context, rootEventID string) (readmodel.ThreadSummaryProjection, error)
}

func adaptThreadCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(threadSummaryCapability); ok {
		caps.thread.summary = r
	}
}
