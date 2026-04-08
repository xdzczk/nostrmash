package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/store"
)

type threadSummaryCapability interface {
	GetThreadSummary(ctx context.Context, rootEventID string) (ThreadSummary, error)
}

type legacyThreadSummaryCapability interface {
	GetThreadSummary(ctx context.Context, rootEventID string) (store.ThreadSummaryProjection, error)
}

type legacyThreadSummaryAdapter struct {
	legacy legacyThreadSummaryCapability
}

func adaptThreadCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(threadSummaryCapability); ok {
		caps.thread.summary = r
		return
	}
	if legacy, ok := reader.(legacyThreadSummaryCapability); ok {
		caps.thread.summary = legacyThreadSummaryAdapter{legacy: legacy}
	}
}

func (a legacyThreadSummaryAdapter) GetThreadSummary(ctx context.Context, rootEventID string) (ThreadSummary, error) {
	row, err := a.legacy.GetThreadSummary(ctx, rootEventID)
	if err != nil {
		return ThreadSummary{}, err
	}
	return threadSummaryFromStore(row), nil
}
