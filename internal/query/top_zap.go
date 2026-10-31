package query

import (
	"context"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type topZapAdReader interface {
	TopZapAdsByEventIDs(context.Context, []string) (map[string]readmodel.TopZapAd, error)
}

// TopZapAdsByEventIDs returns the largest zap on each event.
// Readers without the projection return an unsupported-capability error.
func (s Service) TopZapAdsByEventIDs(ctx context.Context, ids []string) (map[string]readmodel.TopZapAd, error) {
	if len(ids) == 0 {
		return map[string]readmodel.TopZapAd{}, nil
	}
	reader, ok := topZapAdReaderFrom(s.reader)
	if !ok {
		return nil, unsupportedCapabilityError("top zaps")
	}
	return reader.TopZapAdsByEventIDs(ctx, ids)
}

func topZapAdReaderFrom(reader any) (topZapAdReader, bool) {
	if typed, ok := reader.(topZapAdReader); ok {
		return typed, true
	}
	adapter, ok := reader.(readModelReaderAdapter)
	if !ok {
		return nil, false
	}
	typed, ok := adapter.readModel.(topZapAdReader)
	return typed, ok
}
