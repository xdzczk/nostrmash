package query

import "context"

type quoteCountReader interface {
	QuoteCountsByEventIDs(context.Context, []string) (map[string]int64, error)
}

// QuoteCountsByEventIDs returns kind-1 q-tag quotes for each event.
// Readers without the projection return an unsupported-capability error.
func (s Service) QuoteCountsByEventIDs(ctx context.Context, ids []string) (map[string]int64, error) {
	if len(ids) == 0 {
		return map[string]int64{}, nil
	}
	reader, ok := quoteCountReaderFrom(s.reader)
	if !ok {
		return nil, unsupportedCapabilityError("quote counts")
	}
	return reader.QuoteCountsByEventIDs(ctx, ids)
}

func quoteCountReaderFrom(reader any) (quoteCountReader, bool) {
	if typed, ok := reader.(quoteCountReader); ok {
		return typed, true
	}
	adapter, ok := reader.(readModelReaderAdapter)
	if !ok {
		return nil, false
	}
	typed, ok := adapter.readModel.(quoteCountReader)
	return typed, ok
}
