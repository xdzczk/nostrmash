package api_primal

import (
	"github.com/xdzczk/nostrmash/internal/query"
)

// EventReader accepts query-native or legacy store-backed readers.
type EventReader = any

// Handlers translates Primal-compatible requests/responses at the boundary only.
type Handlers struct {
	service      query.Service
	maxBatchSize int
}

type HandlersOptions struct {
	MaxBatchSize int
	QueryOptions query.ServiceOptions
}

func NewHandlers(reader EventReader, maxBatchSize int) (Handlers, error) {
	return NewHandlersWithOptions(reader, HandlersOptions{MaxBatchSize: maxBatchSize})
}

func NewHandlersWithOptions(reader EventReader, options HandlersOptions) (Handlers, error) {
	maxBatchSize := options.MaxBatchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 200
	}
	service, err := query.NewServiceWithOptions(reader, options.QueryOptions)
	if err != nil {
		return Handlers{}, err
	}
	return Handlers{
		service:      service,
		maxBatchSize: maxBatchSize,
	}, nil
}
