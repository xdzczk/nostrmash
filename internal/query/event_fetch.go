package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

func (s Service) GetEventActionCounts(ctx context.Context, eventID string) (ActionCounts, error) {
	return eventService{reader: s.reader, policy: s.fallbackPolicy()}.GetEventActionCounts(ctx, eventID)
}

func (s Service) GetEventByID(ctx context.Context, id string) (raw json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_by_id")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback, persister: s.fallbackEventPersister, policy: s.fallbackPolicy()}.GetEventByID(ctx, id)
}

func (s Service) GetEventBatch(ctx context.Context, ids []string) (out map[string]json.RawMessage, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_event_batch")
	defer func() { span.End(err) }()
	return eventService{reader: s.reader, fallback: s.fallback, persister: s.fallbackEventPersister, policy: s.fallbackPolicy()}.GetEventBatch(ctx, ids)
}

func (s Service) GetRelaysHealth(ctx context.Context) ([]model.IngestCheckpoint, error) {
	rows, err := s.reader.ListRelayHealth(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]model.IngestCheckpoint, 0, len(rows))
	for _, row := range rows {
		checkpoint := row
		checkpoint.UpdatedAt = checkpoint.UpdatedAt.UTC()
		if checkpoint.EOSESeenAt != nil {
			eoseSeenAt := checkpoint.EOSESeenAt.UTC()
			checkpoint.EOSESeenAt = &eoseSeenAt
		}
		out = append(out, checkpoint)
	}
	return out, nil
}
