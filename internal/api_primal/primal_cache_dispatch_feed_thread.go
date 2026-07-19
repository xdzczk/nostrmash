package api_primal

import (
	"context"
	"errors"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) cacheDispatchThreadView(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID, _ := kwargs["event_id"].(string)
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	maxDepth := toBoundedPositiveInt(kwargs["max_depth"], 100, 100)
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	cursorValue, err := optionalStringValue(kwargs["cursor"])
	if err != nil {
		return nil, errors.New("cursor is malformed")
	}
	cursor, err := decodeEventCursor(cursorValue)
	if err != nil {
		return nil, errors.New("cursor is malformed")
	}
	thread, err := g.query.GetThreadWindow(ctx, query.ThreadWindowRequest{
		EventID:  eventID,
		Limit:    limit,
		MaxDepth: maxDepth,
		Cursor:   cursor,
		Offset:   offset,
	})
	if err != nil {
		return nil, errors.New("thread fetch failed")
	}
	nextCursor, err := encodeEventCursor(thread.NextCursor)
	if err != nil {
		return nil, errors.New("thread fetch failed")
	}
	return g.buildThreadViewStream(ctx, thread, nextCursor), nil
}

func (g WSGateway) cacheDispatchFeed(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	events, err := g.query.GetAuthorEvents(ctx, pubkey, limit)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return rawMessagesToAny(events), nil
}

func (g WSGateway) cacheDispatchAuthorReplies(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	events, err := g.query.GetAuthorReplies(ctx, pubkey, limit)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return rawMessagesToAny(events), nil
}

func (g WSGateway) cacheDispatchEventActions(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID, _ := kwargs["event_id"].(string)
	counts, err := g.query.GetEventActionCounts(ctx, eventID)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{counts}, nil
}

func (g WSGateway) cacheDispatchContactList(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	entry, err := g.query.GetContactList(ctx, pubkey)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{map[string]any{
		"pubkey":     entry.Pubkey,
		"event_id":   entry.EventID,
		"created_at": entry.CreatedAt,
		"contacts":   entry.ContactsJSONRaw,
	}}, nil
}

func (g WSGateway) cacheDispatchRelayList(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	entry, err := g.query.GetRelayList(ctx, pubkey)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{map[string]any{
		"pubkey":     entry.Pubkey,
		"event_id":   entry.EventID,
		"created_at": entry.CreatedAt,
		"relays":     entry.RelaysJSONRaw,
	}}, nil
}
