package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) cacheDispatchBookmarks(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	return rawMessagesToAnyMust(g.query.GetBookmarks(ctx, pubkey, limit))
}

func (g WSGateway) resolveHighlightsResponse(ctx context.Context, kwargs map[string]any) ([]any, error) {
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	if eventID := strings.TrimSpace(stringValue(kwargs["event_id"])); eventID != "" {
		values, err := g.query.GetHighlightsByEventID(ctx, eventID, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return g.buildEventsWithMetadataAndRange(ctx, values, "created_at"), nil
	}
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	identifier := strings.TrimSpace(stringValue(kwargs["identifier"]))
	if pubkey != "" && identifier != "" {
		kind := toInt(kwargs["kind"], 30023)
		values, err := g.query.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return g.buildEventsWithMetadataAndRange(ctx, values, "created_at"), nil
	}
	values, err := g.query.GetHighlights(ctx, pubkey, limit)
	if err != nil {
		return nil, errors.New("request failed")
	}
	return g.buildEventsWithMetadataAndRange(ctx, values, "created_at"), nil
}

func (g WSGateway) resolveLongFormContentFeed(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	notes := strings.ToLower(strings.TrimSpace(stringValue(kwargs["notes"])))
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	switch notes {
	case "", "authored":
		values, err := g.query.GetLongForm(ctx, pubkey, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return g.buildEventsWithMetadataAndRange(ctx, values, "created_at"), nil
	case "follows":
		if pubkey == "" {
			return []any{buildRangeEvent("created_at", 0, 0, false)}, nil
		}
		contactList, err := g.query.GetContactList(ctx, pubkey)
		if err != nil && !query.IsNotFound(err) {
			return nil, errors.New("request failed")
		}
		follows := parseContactListPubkeys(contactList.ContactsJSONRaw)
		collected := make([]json.RawMessage, 0, limit)
		for followed := range follows {
			values, fetchErr := g.query.GetLongForm(ctx, followed, limit)
			if fetchErr != nil {
				return nil, errors.New("request failed")
			}
			collected = append(collected, values...)
		}
		collected = sortAndLimitEvents(collected, limit)
		return g.buildEventsWithMetadataAndRange(ctx, collected, "created_at"), nil
	default:
		return nil, errors.New("unsupported notes mode")
	}
}
