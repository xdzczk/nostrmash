package api_primal

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) cacheDispatchParameterizedReplaceableList(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	identifier, hasIdentifier, err := compatIdentifierValue(kwargs)
	if err != nil || !hasIdentifier {
		return nil, errors.New("request failed")
	}
	// Primal list semantics are identifier-scoped in categorized people namespace.
	values, err := g.query.GetParameterizedReplaceableListByIdentifier(ctx, pubkey, parameterizedListKind, identifier, limit)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return rawMessagesToAny(values), nil
}

func (g WSGateway) cacheDispatchParametrizedReplaceableEvent(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	kind := toInt(kwargs["kind"], 30000)
	identifier, hasIdentifier, err := compatIdentifierValue(kwargs)
	if err != nil || !hasIdentifier {
		return nil, errors.New("request failed")
	}
	event, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, identifier)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{event}, nil
}

func (g WSGateway) cacheDispatchParametrizedReplaceableEvents(ctx context.Context, kwargs map[string]any) ([]any, error) {
	if rawEvents, ok := kwargs["events"]; ok {
		refs, err := parseParameterizedReplaceableRefs(rawEvents)
		if err != nil {
			return nil, errors.New("request failed")
		}
		out := make([]json.RawMessage, 0, len(refs))
		for _, ref := range refs {
			event, err := g.query.GetParameterizedReplaceableEvent(ctx, ref.pubkey, ref.kind, ref.identifier)
			if err != nil {
				if query.IsNotFound(err) {
					continue
				}
				return nil, wrapPrimalRequestError(err)
			}
			out = append(out, event)
		}
		return rawMessagesToAny(out), nil
	}
	kind := toInt(kwargs["kind"], 30000)
	dTag, _ := kwargs["d_tag"].(string)
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetParameterizedReplaceableEvents(ctx, kind, dTag, limit)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return rawMessagesToAny(values), nil
}
