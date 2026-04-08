package api_primal

import (
	"context"
	"errors"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) cacheDispatchSearch(ctx context.Context, kwargs map[string]any) ([]any, error) {
	q, _ := kwargs["query"].(string)
	limit := toInt(kwargs["limit"], 20)
	return g.resolveUnifiedSearch(ctx, q, limit)
}

func (g WSGateway) resolveUnifiedSearch(ctx context.Context, text string, limit int) ([]any, error) {
	result, err := g.query.Search(ctx, text, limit)
	if err != nil {
		return nil, errors.New("search failed")
	}
	out := make([]any, 0, len(result.Events)+len(result.Profiles))
	for _, event := range result.Events {
		out = append(out, event)
	}
	for _, profile := range result.Profiles {
		out = append(out, map[string]any{
			"kind":                0,
			"pubkey":              profile.Pubkey,
			"metadata_event_id":   profile.MetadataEventID,
			"metadata_created_at": profile.MetadataCreatedAt,
			"profile":             profile.ProfileJSON,
		})
	}
	return out, nil
}

func (g WSGateway) cacheDispatchUserZaps(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	return rawMessagesToAnyMust(g.query.GetZaps(ctx, pubkey, limit))
}

func (g WSGateway) cacheDispatchUserZapsBySats(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	return rawMessagesToAnyMust(g.query.GetUserZapsBySats(ctx, pubkey, limit))
}

func (g WSGateway) cacheDispatchEventZapsBySats(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID, _ := kwargs["event_id"].(string)
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetEventZapsBySats(ctx, eventID, limit)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return rawMessagesToAny(values), nil
}

func (g WSGateway) cacheDispatchIsUserFollowing(ctx context.Context, kwargs map[string]any) ([]any, error) {
	follower, _ := kwargs["follower_pubkey"].(string)
	followed, _ := kwargs["followed_pubkey"].(string)
	ok, err := g.query.IsUserFollowing(ctx, follower, followed)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
	}
	return []any{map[string]any{
		"follower_pubkey": follower,
		"followed_pubkey": followed,
		"is_following":    ok,
	}}, nil
}

func (g WSGateway) cacheDispatchMutualFollows(ctx context.Context, kwargs map[string]any) ([]any, error) {
	left, _ := kwargs["left_pubkey"].(string)
	right, _ := kwargs["right_pubkey"].(string)
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetMutualFollows(ctx, left, right, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			// Compatibility: this call historically returned an empty list when unsupported.
			values = []string{}
		} else {
			return nil, wrapPrimalRequestError(err)
		}
	}
	return []any{map[string]any{
		"left_pubkey":  left,
		"right_pubkey": right,
		"pubkeys":      values,
	}}, nil
}
