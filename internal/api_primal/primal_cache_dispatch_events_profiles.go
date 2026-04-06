package api_primal

import (
	"context"
	"errors"
)

func (g WSGateway) cacheDispatchEvents(ctx context.Context, kwargs map[string]any) ([]any, error) {
	ids := toStringSlice(kwargs["event_ids"])
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil, errors.New("event fetch failed")
	}
	out := make([]any, 0, len(found))
	for _, id := range ids {
		if raw, ok := found[id]; ok {
			out = append(out, raw)
		}
	}
	return out, nil
}

func (g WSGateway) cacheDispatchUserProfile(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	profile, err := g.query.GetProfile(ctx, pubkey)
	if err != nil {
		return nil, errors.New("profile fetch failed")
	}
	return []any{map[string]any{
		"pubkey":              profile.Pubkey,
		"metadata_event_id":   profile.MetadataEventID,
		"metadata_created_at": profile.MetadataCreatedAt,
		"profile":             profile.ProfileJSON,
	}}, nil
}

func (g WSGateway) cacheDispatchUserInfos(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkeys := toStringSlice(kwargs["pubkeys"])
	result, err := g.query.GetUserInfos(ctx, pubkeys)
	if err != nil {
		return nil, errors.New("profile batch fetch failed")
	}
	out := make([]any, 0, len(result.Profiles))
	for _, profile := range result.Profiles {
		out = append(out, map[string]any{
			"pubkey":              profile.Pubkey,
			"metadata_event_id":   profile.MetadataEventID,
			"metadata_created_at": profile.MetadataCreatedAt,
			"profile":             profile.ProfileJSON,
		})
	}
	return out, nil
}
