package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

func (g WSGateway) cacheDispatchNetworkStats(ctx context.Context, kwargs map[string]any) ([]any, error) {
	stats, err := g.query.GetNetworkStats(ctx)
	if err != nil {
		return nil, errors.New("request failed")
	}
	return []any{stats}, nil
}

func (g WSGateway) cacheDispatchServerName(ctx context.Context, kwargs map[string]any) ([]any, error) {
	_ = kwargs
	return []any{map[string]any{"server_name": "nostrmash"}}, nil
}

func (g WSGateway) cacheDispatchRecommendedReads(ctx context.Context, kwargs map[string]any) ([]any, error) {
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetCuratedRecommendedReads(ctx, limit)
	if err != nil {
		return nil, errors.New("request failed")
	}
	return []any{buildCuratedListEvent(primalKindRecommendedRead, map[string]any{
		"reads": values,
	})}, nil
}

func (g WSGateway) cacheDispatchReadsTopics(ctx context.Context, kwargs map[string]any) ([]any, error) {
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetCuratedReadsTopics(ctx, limit)
	if err != nil {
		return nil, errors.New("request failed")
	}
	return []any{buildCuratedListEvent(primalKindReadsTopics, map[string]any{
		"topics": values,
	})}, nil
}

func (g WSGateway) cacheDispatchFeaturedAuthors(ctx context.Context, kwargs map[string]any) ([]any, error) {
	limit := toInt(kwargs["limit"], 20)
	values, err := g.query.GetCuratedFeaturedAuthors(ctx, limit)
	if err != nil {
		return nil, errors.New("request failed")
	}
	pubkeys := make([]string, 0, len(values))
	for _, value := range values {
		pubkey := strings.TrimSpace(value.Pubkey)
		if pubkey == "" {
			continue
		}
		pubkeys = append(pubkeys, pubkey)
	}
	out := []any{buildCuratedListEvent(primalKindFeaturedAuthors, map[string]any{
		"authors": values,
	})}
	out = append(out, g.buildMetadataEvents(ctx, pubkeys)...)
	return out, nil
}

func (g WSGateway) cacheDispatchCreatorPaidTiers(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey, _ := kwargs["pubkey"].(string)
	pubkey = strings.TrimSpace(pubkey)
	liveTierIndexEvents, err := g.query.GetRecentEventsByKindAndPubkey(ctx, 17000, pubkey, 1)
	if err == nil && len(liveTierIndexEvents) > 0 {
		out := make([]any, 0, 8)
		out = append(out, liveTierIndexEvents[0])
		referencedIDs := tagValuesFromRawEvent(liveTierIndexEvents[0], "e")
		if len(referencedIDs) > 0 {
			if found, batchErr := g.query.GetEventBatch(ctx, referencedIDs); batchErr == nil {
				for _, id := range referencedIDs {
					if raw, ok := found[id]; ok {
						out = append(out, raw)
					}
				}
			}
		}
		return out, nil
	}
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "not implemented") {
		return nil, errors.New("request failed")
	}
	tiers, err := g.query.GetCreatorPaidTiers(ctx, pubkey)
	if err != nil {
		return nil, errors.New("request failed")
	}
	tierPayloads := make([]any, 0, len(tiers))
	for _, tier := range tiers {
		var decoded any
		if err := json.Unmarshal(tier, &decoded); err != nil {
			continue
		}
		tierPayloads = append(tierPayloads, decoded)
	}
	return []any{buildCuratedListEvent(primalKindCreatorPaidTier, map[string]any{
		"pubkey": strings.TrimSpace(pubkey),
		"tiers":  tierPayloads,
	})}, nil
}

func (g WSGateway) cacheDispatchUserOfLNAddress(ctx context.Context, kwargs map[string]any) ([]any, error) {
	address, _ := kwargs["ln_address"].(string)
	result, metadata, ok, err := g.resolveUserOfLNAddress(ctx, address)
	if err != nil {
		return nil, errors.New("request failed")
	}
	if !ok {
		return []any{}, nil
	}
	out := []any{result}
	out = append(out, metadata...)
	return out, nil
}

func (g WSGateway) resolveUserOfLNAddress(ctx context.Context, address string) (map[string]any, []any, bool, error) {
	normalized := strings.TrimSpace(strings.ToLower(address))
	if normalized == "" {
		return nil, nil, false, nil
	}
	pubkey, err := g.query.GetPubkeyByLNAddress(ctx, normalized)
	if err != nil || strings.TrimSpace(pubkey) == "" {
		return nil, nil, false, nil
	}
	contentRaw, _ := json.Marshal(map[string]any{"pubkey": pubkey})
	return map[string]any{
		"kind":    primalKindUserPubkey,
		"content": string(contentRaw),
	}, g.buildMetadataEvents(ctx, []string{pubkey}), true, nil
}
