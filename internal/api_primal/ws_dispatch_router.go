package api_primal

import (
	"context"
	"errors"
	"strings"
)

func (g WSGateway) resolveFilter(ctx context.Context, filter map[string]any) ([]any, error) {
	if cacheRaw, ok := filter["cache"]; ok {
		reqName, kwargs, err := parseCacheCallFilter(cacheRaw)
		if err != nil {
			return nil, err
		}
		return g.dispatchCacheCall(ctx, reqName, kwargs)
	}
	if idsRaw, ok := filter["ids"]; ok {
		return g.resolveIDsFilter(ctx, idsRaw)
	}
	if search, ok := filter["search"].(string); ok {
		limit := toInt(filter["limit"], 20)
		return g.resolveUnifiedSearch(ctx, search, limit)
	}
	return nil, errors.New("unsupported")
}

func parseCacheCallFilter(cacheRaw any) (string, map[string]any, error) {
	cacheArgs, ok := cacheRaw.([]any)
	if !ok || len(cacheArgs) == 0 {
		return "", nil, errors.New("invalid cache payload")
	}
	reqName, _ := cacheArgs[0].(string)
	kwargs := map[string]any{}
	if len(cacheArgs) > 1 {
		if m, ok := cacheArgs[1].(map[string]any); ok {
			kwargs = m
		}
	}
	return reqName, kwargs, nil
}

func (g WSGateway) resolveIDsFilter(ctx context.Context, idsRaw any) ([]any, error) {
	ids := toStringSlice(idsRaw)
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

func requestKindFromFilter(filter map[string]any) string {
	if cacheRaw, ok := filter["cache"]; ok {
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			return "cache"
		}
		if name, ok := cacheArgs[0].(string); ok {
			name = strings.TrimSpace(strings.ToLower(name))
			if name != "" {
				return name
			}
		}
		return "cache"
	}
	if _, ok := filter["ids"]; ok {
		return "ids"
	}
	if _, ok := filter["search"]; ok {
		return "search"
	}
	if _, ok := filter["since"]; ok {
		return "range"
	}
	if _, ok := filter["until"]; ok {
		return "range"
	}
	return "unknown"
}
