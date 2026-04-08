package api_primal

import (
	"context"
	"errors"
	"strings"
)

type wsFilterHandler func(WSGateway, context.Context, map[string]any, any) ([]any, error)
type wsFilterKindResolver func(any) string

type wsFilterRoute struct {
	key         string
	handler     wsFilterHandler
	resolveKind wsFilterKindResolver
	defaultKind string
}

var wsFilterRoutes = []wsFilterRoute{
	{
		key:         "cache",
		handler:     wsCacheFilterHandler,
		resolveKind: resolveCacheRequestKind,
		defaultKind: "cache",
	},
	{
		key:         "ids",
		handler:     wsIDsFilterHandler,
		defaultKind: "ids",
	},
	{
		key:         "search",
		handler:     wsSearchFilterHandler,
		defaultKind: "search",
	},
}

func (g WSGateway) resolveFilter(ctx context.Context, filter map[string]any) ([]any, error) {
	for _, route := range wsFilterRoutes {
		raw, ok := filter[route.key]
		if !ok {
			continue
		}
		return route.handler(g, ctx, filter, raw)
	}
	return nil, errors.New("unsupported")
}

func wsCacheFilterHandler(g WSGateway, ctx context.Context, _ map[string]any, raw any) ([]any, error) {
	reqName, kwargs, err := parseCacheCallFilter(raw)
	if err != nil {
		return nil, err
	}
	return g.dispatchCacheCall(ctx, reqName, kwargs)
}

func wsIDsFilterHandler(g WSGateway, ctx context.Context, _ map[string]any, raw any) ([]any, error) {
	return g.resolveIDsFilter(ctx, raw)
}

func wsSearchFilterHandler(g WSGateway, ctx context.Context, filter map[string]any, raw any) ([]any, error) {
	search, ok := raw.(string)
	if !ok {
		return nil, errors.New("unsupported")
	}
	limit := toInt(filter["limit"], 20)
	return g.resolveUnifiedSearch(ctx, search, limit)
}

func resolveCacheRequestKind(raw any) string {
	cacheArgs, ok := raw.([]any)
	if !ok || len(cacheArgs) == 0 {
		return "cache"
	}
	name, _ := cacheArgs[0].(string)
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "cache"
	}
	return name
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
	for _, route := range wsFilterRoutes {
		raw, ok := filter[route.key]
		if !ok {
			continue
		}
		if route.resolveKind != nil {
			return route.resolveKind(raw)
		}
		return route.defaultKind
	}
	if _, ok := filter["since"]; ok {
		return "range"
	}
	if _, ok := filter["until"]; ok {
		return "range"
	}
	return "unknown"
}
