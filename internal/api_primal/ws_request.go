package api_primal

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

func (g WSGateway) handleRequestFilters(ctx context.Context, subID string, remoteAddr string, filters []any) [][]any {
	frames := make([][]any, 0)
	for _, rawFilter := range filters {
		requestKind := "unknown"
		start := time.Now()
		requestCtx, span := traceutil.StartSpan(ctx, "primal_ws.request",
			traceutil.KV("ws.sub_id", subID),
			traceutil.KV("ws.remote_addr", remoteAddr),
		)
		if err := ctx.Err(); err != nil {
			metrics.ObservePrimalWSRequest(requestKind, "timeout", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "timeout", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, "request_timeout"})
			span.End(err)
			break
		}
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			metrics.ObservePrimalWSRequest(requestKind, "invalid_filter", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "invalid_filter", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, "invalid filter payload"})
			span.End(errors.New("invalid filter payload"))
			continue
		}
		requestKind = requestKindFromFilter(filter)
		eventFrames, err := g.resolveFilter(requestCtx, filter)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				metrics.ObservePrimalWSRequest(requestKind, "timeout", time.Since(start))
				g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "timeout", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
				frames = append(frames, []any{"NOTICE", subID, "request_timeout"})
				span.End(err)
				break
			}
			metrics.ObservePrimalWSRequest(requestKind, "error", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "error", "error", err.Error(), "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, err.Error()})
			span.End(err)
			continue
		}
		metrics.ObservePrimalWSRequest(requestKind, "ok", time.Since(start))
		g.log.Info("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "ok", "events_emitted", len(eventFrames), "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
		for _, event := range eventFrames {
			frames = append(frames, []any{"EVENT", subID, event})
		}
		span.End(nil)
	}
	return frames
}

func (g WSGateway) resolveFilter(ctx context.Context, filter map[string]any) ([]any, error) {
	if cacheRaw, ok := filter["cache"]; ok {
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			return nil, errors.New("invalid cache payload")
		}
		reqName, _ := cacheArgs[0].(string)
		kwargs := map[string]any{}
		if len(cacheArgs) > 1 {
			if m, ok := cacheArgs[1].(map[string]any); ok {
				kwargs = m
			}
		}
		return g.dispatchCacheCall(ctx, reqName, kwargs)
	}
	if idsRaw, ok := filter["ids"]; ok {
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
	if search, ok := filter["search"].(string); ok {
		limit := toInt(filter["limit"], 20)
		return g.resolveUnifiedSearch(ctx, search, limit)
	}
	return nil, errors.New("unsupported")
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
