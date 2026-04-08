package api_primal

import (
	"context"
	"errors"
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
