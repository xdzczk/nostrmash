package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/query"
)

type WSGateway struct {
	query    query.Service
	upgrader websocket.Upgrader
	opts     WSGatewayOptions
	log      *slog.Logger
}

type WSGatewayOptions struct {
	MaxSubscriptions int
	RequestTimeout   time.Duration
	MaxMessageBytes  int64
	MaxReqPerMinute  int
	AllowedOrigins   []string
	AllowAnyOrigin   bool
	Logger           *slog.Logger
}

func NewWSGateway(reader EventReader, opts WSGatewayOptions) WSGateway {
	if opts.MaxSubscriptions <= 0 {
		opts.MaxSubscriptions = 200
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = 10 * time.Second
	}
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = 1 << 20 // 1 MiB
	}
	if opts.MaxReqPerMinute <= 0 {
		opts.MaxReqPerMinute = 240
	}
	wsLog := opts.Logger
	if wsLog == nil {
		wsLog = logging.New("api_primal_ws")
	}
	return WSGateway{
		query: query.NewService(reader),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return checkOrigin(r, opts) },
		},
		opts: opts,
		log:  wsLog,
	}
}

func (g WSGateway) Handle(w http.ResponseWriter, r *http.Request) {
	conn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	metrics.IncPrimalWSConnection()
	defer metrics.DecPrimalWSConnection()
	conn.SetReadLimit(g.opts.MaxMessageBytes)
	remoteAddr := conn.RemoteAddr().String()
	g.log.Info("compat_ws_connected", "remote_addr", remoteAddr)
	defer g.log.Info("compat_ws_disconnected", "remote_addr", remoteAddr)

	var mu sync.Mutex
	subscriptions := make(map[string]struct{})
	windowStarted := time.Now().UTC()
	reqInWindow := 0

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		msg, err := decodeFrame(payload)
		if err != nil {
			_ = writeFrame(conn, []any{"NOTICE", "", "invalid json frame"})
			continue
		}
		if len(msg) < 2 {
			_ = writeFrame(conn, []any{"NOTICE", "", "malformed message"})
			continue
		}
		kind, _ := msg[0].(string)
		subID, _ := msg[1].(string)
		metrics.IncPrimalWSFrame(strings.ToLower(strings.TrimSpace(kind)))
		switch kind {
		case "REQ":
			now := time.Now().UTC()
			if now.Sub(windowStarted) >= time.Minute {
				windowStarted = now
				reqInWindow = 0
			}
			reqInWindow++
			if reqInWindow > g.opts.MaxReqPerMinute {
				metrics.ObservePrimalWSRequest("request", "rate_limited", 0)
				_ = writeFrame(conn, []any{"NOTICE", subID, "rate limit exceeded"})
				_ = writeFrame(conn, []any{"EOSE", subID})
				continue
			}
			mu.Lock()
			if len(subscriptions) >= g.opts.MaxSubscriptions {
				mu.Unlock()
				metrics.ObservePrimalWSRequest("request", "subscription_limit", 0)
				_ = writeFrame(conn, []any{"NOTICE", subID, "too many subscriptions"})
				_ = writeFrame(conn, []any{"EOSE", subID})
				continue
			}
			subscriptions[subID] = struct{}{}
			mu.Unlock()
			if len(msg) < 3 {
				metrics.ObservePrimalWSRequest("request", "invalid_request", 0)
				_ = writeFrame(conn, []any{"NOTICE", subID, "missing filter"})
				_ = writeFrame(conn, []any{"EOSE", subID})
				continue
			}
			ctx, cancel := context.WithTimeout(r.Context(), g.opts.RequestTimeout)
			frames := g.handleRequestFilters(ctx, subID, remoteAddr, msg[2:])
			cancel()
			for _, frame := range frames {
				if err := writeFrame(conn, frame); err != nil {
					return
				}
			}
			_ = writeFrame(conn, []any{"EOSE", subID})
		case "CLOSE":
			mu.Lock()
			delete(subscriptions, subID)
			mu.Unlock()
			g.log.Info("compat_ws_close", "sub_id", subID, "remote_addr", remoteAddr)
		default:
			metrics.ObservePrimalWSRequest("request", "unsupported_frame", 0)
			_ = writeFrame(conn, []any{"NOTICE", subID, "unsupported frame type"})
		}
	}
}

func (g WSGateway) handleRequestFilters(ctx context.Context, subID string, remoteAddr string, filters []any) [][]any {
	frames := make([][]any, 0)
	for _, rawFilter := range filters {
		requestKind := "unknown"
		start := time.Now()
		if err := ctx.Err(); err != nil {
			metrics.ObservePrimalWSRequest(requestKind, "timeout", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "timeout", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, "request_timeout"})
			break
		}
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			metrics.ObservePrimalWSRequest(requestKind, "invalid_filter", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "invalid_filter", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, "invalid filter payload"})
			continue
		}
		requestKind = requestKindFromFilter(filter)
		eventFrames, err := g.resolveFilter(ctx, filter)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				metrics.ObservePrimalWSRequest(requestKind, "timeout", time.Since(start))
				g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "timeout", "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
				frames = append(frames, []any{"NOTICE", subID, "request_timeout"})
				break
			}
			metrics.ObservePrimalWSRequest(requestKind, "error", time.Since(start))
			g.log.Warn("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "error", "error", err.Error(), "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
			frames = append(frames, []any{"NOTICE", subID, err.Error()})
			continue
		}
		metrics.ObservePrimalWSRequest(requestKind, "ok", time.Since(start))
		g.log.Info("compat_ws_request", "sub_id", subID, "request_kind", requestKind, "outcome", "ok", "events_emitted", len(eventFrames), "duration_ms", time.Since(start).Milliseconds(), "remote_addr", remoteAddr)
		for _, event := range eventFrames {
			frames = append(frames, []any{"EVENT", subID, event})
		}
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
		res, err := g.query.Search(ctx, search, limit)
		if err != nil {
			return nil, errors.New("search failed")
		}
		out := make([]any, 0, len(res.Events)+len(res.Profiles))
		for _, event := range res.Events {
			out = append(out, event)
		}
		for _, profile := range res.Profiles {
			out = append(out, map[string]any{
				"kind":    0,
				"pubkey":  profile.Pubkey,
				"profile": profile.ProfileJSON,
			})
		}
		return out, nil
	}
	return nil, errors.New("unsupported filter")
}

func (g WSGateway) dispatchCacheCall(ctx context.Context, reqName string, kwargs map[string]any) ([]any, error) {
	switch strings.ToLower(strings.TrimSpace(reqName)) {
	case "events":
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
	case "user_profile":
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
	case "user_infos":
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
	case "thread_view":
		eventID, _ := kwargs["event_id"].(string)
		limit := toInt(kwargs["limit"], 20)
		maxDepth := toInt(kwargs["max_depth"], 100)
		thread, err := g.query.GetThreadView(ctx, eventID, limit, maxDepth, nil)
		if err != nil {
			return nil, errors.New("thread fetch failed")
		}
		return []any{map[string]any{
			"event_id":             eventID,
			"event":                thread.Event,
			"ancestors":            thread.Ancestors,
			"missing_ancestor_ids": thread.MissingAncestorIDs,
			"replies":              thread.Replies,
			"consistency":          thread.Consistency,
		}}, nil
	case "feed":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		events, err := g.query.GetAuthorEvents(ctx, pubkey, limit)
		if err != nil {
			return nil, errors.New("author events fetch failed")
		}
		return rawMessagesToAny(events), nil
	case "author_replies":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		events, err := g.query.GetAuthorReplies(ctx, pubkey, limit)
		if err != nil {
			return nil, errors.New("author replies fetch failed")
		}
		return rawMessagesToAny(events), nil
	case "event_actions":
		eventID, _ := kwargs["event_id"].(string)
		counts, err := g.query.GetActionCounts(ctx, eventID)
		if err != nil {
			return nil, errors.New("event actions fetch failed")
		}
		return []any{counts}, nil
	case "contact_list":
		pubkey, _ := kwargs["pubkey"].(string)
		entry, err := g.query.GetContactList(ctx, pubkey)
		if err != nil {
			return nil, errors.New("contact list fetch failed")
		}
		return []any{map[string]any{
			"pubkey":     entry.Pubkey,
			"event_id":   entry.EventID,
			"created_at": entry.CreatedAt,
			"contacts":   entry.ContactsJSONRaw,
		}}, nil
	case "relay_list":
		pubkey, _ := kwargs["pubkey"].(string)
		entry, err := g.query.GetRelayList(ctx, pubkey)
		if err != nil {
			return nil, errors.New("relay list fetch failed")
		}
		return []any{map[string]any{
			"pubkey":     entry.Pubkey,
			"event_id":   entry.EventID,
			"created_at": entry.CreatedAt,
			"relays":     entry.RelaysJSONRaw,
		}}, nil
	case "search":
		q, _ := kwargs["query"].(string)
		limit := toInt(kwargs["limit"], 20)
		result, err := g.query.Search(ctx, q, limit)
		if err != nil {
			return nil, errors.New("search failed")
		}
		out := make([]any, 0, len(result.Events))
		for _, event := range result.Events {
			out = append(out, event)
		}
		return out, nil
	case "user_zaps":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetZaps(ctx, pubkey, limit))
	case "get_bookmarks":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetBookmarks(ctx, pubkey, limit))
	case "get_highlights":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetHighlights(ctx, pubkey, limit))
	case "long_form_content_feed":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetLongForm(ctx, pubkey, limit))
	case "get_directmsgs":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetDirectMessages(ctx, pubkey, limit))
	case "user_followers":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetFollowers(ctx, pubkey, limit))
	default:
		return nil, errors.New("unknown api request")
	}
}

func rawMessagesToAny(values []json.RawMessage) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func rawMessagesToAnyMust(values []json.RawMessage, err error) ([]any, error) {
	if err != nil {
		return nil, errors.New("request failed")
	}
	return rawMessagesToAny(values), nil
}

func decodeFrame(payload []byte) ([]any, error) {
	var out []any
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeFrame(conn *websocket.Conn, frame any) error {
	raw, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}

func toStringSlice(v any) []string {
	values, ok := v.([]any)
	if !ok {
		if stringsValue, ok := v.([]string); ok {
			return stringsValue
		}
		return []string{}
	}
	out := make([]string, 0, len(values))
	for _, value := range values {
		s, ok := value.(string)
		if !ok {
			continue
		}
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

func toInt(v any, fallback int) int {
	switch value := v.(type) {
	case int:
		if value > 0 {
			return value
		}
	case float64:
		casted := int(value)
		if casted > 0 {
			return casted
		}
	}
	return fallback
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

func checkOrigin(r *http.Request, opts WSGatewayOptions) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		// Non-browser clients commonly omit Origin.
		return true
	}
	if opts.AllowAnyOrigin {
		return true
	}
	if len(opts.AllowedOrigins) == 0 {
		return false
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Scheme == "" || parsedOrigin.Host == "" {
		return false
	}
	normalizedOrigin := parsedOrigin.Scheme + "://" + parsedOrigin.Host
	for _, allowed := range opts.AllowedOrigins {
		allowed = strings.TrimSpace(allowed)
		if allowed == "" {
			continue
		}
		if strings.EqualFold(allowed, normalizedOrigin) {
			return true
		}
	}
	return false
}
