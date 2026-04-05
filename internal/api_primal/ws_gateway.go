package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/store"
)

type WSGateway struct {
	query    query.Service
	upgrader websocket.Upgrader
	opts     WSGatewayOptions
	log      *slog.Logger
}

type WSGatewayOptions struct {
	MaxSubscriptions  int
	RequestTimeout    time.Duration
	MaxMessageBytes   int64
	MaxReqPerMinute   int
	MaxDMReqPerMinute int
	AllowedOrigins    []string
	AllowAnyOrigin    bool
	Logger            *slog.Logger
}

type dmLiveSubscription struct {
	SubID    string
	Kind     string
	Receiver string
	Sender   string
}

const (
	primalKindRange           = 10000113
	primalKindDirectMsgCount  = 10000117
	primalKindDirectMsgCounts = 10000118
	primalKindDirectMsgCount2 = 10000134
	primalKindFilteringReason = 10000131
	primalKindHiddenByContent = 10000137
	primalKindUserPubkey      = 10000138
	primalKindRecommendedRead = 10000145
	primalKindReadsTopics     = 10000146
	primalKindCreatorPaidTier = 10000147
	primalKindFeaturedAuthors = 10000148
	parameterizedListKind     = 30000
)

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
	if opts.MaxDMReqPerMinute <= 0 {
		opts.MaxDMReqPerMinute = 30
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
	var writeMu sync.Mutex
	subscriptions := make(map[string]struct{})
	dmLiveSubscriptions := make(map[string]dmLiveSubscription)
	windowStarted := time.Now().UTC()
	reqInWindow := 0
	dmReqInWindow := 0
	done := make(chan struct{})
	defer close(done)

	sendFrame := func(frame any) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		return writeFrame(conn, frame)
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				mu.Lock()
				liveSubs := make([]dmLiveSubscription, 0, len(dmLiveSubscriptions))
				for _, sub := range dmLiveSubscriptions {
					liveSubs = append(liveSubs, sub)
				}
				mu.Unlock()
				for _, sub := range liveSubs {
					frame, err := g.resolveDMCountFrame(r.Context(), sub)
					if err != nil {
						continue
					}
					if err := sendFrame(frame); err != nil {
						return
					}
				}
			}
		}
	}()

	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			return
		}
		msg, err := decodeFrame(payload)
		if err != nil {
			_ = sendFrame([]any{"NOTICE", "", "invalid json frame"})
			continue
		}
		if len(msg) < 2 {
			_ = sendFrame([]any{"NOTICE", "", "malformed message"})
			continue
		}
		kind, _ := msg[0].(string)
		subID, _ := msg[1].(string)
		metrics.IncPrimalWSFrame(strings.ToLower(strings.TrimSpace(kind)))
		switch kind {
		case "REQ":
			if len(msg) < 3 {
				metrics.ObservePrimalWSRequest("request", "invalid_request", 0)
				_ = sendFrame([]any{"NOTICE", subID, "missing filter"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			now := time.Now().UTC()
			if now.Sub(windowStarted) >= time.Minute {
				windowStarted = now
				reqInWindow = 0
				dmReqInWindow = 0
			}
			reqInWindow++
			if reqInWindow > g.opts.MaxReqPerMinute {
				metrics.ObservePrimalWSRequest("request", "rate_limited", 0)
				_ = sendFrame([]any{"NOTICE", subID, "rate limit exceeded"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}

			if isDirectMessagesRequest(msg[2:]) {
				dmReqInWindow++
				if dmReqInWindow > g.opts.MaxDMReqPerMinute {
					metrics.ObservePrimalWSRequest("get_directmsgs", "rate_limited", 0)
					_ = sendFrame([]any{"NOTICE", subID, "dm rate limit exceeded"})
					_ = sendFrame([]any{"EOSE", subID})
					continue
				}
			}

			mu.Lock()
			if len(subscriptions) >= g.opts.MaxSubscriptions {
				mu.Unlock()
				metrics.ObservePrimalWSRequest("request", "subscription_limit", 0)
				_ = sendFrame([]any{"NOTICE", subID, "too many subscriptions"})
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			subscriptions[subID] = struct{}{}
			mu.Unlock()
			if liveSub, ok := parseDMLiveSubscription(subID, msg[2:]); ok && hasOnlyDMLiveFilters(msg[2:]) {
				mu.Lock()
				dmLiveSubscriptions[subID] = liveSub
				mu.Unlock()
				_ = sendFrame([]any{"EOSE", subID})
				continue
			}
			ctx, cancel := context.WithTimeout(r.Context(), g.opts.RequestTimeout)
			frames := g.handleRequestFilters(ctx, subID, remoteAddr, msg[2:])
			cancel()
			for _, frame := range frames {
				if err := sendFrame(frame); err != nil {
					return
				}
			}
			_ = sendFrame([]any{"EOSE", subID})
			if liveSub, ok := parseDMLiveSubscription(subID, msg[2:]); ok {
				mu.Lock()
				dmLiveSubscriptions[subID] = liveSub
				mu.Unlock()
				continue
			}
			mu.Lock()
			delete(subscriptions, subID)
			delete(dmLiveSubscriptions, subID)
			mu.Unlock()
		case "CLOSE":
			mu.Lock()
			delete(subscriptions, subID)
			delete(dmLiveSubscriptions, subID)
			mu.Unlock()
			g.log.Info("compat_ws_close", "sub_id", subID, "remote_addr", remoteAddr)
		default:
			metrics.ObservePrimalWSRequest("request", "unsupported_frame", 0)
			_ = sendFrame([]any{"NOTICE", subID, "unsupported frame type"})
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
		return g.resolveUnifiedSearch(ctx, search, limit)
	}
	return nil, errors.New("unsupported")
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
		limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
		maxDepth := toBoundedPositiveInt(kwargs["max_depth"], 100, 100)
		offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
		cursorValue, err := optionalStringValue(kwargs["cursor"])
		if err != nil {
			return nil, errors.New("cursor is malformed")
		}
		cursor, err := decodeEventCursor(cursorValue)
		if err != nil {
			return nil, errors.New("cursor is malformed")
		}
		thread, err := g.resolveThreadViewDescending(ctx, eventID, limit, maxDepth, cursor, offset)
		if err != nil {
			return nil, errors.New("thread fetch failed")
		}
		nextCursor, err := encodeEventCursor(thread.NextCursor)
		if err != nil {
			return nil, errors.New("thread fetch failed")
		}
		return g.buildThreadViewStream(ctx, thread, nextCursor), nil
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
		return g.resolveUnifiedSearch(ctx, q, limit)
	case "user_zaps":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetZaps(ctx, pubkey, limit))
	case "user_zaps_by_satszapped":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetUserZapsBySats(ctx, pubkey, limit))
	case "event_zaps_by_satszapped":
		eventID, _ := kwargs["event_id"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetEventZapsBySats(ctx, eventID, limit))
	case "is_user_following":
		follower, _ := kwargs["follower_pubkey"].(string)
		followed, _ := kwargs["followed_pubkey"].(string)
		ok, err := g.query.IsUserFollowing(ctx, follower, followed)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{map[string]any{
			"follower_pubkey": follower,
			"followed_pubkey": followed,
			"is_following":    ok,
		}}, nil
	case "mutual_follows":
		left, _ := kwargs["left_pubkey"].(string)
		right, _ := kwargs["right_pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		values, err := g.query.GetMutualFollows(ctx, left, right, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{map[string]any{
			"left_pubkey":  left,
			"right_pubkey": right,
			"pubkeys":      values,
		}}, nil
	case "get_directmsg_contacts":
		pubkey, _ := kwargs["pubkey"].(string)
		if err := validatePubkeyHex(pubkey); err != nil {
			return nil, err
		}
		relation, err := parseDirectMessageContactsRelation(kwargs["relation"])
		if err != nil {
			return nil, err
		}
		limit := toInt(kwargs["limit"], 20)
		offset := toInt(kwargs["offset"], 0)
		since := toInt64(kwargs["since"], 0)
		until := toInt64(kwargs["until"], time.Now().Unix())
		values, err := g.query.GetDirectMessageContactsDetailed(ctx, pubkey, limit, offset, since, until)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return g.buildDirectMessageContactsPayload(ctx, pubkey, relation, values)
	case "get_bookmarks":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetBookmarks(ctx, pubkey, limit))
	case "get_highlights":
		return g.resolveHighlightsResponse(ctx, kwargs)
	case "long_form_content_feed":
		return g.resolveLongFormContentFeed(ctx, kwargs)
	case "long_form_content_thread_view":
		return g.resolveLongFormContentThreadView(ctx, kwargs)
	case "get_directmsgs":
		pubkey, _ := kwargs["pubkey"].(string)
		if err := validatePubkeyHex(pubkey); err != nil {
			return nil, err
		}
		peer, _ := kwargs["peer_pubkey"].(string)
		if strings.TrimSpace(peer) == "" {
			peer, _ = kwargs["sender"].(string)
		}
		if err := validatePubkeyHex(peer); err != nil {
			return nil, err
		}
		since := toInt64(kwargs["since"], 0)
		until := toInt64(kwargs["until"], time.Now().Unix())
		limit := toInt(kwargs["limit"], 20)
		offset := toInt(kwargs["offset"], 0)
		values, err := g.query.GetDirectMessagesWithRange(ctx, pubkey, peer, since, until, limit, offset)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return g.buildDirectMessagesPayload(ctx, pubkey, peer, values), nil
	case "directmsg_count":
		pubkey, _ := kwargs["pubkey"].(string)
		if err := validatePubkeyHex(pubkey); err != nil {
			return nil, err
		}
		sender, _ := kwargs["sender"].(string)
		if sender != "" {
			if err := validatePubkeyHex(sender); err != nil {
				return nil, err
			}
		}
		count, err := g.query.GetDirectMessageCount(ctx, pubkey, sender)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{buildDirectMessageCountEvent(count)}, nil
	case "directmsg_count_2":
		pubkey, _ := kwargs["pubkey"].(string)
		if err := validatePubkeyHex(pubkey); err != nil {
			return nil, err
		}
		sender, _ := kwargs["sender"].(string)
		if sender != "" {
			if err := validatePubkeyHex(sender); err != nil {
				return nil, err
			}
		}
		count, err := g.query.GetDirectMessageCount(ctx, pubkey, sender)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{buildDirectMessageCount2Event(count)}, nil
	case "reset_directmsg_count":
		receiver, sender, err := parseAndValidateDMResetAuth(kwargs)
		if err != nil {
			return nil, err
		}
		if err := g.query.ResetDirectMessageCount(ctx, receiver, sender); err != nil {
			return nil, errors.New("request failed")
		}
		if err := g.query.ResetDirectMessageUnread(ctx, receiver, sender); err != nil {
			return nil, errors.New("request failed")
		}
		return []any{}, nil
	case "reset_directmsg_counts":
		receiver, err := parseAndValidateDMResetAllAuth(kwargs)
		if err != nil {
			return nil, err
		}
		if err := g.query.ResetDirectMessageCounts(ctx, receiver); err != nil {
			return nil, errors.New("request failed")
		}
		return []any{}, nil
	case "user_mentions":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetMentions(ctx, pubkey, limit))
	case "user_followers":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetFollowers(ctx, pubkey, limit))
	case "mutelist":
		pubkey, _ := kwargs["pubkey"].(string)
		return g.buildModerationListResponse(ctx, pubkey, moderationListMute)
	case "mutelists":
		pubkey, _ := kwargs["pubkey"].(string)
		return g.buildModerationListResponse(ctx, pubkey, moderationListMutelists)
	case "allowlist":
		pubkey, _ := kwargs["pubkey"].(string)
		return g.buildModerationListResponse(ctx, pubkey, moderationListAllowlist)
	case "is_hidden_by_content_moderation":
		return g.buildHiddenByContentModerationResponse(ctx, kwargs)
	case "search_filterlist":
		return g.buildSearchFilterlistResponse(ctx, kwargs)
	case "parameterized_replaceable_list":
		pubkey, _ := kwargs["pubkey"].(string)
		limit := toInt(kwargs["limit"], 20)
		identifier, hasIdentifier, err := compatIdentifierValue(kwargs)
		if err != nil || !hasIdentifier {
			return nil, errors.New("request failed")
		}
		// Primal list semantics are identifier-scoped in categorized people namespace.
		return rawMessagesToAnyMust(g.query.GetParameterizedReplaceableListByIdentifier(ctx, pubkey, parameterizedListKind, identifier, limit))
	case "parametrized_replaceable_event":
		pubkey, _ := kwargs["pubkey"].(string)
		kind := toInt(kwargs["kind"], 30000)
		identifier, hasIdentifier, err := compatIdentifierValue(kwargs)
		if err != nil || !hasIdentifier {
			return nil, errors.New("request failed")
		}
		event, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, identifier)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{event}, nil
	case "parametrized_replaceable_events":
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
					return nil, errors.New("request failed")
				}
				out = append(out, event)
			}
			return rawMessagesToAny(out), nil
		}
		kind := toInt(kwargs["kind"], 30000)
		dTag, _ := kwargs["d_tag"].(string)
		limit := toInt(kwargs["limit"], 20)
		return rawMessagesToAnyMust(g.query.GetParameterizedReplaceableEvents(ctx, kind, dTag, limit))
	case "network_stats", "net_stats", "nostr_stats":
		stats, err := g.query.GetNetworkStats(ctx)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{stats}, nil
	case "server_name":
		return []any{map[string]any{"server_name": "nostrmash"}}, nil
	case "get_recommended_reads":
		limit := toInt(kwargs["limit"], 20)
		values, err := g.query.GetCuratedRecommendedReads(ctx, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{buildCuratedListEvent(primalKindRecommendedRead, map[string]any{
			"reads": values,
		})}, nil
	case "get_reads_topics":
		limit := toInt(kwargs["limit"], 20)
		values, err := g.query.GetCuratedReadsTopics(ctx, limit)
		if err != nil {
			return nil, errors.New("request failed")
		}
		return []any{buildCuratedListEvent(primalKindReadsTopics, map[string]any{
			"topics": values,
		})}, nil
	case "get_featured_authors":
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
	case "creator_paid_tiers":
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
	case "user_of_ln_address":
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
	default:
		return nil, errors.New("unsupported")
	}
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

func (g WSGateway) resolveLongFormContentThreadView(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	identifier := strings.TrimSpace(stringValue(kwargs["identifier"]))
	if pubkey == "" || identifier == "" {
		return nil, errors.New("request failed")
	}
	kind := toInt(kwargs["kind"], 30023)
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	maxDepth := toBoundedPositiveInt(kwargs["max_depth"], 100, 100)
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	cursorValue, err := optionalStringValue(kwargs["cursor"])
	if err != nil {
		return nil, errors.New("cursor is malformed")
	}
	cursor, err := decodeEventCursor(cursorValue)
	if err != nil {
		return nil, errors.New("cursor is malformed")
	}
	rootEvent, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, identifier)
	if err != nil {
		return nil, errors.New("request failed")
	}
	eventID := eventIDFromRaw(rootEvent)
	if eventID == "" {
		return nil, errors.New("request failed")
	}
	threadBase, descReplies, err := g.collectThreadRepliesDescending(ctx, eventID, maxDepth)
	if err != nil {
		return nil, errors.New("request failed")
	}
	extraLimit := limit + offset + 1000
	if extraLimit < 1000 {
		extraLimit = 1000
	}
	if extraLimit > 5000 {
		extraLimit = 5000
	}
	aTagReplies, err := g.query.GetLongFormThreadATagReplies(ctx, kind, pubkey, identifier, extraLimit)
	if err != nil {
		return nil, errors.New("request failed")
	}
	descReplies = mergeOrderedReplies(descReplies, toOrderedReplies(aTagReplies))
	window, next := paginateOrderedReplies(descReplies, limit, cursor, offset)
	thread := threadBase
	thread.Replies = window
	thread.NextCursor = next
	if len(thread.Replies) == 0 {
		thread.NextCursor = nil
	}
	nextCursor, err := encodeEventCursor(thread.NextCursor)
	if err != nil {
		return nil, errors.New("request failed")
	}
	return g.buildThreadViewStream(ctx, thread, nextCursor), nil
}

func (g WSGateway) resolveThreadViewDescending(
	ctx context.Context,
	eventID string,
	limit int,
	maxDepth int,
	cursor *store.EventOrderCursor,
	offset int,
) (query.ThreadView, error) {
	out, descReplies, err := g.collectThreadRepliesDescending(ctx, eventID, maxDepth)
	if err != nil {
		return query.ThreadView{}, err
	}
	window, next := paginateOrderedReplies(descReplies, limit, cursor, offset)
	out.Replies = window
	out.NextCursor = next
	return out, nil
}

type orderedReply struct {
	raw       json.RawMessage
	createdAt int64
	id        string
}

func (g WSGateway) collectThreadRepliesDescending(
	ctx context.Context,
	eventID string,
	maxDepth int,
) (query.ThreadView, []orderedReply, error) {
	const fetchPageSize = 100
	var out query.ThreadView
	var ascCursor *store.EventOrderCursor
	collected := make([]json.RawMessage, 0, fetchPageSize)
	firstPage := true
	seenCursors := map[string]struct{}{}
	for {
		page, err := g.query.GetThreadView(ctx, eventID, fetchPageSize, maxDepth, ascCursor)
		if err != nil {
			return query.ThreadView{}, nil, err
		}
		if firstPage {
			out = page
			firstPage = false
		}
		collected = append(collected, page.Replies...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Replies) == 0 {
			break
		}
		cursorKey := fmt.Sprintf("%d:%s", page.NextCursor.CreatedAt, strings.TrimSpace(page.NextCursor.ID))
		if _, seen := seenCursors[cursorKey]; seen {
			break
		}
		seenCursors[cursorKey] = struct{}{}
		ascCursor = page.NextCursor
	}

	descReplies := make([]orderedReply, 0, len(collected))
	for i := len(collected) - 1; i >= 0; i-- {
		id, createdAt, ok := eventOrderFromRaw(collected[i])
		if !ok {
			continue
		}
		descReplies = append(descReplies, orderedReply{
			raw:       collected[i],
			createdAt: createdAt,
			id:        id,
		})
	}
	return out, descReplies, nil
}

func paginateOrderedReplies(
	descReplies []orderedReply,
	limit int,
	cursor *store.EventOrderCursor,
	offset int,
) ([]json.RawMessage, *store.EventOrderCursor) {
	start := offset
	if cursor != nil {
		start = len(descReplies)
		for idx, reply := range descReplies {
			if reply.id == cursor.ID && reply.createdAt == cursor.CreatedAt {
				start = idx + 1
				break
			}
		}
	}
	if start < 0 {
		start = 0
	}
	if start > len(descReplies) {
		start = len(descReplies)
	}
	end := start + limit
	if end > len(descReplies) {
		end = len(descReplies)
	}
	window := descReplies[start:end]
	out := make([]json.RawMessage, 0, len(window))
	for _, reply := range window {
		out = append(out, reply.raw)
	}
	var next *store.EventOrderCursor
	if end < len(descReplies) && len(window) > 0 {
		last := window[len(window)-1]
		next = &store.EventOrderCursor{
			CreatedAt: last.createdAt,
			ID:        last.id,
		}
	}
	return out, next
}

func toOrderedReplies(values []json.RawMessage) []orderedReply {
	out := make([]orderedReply, 0, len(values))
	for _, value := range values {
		id, createdAt, ok := eventOrderFromRaw(value)
		if !ok {
			continue
		}
		out = append(out, orderedReply{
			raw:       value,
			createdAt: createdAt,
			id:        id,
		})
	}
	return out
}

func mergeOrderedReplies(base []orderedReply, extra []orderedReply) []orderedReply {
	seen := make(map[string]struct{}, len(base)+len(extra))
	merged := make([]orderedReply, 0, len(base)+len(extra))
	appendUnique := func(values []orderedReply) {
		for _, value := range values {
			if value.id == "" {
				continue
			}
			if _, ok := seen[value.id]; ok {
				continue
			}
			seen[value.id] = struct{}{}
			merged = append(merged, value)
		}
	}
	appendUnique(base)
	appendUnique(extra)
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].createdAt == merged[j].createdAt {
			return merged[i].id > merged[j].id
		}
		return merged[i].createdAt > merged[j].createdAt
	})
	return merged
}

func (g WSGateway) buildThreadViewStream(ctx context.Context, thread query.ThreadView, nextCursor string) []any {
	out := make([]any, 0, len(thread.Replies)+len(thread.Ancestors)+4)
	seen := make(map[string]struct{}, len(thread.Replies)+len(thread.Ancestors)+1)
	appendUnique := func(values []json.RawMessage) {
		for _, value := range values {
			id := eventIDFromRaw(value)
			if id != "" {
				if _, ok := seen[id]; ok {
					continue
				}
				seen[id] = struct{}{}
			}
			out = append(out, value)
		}
	}

	// Primal-like stream behavior: reply page first.
	appendUnique(thread.Replies)
	// Include profile metadata for thread members in-stream.
	appendUnique(anyToRawMessages(g.buildMetadataEvents(ctx, collectThreadPubkeys(thread))))
	// Range marker is emitted before parent-chain expansion in stream mode.
	since, until, hasRange := rangeFromEvents(thread.Replies)
	out = append(out, buildThreadRangeEvent(since, until, hasRange, nextCursor))
	// Parent chain and focal event follow.
	appendUnique(thread.Ancestors)
	appendUnique([]json.RawMessage{thread.Event})
	return out
}

func collectThreadPubkeys(thread query.ThreadView) []string {
	raws := make([]json.RawMessage, 0, len(thread.Replies)+len(thread.Ancestors)+1)
	raws = append(raws, thread.Replies...)
	raws = append(raws, thread.Ancestors...)
	raws = append(raws, thread.Event)
	seen := make(map[string]struct{}, len(raws))
	out := make([]string, 0, len(raws))
	for _, raw := range raws {
		var payload struct {
			Pubkey string `json:"pubkey"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		pubkey := strings.TrimSpace(payload.Pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		out = append(out, pubkey)
	}
	return out
}

func anyToRawMessages(values []any) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(values))
	for _, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			continue
		}
		out = append(out, json.RawMessage(raw))
	}
	return out
}

func eventIDFromRaw(raw json.RawMessage) string {
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.TrimSpace(payload.ID)
}

func eventOrderFromRaw(raw json.RawMessage) (string, int64, bool) {
	var payload struct {
		ID        string `json:"id"`
		CreatedAt int64  `json:"created_at"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return "", 0, false
	}
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" {
		return "", 0, false
	}
	return payload.ID, payload.CreatedAt, true
}

func tagValuesFromRawEvent(raw json.RawMessage, tagName string) []string {
	var payload struct {
		Tags []any `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return []string{}
	}
	out := make([]string, 0, len(payload.Tags))
	for _, rawTag := range payload.Tags {
		fields, ok := rawTag.([]any)
		if !ok || len(fields) < 2 {
			continue
		}
		name, okName := fields[0].(string)
		value, okValue := fields[1].(string)
		if !okName || !okValue {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), tagName) {
			value = strings.TrimSpace(value)
			if value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func buildThreadRangeEvent(since int64, until int64, ok bool, nextCursor string) map[string]any {
	payload := map[string]any{
		"order_by": "created_at",
	}
	if ok {
		payload["since"] = since
		payload["until"] = until
	}
	payload["next_cursor"] = nextCursor
	contentRaw, _ := json.Marshal(payload)
	return map[string]any{
		"kind":    primalKindRange,
		"content": string(contentRaw),
	}
}

func sortAndLimitEvents(values []json.RawMessage, limit int) []json.RawMessage {
	type orderedEvent struct {
		raw       json.RawMessage
		id        string
		createdAt int64
	}
	seen := make(map[string]struct{}, len(values))
	ordered := make([]orderedEvent, 0, len(values))
	for _, value := range values {
		id, createdAt, ok := eventOrderFromRaw(value)
		if !ok {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, orderedEvent{
			raw:       value,
			id:        id,
			createdAt: createdAt,
		})
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].createdAt == ordered[j].createdAt {
			return ordered[i].id > ordered[j].id
		}
		return ordered[i].createdAt > ordered[j].createdAt
	})
	if limit > 0 && len(ordered) > limit {
		ordered = ordered[:limit]
	}
	out := make([]json.RawMessage, 0, len(ordered))
	for _, item := range ordered {
		out = append(out, item.raw)
	}
	return out
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

func toBoundedPositiveInt(v any, fallback int, max int) int {
	value := toInt(v, fallback)
	if value <= 0 {
		return fallback
	}
	if value > max {
		return max
	}
	return value
}

func toBoundedNonNegativeInt(v any, fallback int, max int) int {
	value := fallback
	switch typed := v.(type) {
	case int:
		value = typed
	case float64:
		value = int(typed)
	}
	if value < 0 {
		value = 0
	}
	if value > max {
		value = max
	}
	return value
}

func compatIdentifierValue(kwargs map[string]any) (string, bool, error) {
	if value, ok := kwargs["identifier"]; ok {
		identifier, ok := value.(string)
		if !ok {
			return "", false, errors.New("identifier is not a string")
		}
		return strings.TrimSpace(identifier), true, nil
	}
	if value, ok := kwargs["d_tag"]; ok {
		identifier, ok := value.(string)
		if !ok {
			return "", false, errors.New("d_tag is not a string")
		}
		return strings.TrimSpace(identifier), true, nil
	}
	return "", false, nil
}

type parameterizedReplaceableRef struct {
	pubkey     string
	kind       int
	identifier string
}

func parseParameterizedReplaceableRefs(raw any) ([]parameterizedReplaceableRef, error) {
	values, ok := raw.([]any)
	if !ok {
		return nil, errors.New("events must be an array")
	}
	out := make([]parameterizedReplaceableRef, 0, len(values))
	for _, value := range values {
		entry, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("event entry must be an object")
		}
		pubkey := strings.TrimSpace(stringValue(entry["pubkey"]))
		kind := toInt(entry["kind"], 0)
		identifier, hasIdentifier, err := compatIdentifierValue(entry)
		if err != nil {
			return nil, err
		}
		if pubkey == "" || kind <= 0 || !hasIdentifier {
			return nil, errors.New("event entry must include pubkey, kind and identifier")
		}
		out = append(out, parameterizedReplaceableRef{
			pubkey:     pubkey,
			kind:       kind,
			identifier: identifier,
		})
	}
	return out, nil
}

func optionalStringValue(v any) (string, error) {
	if v == nil {
		return "", nil
	}
	value, ok := v.(string)
	if !ok {
		return "", errors.New("value is not a string")
	}
	return strings.TrimSpace(value), nil
}

func toInt64(v any, fallback int64) int64 {
	switch value := v.(type) {
	case int:
		if value >= 0 {
			return int64(value)
		}
	case int64:
		if value >= 0 {
			return value
		}
	case float64:
		casted := int64(value)
		if casted >= 0 {
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

func isDirectMessagesRequest(filters []any) bool {
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			continue
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			continue
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			continue
		}
		name, _ := cacheArgs[0].(string)
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "get_directmsgs", "directmsg_count", "directmsg_count_2", "reset_directmsg_count", "reset_directmsg_counts", "get_directmsg_contacts":
			return true
		}
	}
	return false
}

func parseDMLiveSubscription(subID string, filters []any) (dmLiveSubscription, bool) {
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			continue
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			continue
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			continue
		}
		name, _ := cacheArgs[0].(string)
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "directmsg_count" && name != "directmsg_count_2" {
			continue
		}
		kwargs := map[string]any{}
		if len(cacheArgs) > 1 {
			if m, ok := cacheArgs[1].(map[string]any); ok {
				kwargs = m
			}
		}
		receiver, _ := kwargs["pubkey"].(string)
		receiver = strings.TrimSpace(receiver)
		if receiver == "" {
			return dmLiveSubscription{}, false
		}
		if err := validatePubkeyHex(receiver); err != nil {
			return dmLiveSubscription{}, false
		}
		sender, _ := kwargs["sender"].(string)
		sender = strings.TrimSpace(sender)
		if sender != "" {
			if err := validatePubkeyHex(sender); err != nil {
				return dmLiveSubscription{}, false
			}
		}
		return dmLiveSubscription{
			SubID:    subID,
			Kind:     name,
			Receiver: receiver,
			Sender:   sender,
		}, true
	}
	return dmLiveSubscription{}, false
}

func hasOnlyDMLiveFilters(filters []any) bool {
	if len(filters) == 0 {
		return false
	}
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			return false
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			return false
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			return false
		}
		name, _ := cacheArgs[0].(string)
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "directmsg_count" && name != "directmsg_count_2" {
			return false
		}
	}
	return true
}

func validatePubkeyHex(pubkey string) error {
	pubkey = strings.TrimSpace(pubkey)
	if len(pubkey) != 64 {
		return errors.New("invalid pubkey")
	}
	for _, r := range pubkey {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		if r >= 'A' && r <= 'F' {
			continue
		}
		return fmt.Errorf("invalid pubkey")
	}
	return nil
}

func parseAndValidateDMResetAuth(kwargs map[string]any) (receiver string, sender string, err error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", "", errors.New("event from the future")
	}
	receiver = strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", "", err
	}
	sender, _ = kwargs["peer_pubkey"].(string)
	if strings.TrimSpace(sender) == "" {
		sender, _ = kwargs["sender"].(string)
	}
	if err := validatePubkeyHex(sender); err != nil {
		return "", "", err
	}
	return receiver, sender, nil
}

func parseAndValidateDMResetAllAuth(kwargs map[string]any) (string, error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", errors.New("event from the future")
	}
	receiver := strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", err
	}
	return receiver, nil
}

func (g WSGateway) resolveDMCountFrame(ctx context.Context, sub dmLiveSubscription) ([]any, error) {
	count, err := g.query.GetDirectMessageCount(ctx, sub.Receiver, sub.Sender)
	if err != nil {
		return nil, err
	}
	if sub.Kind == "directmsg_count_2" {
		return []any{"EVENT", sub.SubID, buildDirectMessageCount2Event(count)}, nil
	}
	return []any{"EVENT", sub.SubID, buildDirectMessageCountEvent(count)}, nil
}

type dmContactDetails struct {
	PeerPubkey    string `json:"peer_pubkey"`
	Cnt           int64  `json:"cnt"`
	LatestAt      int64  `json:"latest_at"`
	LatestEventID string `json:"latest_event_id"`
}

func buildDirectMessageCountEvent(count int64) map[string]any {
	return map[string]any{
		"kind": primalKindDirectMsgCount,
		"cnt":  count,
	}
}

func buildDirectMessageCount2Event(count int64) map[string]any {
	contentRaw, _ := json.Marshal(count)
	return map[string]any{
		"kind":    primalKindDirectMsgCount2,
		"content": string(contentRaw),
	}
}

func parseDirectMessageContactsRelation(raw any) (string, error) {
	relation, _ := raw.(string)
	relation = strings.ToLower(strings.TrimSpace(relation))
	if relation == "" {
		return "any", nil
	}
	switch relation {
	case "any", "follows", "other":
		return relation, nil
	default:
		return "", errors.New("invalid relation")
	}
}

func (g WSGateway) buildDirectMessageContactsPayload(ctx context.Context, pubkey string, relation string, values []json.RawMessage) ([]any, error) {
	follows := map[string]struct{}{}
	if relation != "any" {
		if contactList, err := g.query.GetContactList(ctx, pubkey); err == nil {
			follows = parseContactListPubkeys(contactList.ContactsJSONRaw)
		}
	}
	contacts := make([]dmContactDetails, 0, len(values))
	for _, raw := range values {
		var contact dmContactDetails
		if err := json.Unmarshal(raw, &contact); err != nil {
			continue
		}
		contact.PeerPubkey = strings.TrimSpace(contact.PeerPubkey)
		if contact.PeerPubkey == "" {
			continue
		}
		if relation == "follows" {
			if _, ok := follows[contact.PeerPubkey]; !ok {
				continue
			}
		}
		if relation == "other" {
			if _, ok := follows[contact.PeerPubkey]; ok {
				continue
			}
		}
		contacts = append(contacts, contact)
	}
	content := make(map[string]any, len(contacts))
	peerPubkeys := make([]string, 0, len(contacts))
	latestIDs := make([]string, 0, len(contacts))
	seenPeer := make(map[string]struct{}, len(contacts))
	seenLatest := make(map[string]struct{}, len(contacts))
	for _, contact := range contacts {
		content[contact.PeerPubkey] = map[string]any{
			"cnt":             contact.Cnt,
			"latest_at":       contact.LatestAt,
			"latest_event_id": contact.LatestEventID,
		}
		if _, ok := seenPeer[contact.PeerPubkey]; !ok {
			seenPeer[contact.PeerPubkey] = struct{}{}
			peerPubkeys = append(peerPubkeys, contact.PeerPubkey)
		}
		id := strings.TrimSpace(contact.LatestEventID)
		if id == "" {
			continue
		}
		if _, ok := seenLatest[id]; ok {
			continue
		}
		seenLatest[id] = struct{}{}
		latestIDs = append(latestIDs, id)
	}
	contentRaw, _ := json.Marshal(content)
	out := []any{map[string]any{
		"kind":    primalKindDirectMsgCounts,
		"content": string(contentRaw),
	}}
	if len(latestIDs) > 0 {
		if found, err := g.query.GetEventBatch(ctx, latestIDs); err == nil {
			for _, id := range latestIDs {
				if raw, ok := found[id]; ok {
					out = append(out, raw)
				}
			}
		}
	}
	out = append(out, g.buildMetadataEvents(ctx, peerPubkeys)...)
	since, until, hasRange := rangeFromContactDetails(contacts)
	out = append(out, buildRangeEvent("latest_at", since, until, hasRange))
	return out, nil
}

func parseContactListPubkeys(raw json.RawMessage) map[string]struct{} {
	out := map[string]struct{}{}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" {
				out[value] = struct{}{}
			}
		}
		return out
	}
	var generic []any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return out
	}
	for _, value := range generic {
		switch typed := value.(type) {
		case string:
			typed = strings.TrimSpace(typed)
			if typed != "" {
				out[typed] = struct{}{}
			}
		case map[string]any:
			pubkey, _ := typed["pubkey"].(string)
			pubkey = strings.TrimSpace(pubkey)
			if pubkey != "" {
				out[pubkey] = struct{}{}
			}
		}
	}
	return out
}

type moderationListKind string

const (
	moderationListMute      moderationListKind = "mutelist"
	moderationListMutelists moderationListKind = "mutelists"
	moderationListAllowlist moderationListKind = "allowlist"
)

type moderationListSpec struct {
	kind int
	dTag string
}

type moderationTag struct {
	name  string
	value string
}

func (g WSGateway) buildModerationListResponse(ctx context.Context, pubkey string, listKind moderationListKind) ([]any, error) {
	events, err := g.getModerationListEvents(ctx, pubkey, listKind)
	if err != nil {
		return nil, errors.New("request failed")
	}
	out := rawMessagesToAny(events)
	pubkeys := moderationTagValues(events, "p")
	out = append(out, g.buildMetadataEvents(ctx, pubkeys)...)
	return out, nil
}

func (g WSGateway) buildSearchFilterlistResponse(ctx context.Context, kwargs map[string]any) ([]any, error) {
	targetPubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	viewerPubkey := strings.TrimSpace(stringValue(kwargs["user_pubkey"]))
	if viewerPubkey == "" {
		viewerPubkey = targetPubkey
	}
	muteEvents, err := g.getModerationListEvents(ctx, viewerPubkey, moderationListMute)
	if err != nil {
		return nil, errors.New("request failed")
	}
	allowEvents, err := g.getModerationListEvents(ctx, viewerPubkey, moderationListAllowlist)
	if err != nil {
		return nil, errors.New("request failed")
	}

	var reason map[string]any
	if targetPubkey != "" {
		if moderationListContainsTagValue(allowEvents, "p", targetPubkey) {
			reason = map[string]any{"action": "allow", "pubkey": viewerPubkey, "target_pubkey": targetPubkey}
		} else if moderationListContainsTagValue(muteEvents, "p", targetPubkey) {
			reason = map[string]any{"action": "block", "pubkey": viewerPubkey, "target_pubkey": targetPubkey}
		}
	}
	queryText := strings.TrimSpace(stringValue(kwargs["query"]))
	if reason == nil && queryText != "" {
		matchedTerms := moderationTermsMatchingQuery(muteEvents, queryText)
		if len(matchedTerms) > 0 {
			reason = map[string]any{
				"action":        "block",
				"query":         queryText,
				"matched_terms": matchedTerms,
				"term":          matchedTerms[0],
			}
		}
	}
	if reason == nil {
		return []any{}, nil
	}
	out := make([]any, 0, 2)
	if sourcePubkey := strings.TrimSpace(stringValue(reason["pubkey"])); sourcePubkey != "" {
		out = append(out, g.buildMetadataEvents(ctx, []string{sourcePubkey})...)
	}
	out = append(out, buildFilteringReasonEvent(reason))
	return out, nil
}

func (g WSGateway) buildHiddenByContentModerationResponse(ctx context.Context, kwargs map[string]any) ([]any, error) {
	viewer := strings.TrimSpace(stringValue(kwargs["user_pubkey"]))
	if viewer == "" {
		viewer = strings.TrimSpace(stringValue(kwargs["pubkey"]))
	}
	pubkeys := toStringSlice(kwargs["pubkeys"])
	if strings.TrimSpace(stringValue(kwargs["user_pubkey"])) != "" {
		if single := strings.TrimSpace(stringValue(kwargs["pubkey"])); single != "" {
			pubkeys = append(pubkeys, single)
		}
	}
	if single := strings.TrimSpace(stringValue(kwargs["target_pubkey"])); single != "" {
		pubkeys = append(pubkeys, single)
	}
	pubkeys = uniqueTrimmedStrings(pubkeys)

	eventIDs := toStringSlice(kwargs["event_ids"])
	singleEventID := strings.TrimSpace(stringValue(kwargs["event_id"]))
	if singleEventID != "" {
		eventIDs = append(eventIDs, singleEventID)
	}
	eventIDs = uniqueTrimmedStrings(eventIDs)

	pubkeyHidden := make(map[string]bool, len(pubkeys))
	pubkeyReasons := make(map[string]string, len(pubkeys))
	muteEvents := []json.RawMessage{}
	allowEvents := []json.RawMessage{}
	if viewer != "" && len(pubkeys) > 0 {
		var err error
		muteEvents, err = g.getModerationListEvents(ctx, viewer, moderationListMute)
		if err != nil {
			return nil, errors.New("request failed")
		}
		allowEvents, err = g.getModerationListEvents(ctx, viewer, moderationListAllowlist)
		if err != nil {
			return nil, errors.New("request failed")
		}
	}
	for _, pubkey := range pubkeys {
		if moderationListContainsTagValue(allowEvents, "p", pubkey) {
			pubkeyHidden[pubkey] = false
			pubkeyReasons[pubkey] = "allowed_pubkey:" + pubkey
			continue
		}
		if moderationListContainsTagValue(muteEvents, "p", pubkey) {
			pubkeyHidden[pubkey] = true
			pubkeyReasons[pubkey] = "muted_pubkey:" + pubkey
			continue
		}
		pubkeyHidden[pubkey] = false
		pubkeyReasons[pubkey] = ""
	}

	eventHidden := make(map[string]bool, len(eventIDs))
	eventReasons := make(map[string]string, len(eventIDs))
	for _, eventID := range eventIDs {
		hidden, reason, err := g.query.IsHiddenByContentModeration(ctx, viewer, eventID)
		if err != nil {
			if query.IsNotFound(err) {
				eventHidden[eventID] = false
				eventReasons[eventID] = ""
				continue
			}
			return nil, errors.New("request failed")
		}
		eventHidden[eventID] = hidden
		eventReasons[eventID] = reason
	}

	contentPayload := map[string]any{
		"pubkeys":   pubkeyHidden,
		"event_ids": eventHidden,
		"reasons": map[string]any{
			"pubkeys":   pubkeyReasons,
			"event_ids": eventReasons,
		},
	}
	contentRaw, _ := json.Marshal(contentPayload)
	eventPayload := map[string]any{
		"kind":      primalKindHiddenByContent,
		"content":   string(contentRaw),
		"pubkeys":   pubkeyHidden,
		"event_ids": eventHidden,
		"reasons": map[string]any{
			"pubkeys":   pubkeyReasons,
			"event_ids": eventReasons,
		},
	}
	if singleEventID != "" {
		eventPayload["event_id"] = singleEventID
		eventPayload["hidden"] = eventHidden[singleEventID]
		eventPayload["reason"] = eventReasons[singleEventID]
	}
	return []any{eventPayload}, nil
}

func buildFilteringReasonEvent(reason map[string]any) map[string]any {
	contentRaw, _ := json.Marshal(reason)
	return map[string]any{
		"kind":    primalKindFilteringReason,
		"content": string(contentRaw),
	}
}

func (g WSGateway) getModerationListEvents(ctx context.Context, pubkey string, listKind moderationListKind) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return []json.RawMessage{}, nil
	}
	specs := moderationListSpecs(listKind)
	out := make([]json.RawMessage, 0, len(specs))
	for _, spec := range specs {
		event, ok, err := g.getModerationReplaceableEvent(ctx, pubkey, spec.kind, spec.dTag)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, event)
		}
	}
	return out, nil
}

func moderationListSpecs(listKind moderationListKind) []moderationListSpec {
	switch listKind {
	case moderationListMute:
		return []moderationListSpec{
			{kind: 10000, dTag: ""},
			{kind: 30000, dTag: "mute"},
		}
	case moderationListMutelists:
		return []moderationListSpec{
			{kind: 30000, dTag: "mutelists"},
		}
	case moderationListAllowlist:
		return []moderationListSpec{
			{kind: 30000, dTag: "allowlist"},
			{kind: 10001, dTag: ""},
		}
	default:
		return []moderationListSpec{}
	}
}

func (g WSGateway) getModerationReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, bool, error) {
	event, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, dTag)
	if err == nil {
		return event, true, nil
	}
	if query.IsNotFound(err) || strings.Contains(strings.ToLower(err.Error()), "not implemented") {
		return nil, false, nil
	}
	return nil, false, err
}

func moderationListContainsTagValue(events []json.RawMessage, tagName string, value string) bool {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return false
	}
	for _, tag := range moderationTagsFromEvents(events) {
		if tag.name != tagName {
			continue
		}
		if strings.TrimSpace(strings.ToLower(tag.value)) == value {
			return true
		}
	}
	return false
}

func moderationTagValues(events []json.RawMessage, tagName string) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0)
	for _, tag := range moderationTagsFromEvents(events) {
		if tag.name != tagName {
			continue
		}
		if _, ok := seen[tag.value]; ok {
			continue
		}
		seen[tag.value] = struct{}{}
		out = append(out, tag.value)
	}
	return out
}

func moderationTermsMatchingQuery(events []json.RawMessage, queryText string) []string {
	queryText = strings.TrimSpace(strings.ToLower(queryText))
	if queryText == "" {
		return []string{}
	}
	out := make([]string, 0)
	seen := make(map[string]struct{})
	for _, tag := range moderationTagsFromEvents(events) {
		if tag.name != "t" && tag.name != "word" {
			continue
		}
		term := strings.TrimSpace(strings.ToLower(tag.value))
		if term == "" || !strings.Contains(term, queryText) {
			continue
		}
		if _, ok := seen[tag.value]; ok {
			continue
		}
		seen[tag.value] = struct{}{}
		out = append(out, tag.value)
	}
	return out
}

func moderationTagsFromEvents(events []json.RawMessage) []moderationTag {
	out := make([]moderationTag, 0)
	for _, raw := range events {
		out = append(out, moderationTagsFromRaw(raw)...)
	}
	return out
}

func moderationTagsFromRaw(raw json.RawMessage) []moderationTag {
	var payload struct {
		Tags []any `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	out := make([]moderationTag, 0, len(payload.Tags))
	for _, rawTag := range payload.Tags {
		fields, ok := rawTag.([]any)
		if !ok || len(fields) < 2 {
			continue
		}
		name, okName := fields[0].(string)
		value, okValue := fields[1].(string)
		if !okName || !okValue {
			continue
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if name == "" || value == "" {
			continue
		}
		out = append(out, moderationTag{name: name, value: value})
	}
	return out
}

func uniqueTrimmedStrings(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func stringValue(v any) string {
	value, _ := v.(string)
	return value
}

func (g WSGateway) buildDirectMessagesPayload(ctx context.Context, receiver string, sender string, values []json.RawMessage) []any {
	out := make([]any, 0, len(values)+3)
	for _, value := range values {
		out = append(out, value)
	}
	out = append(out, g.buildMetadataEvents(ctx, []string{receiver, sender})...)
	since, until, hasRange := rangeFromEvents(values)
	out = append(out, buildRangeEvent("created_at", since, until, hasRange))
	return out
}

func (g WSGateway) buildMetadataEvents(ctx context.Context, pubkeys []string) []any {
	normalized := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		normalized = append(normalized, pubkey)
	}
	if len(normalized) == 0 {
		return nil
	}
	infos, err := g.query.GetUserInfos(ctx, normalized)
	if err != nil {
		return nil
	}
	metadataIDs := make([]string, 0, len(infos.Profiles))
	for _, profile := range infos.Profiles {
		id := strings.TrimSpace(profile.MetadataEventID)
		if id != "" {
			metadataIDs = append(metadataIDs, id)
		}
	}
	if len(metadataIDs) == 0 {
		return nil
	}
	rawByID, err := g.query.GetEventBatch(ctx, metadataIDs)
	if err != nil {
		return nil
	}
	out := make([]any, 0, len(metadataIDs))
	for _, id := range metadataIDs {
		if raw, ok := rawByID[id]; ok {
			out = append(out, raw)
		}
	}
	return out
}

func (g WSGateway) buildEventsWithMetadataAndRange(ctx context.Context, values []json.RawMessage, orderBy string) []any {
	out := make([]any, 0, len(values)+3)
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		id := eventIDFromRaw(value)
		if id != "" {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
		}
		out = append(out, value)
	}
	out = append(out, g.buildMetadataEvents(ctx, collectPubkeysFromEvents(values))...)
	since, until, hasRange := rangeFromEvents(values)
	out = append(out, buildRangeEvent(orderBy, since, until, hasRange))
	return out
}

func collectPubkeysFromEvents(values []json.RawMessage) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
		var payload struct {
			Pubkey string `json:"pubkey"`
		}
		if err := json.Unmarshal(raw, &payload); err != nil {
			continue
		}
		pubkey := strings.TrimSpace(payload.Pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		out = append(out, pubkey)
	}
	return out
}

func buildRangeEvent(orderBy string, since int64, until int64, ok bool) map[string]any {
	payload := map[string]any{"order_by": orderBy}
	if ok {
		payload["since"] = since
		payload["until"] = until
	}
	contentRaw, _ := json.Marshal(payload)
	return map[string]any{
		"kind":    primalKindRange,
		"content": string(contentRaw),
	}
}

func rangeFromEvents(values []json.RawMessage) (int64, int64, bool) {
	var since int64
	var until int64
	found := false
	for _, raw := range values {
		var event struct {
			CreatedAt int64 `json:"created_at"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			continue
		}
		if !found {
			since = event.CreatedAt
			until = event.CreatedAt
			found = true
			continue
		}
		if event.CreatedAt < since {
			since = event.CreatedAt
		}
		if event.CreatedAt > until {
			until = event.CreatedAt
		}
	}
	return since, until, found
}

func rangeFromContactDetails(values []dmContactDetails) (int64, int64, bool) {
	var since int64
	var until int64
	found := false
	for _, value := range values {
		if !found {
			since = value.LatestAt
			until = value.LatestAt
			found = true
			continue
		}
		if value.LatestAt < since {
			since = value.LatestAt
		}
		if value.LatestAt > until {
			until = value.LatestAt
		}
	}
	return since, until, found
}

func buildCuratedListEvent(kind int, payload map[string]any) map[string]any {
	contentRaw, _ := json.Marshal(payload)
	return map[string]any{
		"kind":    kind,
		"content": string(contentRaw),
	}
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
