package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

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
		thread, err := g.query.GetThreadWindow(ctx, query.ThreadWindowRequest{
			EventID:  eventID,
			Limit:    limit,
			MaxDepth: maxDepth,
			Cursor:   cursor,
			Offset:   offset,
		})
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
