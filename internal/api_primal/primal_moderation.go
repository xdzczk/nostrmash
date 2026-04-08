package api_primal

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

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
		return nil, wrapPrimalRequestError(err)
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
		return nil, wrapPrimalRequestError(err)
	}
	allowEvents, err := g.getModerationListEvents(ctx, viewerPubkey, moderationListAllowlist)
	if err != nil {
		return nil, wrapPrimalRequestError(err)
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
			return nil, wrapPrimalRequestError(err)
		}
		allowEvents, err = g.getModerationListEvents(ctx, viewer, moderationListAllowlist)
		if err != nil {
			return nil, wrapPrimalRequestError(err)
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
			if query.IsUnsupportedCapability(err) {
				// Compatibility: legacy clients expect this cache call to resolve with a payload.
				eventHidden[eventID] = false
				eventReasons[eventID] = ""
				continue
			}
			return nil, wrapPrimalRequestError(err)
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
