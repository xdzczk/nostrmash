package api_primal

import (
	"context"
	"encoding/json"
	"errors"
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
