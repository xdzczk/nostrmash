package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

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
	threadBase, collectedReplies, err := g.collectThreadRepliesDescending(ctx, eventID, maxDepth)
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
	window, next := query.WindowDescendingReplies(collectedReplies, aTagReplies, limit, cursor, offset)
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

func (g WSGateway) collectThreadRepliesDescending(
	ctx context.Context,
	eventID string,
	maxDepth int,
) (query.ThreadView, []json.RawMessage, error) {
	const fetchPageSize = 100
	var out query.ThreadView
	var ascCursor *query.EventCursor
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

	return out, collected, nil
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
