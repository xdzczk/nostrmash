package query

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
)

type orderedReply struct {
	raw       json.RawMessage
	createdAt int64
	id        string
}

func toDescendingReplies(values []json.RawMessage) []orderedReply {
	out := make([]orderedReply, 0, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		var payload struct {
			ID        string `json:"id"`
			CreatedAt int64  `json:"created_at"`
		}
		if err := json.Unmarshal(values[i], &payload); err != nil {
			continue
		}
		payload.ID = strings.TrimSpace(payload.ID)
		if payload.ID == "" {
			continue
		}
		out = append(out, orderedReply{
			raw:       values[i],
			createdAt: payload.CreatedAt,
			id:        payload.ID,
		})
	}
	return out
}

func paginateReplies(
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
		var payload struct {
			ID        string `json:"id"`
			CreatedAt int64  `json:"created_at"`
		}
		if err := json.Unmarshal(value, &payload); err != nil {
			continue
		}
		payload.ID = strings.TrimSpace(payload.ID)
		if payload.ID == "" {
			continue
		}
		out = append(out, orderedReply{
			raw:       value,
			createdAt: payload.CreatedAt,
			id:        payload.ID,
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

// WindowDescendingReplies merges descending reply collections and applies cursor/offset paging.
func WindowDescendingReplies(
	baseReplies []json.RawMessage,
	extraReplies []json.RawMessage,
	limit int,
	cursor *store.EventOrderCursor,
	offset int,
) ([]json.RawMessage, *store.EventOrderCursor) {
	baseOrdered := toDescendingReplies(baseReplies)
	if len(extraReplies) == 0 {
		return paginateReplies(baseOrdered, limit, cursor, offset)
	}
	extraOrdered := toOrderedReplies(extraReplies)
	merged := mergeOrderedReplies(baseOrdered, extraOrdered)
	return paginateReplies(merged, limit, cursor, offset)
}
