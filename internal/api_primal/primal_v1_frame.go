package api_primal

import (
	"bytes"
	"compress/zlib"
	_ "embed"
	"encoding/json"
	"strings"
)

//go:embed default_settings.json
var defaultSettingsJSON []byte

func encodeZlibEvents(subID string, events []any) ([]byte, error) {
	raw, err := json.Marshal([]any{"EVENTS", subID, events})
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	writer := zlib.NewWriter(&buf)
	if _, err := writer.Write(raw); err != nil {
		_ = writer.Close()
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func cacheCallFromFilters(filters []any) (string, map[string]any) {
	for _, raw := range filters {
		filter, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			continue
		}
		name, kwargs, err := parseCacheCallFilter(cacheRaw)
		if err != nil {
			continue
		}
		return strings.ToLower(strings.TrimSpace(name)), kwargs
	}
	return "", nil
}

func filtersUseKindOrAuthor(filters []any) bool {
	for _, raw := range filters {
		filter, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if _, ok := filter["kinds"]; ok {
			return true
		}
		if _, ok := filter["authors"]; ok {
			return true
		}
	}
	return false
}

func isBroadcastRequest(filters []any) bool {
	name, _ := cacheCallFromFilters(filters)
	return name == "broadcast_events" || name == "broadcast_reply"
}

func kindContent(kind int, content string) map[string]any {
	return map[string]any{"kind": kind, "content": content}
}

func kindObject(kind int, payload any) map[string]any {
	raw, err := json.Marshal(payload)
	if err != nil {
		raw = []byte("null")
	}
	return kindContent(kind, string(raw))
}

func boolValue(v any, fallback bool) bool {
	switch typed := v.(type) {
	case bool:
		return typed
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "true", "1":
			return true
		case "false", "0":
			return false
		}
	}
	return fallback
}

func toIntSlice(v any) []int {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int, 0, len(items))
	for _, item := range items {
		value := toInt(item, 0)
		if value > 0 {
			out = append(out, value)
		}
	}
	return out
}

func kindOf(v any) (int, bool) {
	switch typed := v.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(parsed), true
	default:
		return 0, false
	}
}

func pubkeyFromRaw(raw json.RawMessage) string {
	var payload struct {
		Pubkey string `json:"pubkey"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(payload.Pubkey))
}

func createdAtFromRaw(raw json.RawMessage) (int64, bool) {
	var payload struct {
		CreatedAt int64 `json:"created_at"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, false
	}
	return payload.CreatedAt, true
}

func buildPrimalRange(orderBy string, since any, until any, elements []string) map[string]any {
	if elements == nil {
		elements = []string{}
	}
	return kindObject(primalKindRange, map[string]any{
		"since":    since,
		"until":    until,
		"order_by": orderBy,
		"elements": elements,
	})
}

func mergeJSONObjects(base json.RawMessage, overlay json.RawMessage) json.RawMessage {
	left := map[string]any{}
	right := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &left)
	}
	if err := json.Unmarshal(overlay, &right); err != nil {
		return overlay
	}
	if left == nil {
		left = map[string]any{}
	}
	for key, value := range right {
		left[key] = value
	}
	out, err := json.Marshal(left)
	if err != nil {
		return overlay
	}
	return out
}

func objectKeysInOrder(raw string) []string {
	decoder := json.NewDecoder(strings.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return nil
	}
	keys := make([]string, 0)
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			break
		}
		key, _ := keyToken.(string)
		var skip json.RawMessage
		if err := decoder.Decode(&skip); err != nil {
			break
		}
		if strings.TrimSpace(key) != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func attachRangeElements(events []any, elements []string) []any {
	if len(events) == 0 {
		return events
	}
	last, ok := events[len(events)-1].(map[string]any)
	if !ok {
		return events
	}
	kind, ok := kindOf(last["kind"])
	if !ok || kind != primalKindRange {
		return events
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(stringValue(last["content"])), &payload); err != nil || payload == nil {
		payload = map[string]any{}
	}
	if elements == nil {
		elements = []string{}
	}
	payload["elements"] = elements
	raw, err := json.Marshal(payload)
	if err != nil {
		return events
	}
	last["content"] = string(raw)
	events[len(events)-1] = last
	return events
}

func rawEventIDs(events []any) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		raw, ok := event.(json.RawMessage)
		if !ok {
			continue
		}
		if id := eventIDFromRaw(raw); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}
