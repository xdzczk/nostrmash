package store

import (
	"encoding/json"
	"strconv"
	"strings"
)

func enrichZapRecords(records []json.RawMessage) []json.RawMessage {
	if len(records) == 0 {
		return records
	}
	out := make([]json.RawMessage, 0, len(records))
	for _, record := range records {
		out = append(out, enrichZapRecord(record))
	}
	return out
}

func enrichZapRecord(record json.RawMessage) json.RawMessage {
	var obj map[string]any
	if err := json.Unmarshal(record, &obj); err != nil {
		return record
	}

	var eventRaw json.RawMessage
	if raw, ok := obj["event"]; ok {
		switch typed := raw.(type) {
		case json.RawMessage:
			eventRaw = typed
		case string:
			eventRaw = json.RawMessage(typed)
		default:
			if encoded, err := json.Marshal(typed); err == nil {
				eventRaw = encoded
			}
		}
	}

	msats, zapText := extractZapRequestDetails(eventRaw)
	if msats == 0 {
		switch sats := obj["sats"].(type) {
		case float64:
			if sats > 0 {
				msats = int64(sats) * 1000
			}
		case int64:
			if sats > 0 {
				msats = sats * 1000
			}
		}
	}
	if msats > 0 {
		obj["msats"] = msats
		obj["sats"] = msats / 1000
	}
	if zapText != "" {
		obj["zap_text"] = zapText
	}
	if targetEvent, ok := obj["target_event"]; ok && targetEvent == nil {
		delete(obj, "target_event")
	}

	out, err := json.Marshal(obj)
	if err != nil {
		return record
	}
	return out
}

func extractZapRequestDetails(eventRaw json.RawMessage) (msats int64, zapText string) {
	if len(eventRaw) == 0 {
		return 0, ""
	}
	var event struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(eventRaw, &event); err != nil {
		return 0, ""
	}

	msats = parseZapAmountMillisats(firstTagValueFromTags(event.Tags, "amount"))
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "description" {
			continue
		}
		var req struct {
			Content string     `json:"content"`
			Tags    [][]string `json:"tags"`
		}
		if err := json.Unmarshal([]byte(tag[1]), &req); err != nil {
			continue
		}
		zapText = strings.TrimSpace(req.Content)
		if amount := parseZapAmountMillisats(firstTagValueFromTags(req.Tags, "amount")); amount > 0 {
			msats = amount
		}
	}
	return msats, zapText
}

func firstTagValueFromTags(tags [][]string, tagName string) string {
	for _, tag := range tags {
		if len(tag) < 2 {
			continue
		}
		if tag[0] != tagName {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value != "" {
			return value
		}
	}
	return ""
}

func parseZapAmountMillisats(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	amount, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || amount <= 0 {
		return 0
	}
	if amount >= 1000 {
		return amount
	}
	return amount * 1000
}
