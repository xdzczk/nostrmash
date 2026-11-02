package readmodel

import (
	"encoding/json"
	"fmt"
)

// MergeEventCountsIntoRaw attaches eventual engagement counters to a raw event
// payload so list UIs can render note cards without a second /events/{id}/counts
// round-trip.
func MergeEventCountsIntoRaw(raw json.RawMessage, counts EventCounts) (json.RawMessage, error) {
	if len(raw) == 0 {
		return raw, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode event raw for count enrichment: %w", err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["reply_count"] = counts.ReplyCount
	payload["reaction_count"] = counts.ReactionCount
	payload["repost_count"] = counts.RepostCount
	payload["zap_count"] = counts.ZapCount
	payload["zap_msats"] = counts.ZapMSats
	nested := map[string]any{
		"reply_count":    counts.ReplyCount,
		"reaction_count": counts.ReactionCount,
		"repost_count":   counts.RepostCount,
		"zap_count":      counts.ZapCount,
		"zap_msats":      counts.ZapMSats,
		"consistency":    "eventual",
	}
	if counts.QuoteCount > 0 {
		payload["quote_count"] = counts.QuoteCount
		nested["quote_count"] = counts.QuoteCount
	}
	payload["counts"] = nested
	out, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode event raw after count enrichment: %w", err)
	}
	return json.RawMessage(out), nil
}

// MergeQuoteCountIntoRaw sets the additive kind-1 quote total on an event
// payload. Zero is omitted so a missing count stays distinct from a counted one.
func MergeQuoteCountIntoRaw(raw json.RawMessage, count int64) (json.RawMessage, error) {
	if count <= 0 || len(raw) == 0 {
		return raw, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode event raw for quote count: %w", err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	payload["quote_count"] = count
	if nested, ok := payload["counts"].(map[string]any); ok {
		nested["quote_count"] = count
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode event raw after quote count: %w", err)
	}
	return json.RawMessage(out), nil
}
