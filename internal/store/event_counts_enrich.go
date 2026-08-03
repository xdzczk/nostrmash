package store

import (
	"encoding/json"
	"fmt"
)

// mergeEventCountsIntoRaw attaches eventual engagement counters to a raw event
// payload so list UIs (profile activity, author feeds) can render note cards
// without a second /events/{id}/counts round-trip.
func mergeEventCountsIntoRaw(raw json.RawMessage, counts EventCounts) (json.RawMessage, error) {
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
	payload["counts"] = map[string]any{
		"reply_count":    counts.ReplyCount,
		"reaction_count": counts.ReactionCount,
		"repost_count":   counts.RepostCount,
		"zap_count":      counts.ZapCount,
		"zap_msats":      counts.ZapMSats,
		"consistency":    "eventual",
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode event raw after count enrichment: %w", err)
	}
	return json.RawMessage(out), nil
}
