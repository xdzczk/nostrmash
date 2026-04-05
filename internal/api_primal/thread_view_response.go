package api_primal

import "encoding/json"

func buildThreadViewResponse(
	eventID string,
	event json.RawMessage,
	ancestors []json.RawMessage,
	missingAncestorIDs []string,
	replies []json.RawMessage,
	nextCursor string,
	consistency string,
) map[string]any {
	if consistency == "" {
		consistency = "eventual"
	}
	return map[string]any{
		"event_id":             eventID,
		"event":                event,
		"ancestors":            ancestors,
		"missing_ancestor_ids": missingAncestorIDs,
		"replies":              replies,
		"next_cursor":          nextCursor,
		"consistency":          consistency,
	}
}
