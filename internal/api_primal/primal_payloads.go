package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

func tagValuesFromRawEvent(raw json.RawMessage, tagName string) []string {
	var payload struct {
		Tags []any `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return []string{}
	}
	out := make([]string, 0, len(payload.Tags))
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
		if strings.EqualFold(strings.TrimSpace(name), tagName) {
			value = strings.TrimSpace(value)
			if value != "" {
				out = append(out, value)
			}
		}
	}
	return out
}

func rawMessagesToAny(values []json.RawMessage) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

func rawMessagesToAnyMust(values []json.RawMessage, err error) ([]any, error) {
	if err != nil {
		return nil, errors.New("request failed")
	}
	return rawMessagesToAny(values), nil
}

func (g WSGateway) buildMetadataEvents(ctx context.Context, pubkeys []string) []any {
	normalized := make([]string, 0, len(pubkeys))
	seen := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		normalized = append(normalized, pubkey)
	}
	if len(normalized) == 0 {
		return nil
	}
	infos, err := g.query.GetUserInfos(ctx, normalized)
	if err != nil {
		return nil
	}
	metadataIDs := make([]string, 0, len(infos.Profiles))
	for _, profile := range infos.Profiles {
		id := strings.TrimSpace(profile.MetadataEventID)
		if id != "" {
			metadataIDs = append(metadataIDs, id)
		}
	}
	if len(metadataIDs) == 0 {
		return nil
	}
	rawByID, err := g.query.GetEventBatch(ctx, metadataIDs)
	if err != nil {
		return nil
	}
	out := make([]any, 0, len(metadataIDs))
	for _, id := range metadataIDs {
		if raw, ok := rawByID[id]; ok {
			out = append(out, raw)
		}
	}
	return out
}

func (g WSGateway) buildEventsWithMetadataAndRange(ctx context.Context, values []json.RawMessage, orderBy string) []any {
	out := make([]any, 0, len(values)+3)
	seen := make(map[string]struct{}, len(values))
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
	out = append(out, g.buildMetadataEvents(ctx, collectPubkeysFromEvents(values))...)
	since, until, hasRange := rangeFromEvents(values)
	out = append(out, buildRangeEvent(orderBy, since, until, hasRange))
	return out
}

func collectPubkeysFromEvents(values []json.RawMessage) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, raw := range values {
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

func buildRangeEvent(orderBy string, since int64, until int64, ok bool) map[string]any {
	payload := map[string]any{"order_by": orderBy}
	if ok {
		payload["since"] = since
		payload["until"] = until
	}
	contentRaw, _ := json.Marshal(payload)
	return map[string]any{
		"kind":    primalKindRange,
		"content": string(contentRaw),
	}
}

func rangeFromEvents(values []json.RawMessage) (int64, int64, bool) {
	var since int64
	var until int64
	found := false
	for _, raw := range values {
		var event struct {
			CreatedAt int64 `json:"created_at"`
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			continue
		}
		if !found {
			since = event.CreatedAt
			until = event.CreatedAt
			found = true
			continue
		}
		if event.CreatedAt < since {
			since = event.CreatedAt
		}
		if event.CreatedAt > until {
			until = event.CreatedAt
		}
	}
	return since, until, found
}

func rangeFromContactDetails(values []dmContactDetails) (int64, int64, bool) {
	var since int64
	var until int64
	found := false
	for _, value := range values {
		if !found {
			since = value.LatestAt
			until = value.LatestAt
			found = true
			continue
		}
		if value.LatestAt < since {
			since = value.LatestAt
		}
		if value.LatestAt > until {
			until = value.LatestAt
		}
	}
	return since, until, found
}

func buildCuratedListEvent(kind int, payload map[string]any) map[string]any {
	contentRaw, _ := json.Marshal(payload)
	return map[string]any{
		"kind":    kind,
		"content": string(contentRaw),
	}
}
