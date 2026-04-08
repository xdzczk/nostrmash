package api_primal

import (
	"encoding/json"
	"strings"
)

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
