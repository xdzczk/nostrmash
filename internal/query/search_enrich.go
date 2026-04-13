package query

import (
	"encoding/json"
	"strings"
)

const maxEnrichmentPubkeys = 20

func extractCandidatePubkeysFromEvents(events []json.RawMessage, limit int) []string {
	if limit <= 0 {
		limit = maxEnrichmentPubkeys
	}
	seen := make(map[string]struct{}, limit)
	pubkeys := make([]string, 0, limit)
	add := func(pk string) bool {
		pk = strings.TrimSpace(pk)
		if pk == "" || len(pk) != 64 {
			return false
		}
		if _, ok := seen[pk]; ok {
			return false
		}
		seen[pk] = struct{}{}
		pubkeys = append(pubkeys, pk)
		return len(pubkeys) >= limit
	}
	for _, raw := range events {
		var envelope struct {
			Pubkey string     `json:"pubkey"`
			Tags   [][]string `json:"tags"`
		}
		if json.Unmarshal(raw, &envelope) != nil {
			continue
		}
		if add(envelope.Pubkey) {
			return pubkeys
		}
		for _, tag := range envelope.Tags {
			if len(tag) >= 2 && tag[0] == "p" {
				if add(tag[1]) {
					return pubkeys
				}
			}
		}
	}
	return pubkeys
}

func profileMatchesTextQuery(p Profile, query string) bool {
	if query == "" {
		return false
	}
	q := strings.ToLower(query)
	var obj map[string]any
	if err := json.Unmarshal(p.ProfileJSON, &obj); err != nil {
		return false
	}
	for _, key := range []string{"name", "display_name", "nip05"} {
		if val, ok := obj[key].(string); ok && strings.Contains(strings.ToLower(val), q) {
			return true
		}
	}
	return false
}
