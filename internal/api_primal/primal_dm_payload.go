package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
)

type dmContactDetails struct {
	PeerPubkey    string `json:"peer_pubkey"`
	Cnt           int64  `json:"cnt"`
	LatestAt      int64  `json:"latest_at"`
	LatestEventID string `json:"latest_event_id"`
}

func parseDirectMessageContactsRelation(raw any) (string, error) {
	relation, _ := raw.(string)
	relation = strings.ToLower(strings.TrimSpace(relation))
	if relation == "" {
		return "any", nil
	}
	switch relation {
	case "any", "follows", "other":
		return relation, nil
	default:
		return "", errors.New("invalid relation")
	}
}

func (g WSGateway) buildDirectMessageContactsPayload(ctx context.Context, pubkey string, relation string, values []json.RawMessage) ([]any, error) {
	follows := map[string]struct{}{}
	if relation != "any" {
		if contactList, err := g.query.GetContactList(ctx, pubkey); err == nil {
			follows = parseContactListPubkeys(contactList.ContactsJSONRaw)
		}
	}
	contacts := make([]dmContactDetails, 0, len(values))
	for _, raw := range values {
		var contact dmContactDetails
		if err := json.Unmarshal(raw, &contact); err != nil {
			continue
		}
		contact.PeerPubkey = strings.TrimSpace(contact.PeerPubkey)
		if contact.PeerPubkey == "" {
			continue
		}
		if relation == "follows" {
			if _, ok := follows[contact.PeerPubkey]; !ok {
				continue
			}
		}
		if relation == "other" {
			if _, ok := follows[contact.PeerPubkey]; ok {
				continue
			}
		}
		contacts = append(contacts, contact)
	}
	content := make(map[string]any, len(contacts))
	peerPubkeys := make([]string, 0, len(contacts))
	latestIDs := make([]string, 0, len(contacts))
	seenPeer := make(map[string]struct{}, len(contacts))
	seenLatest := make(map[string]struct{}, len(contacts))
	for _, contact := range contacts {
		content[contact.PeerPubkey] = map[string]any{
			"cnt":             contact.Cnt,
			"latest_at":       contact.LatestAt,
			"latest_event_id": contact.LatestEventID,
		}
		if _, ok := seenPeer[contact.PeerPubkey]; !ok {
			seenPeer[contact.PeerPubkey] = struct{}{}
			peerPubkeys = append(peerPubkeys, contact.PeerPubkey)
		}
		id := strings.TrimSpace(contact.LatestEventID)
		if id == "" {
			continue
		}
		if _, ok := seenLatest[id]; ok {
			continue
		}
		seenLatest[id] = struct{}{}
		latestIDs = append(latestIDs, id)
	}
	contentRaw, _ := json.Marshal(content)
	out := []any{map[string]any{
		"kind":    primalKindDirectMsgCounts,
		"content": string(contentRaw),
	}}
	if len(latestIDs) > 0 {
		if found, err := g.query.GetEventBatch(ctx, latestIDs); err == nil {
			for _, id := range latestIDs {
				if raw, ok := found[id]; ok {
					out = append(out, raw)
				}
			}
		}
	}
	out = append(out, g.buildMetadataEvents(ctx, peerPubkeys)...)
	since, until, hasRange := rangeFromContactDetails(contacts)
	out = append(out, buildRangeEvent("latest_at", since, until, hasRange))
	return out, nil
}

func parseContactListPubkeys(raw json.RawMessage) map[string]struct{} {
	out := map[string]struct{}{}
	var values []string
	if err := json.Unmarshal(raw, &values); err == nil {
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value != "" {
				out[value] = struct{}{}
			}
		}
		return out
	}
	var generic []any
	if err := json.Unmarshal(raw, &generic); err != nil {
		return out
	}
	for _, value := range generic {
		switch typed := value.(type) {
		case string:
			typed = strings.TrimSpace(typed)
			if typed != "" {
				out[typed] = struct{}{}
			}
		case map[string]any:
			pubkey, _ := typed["pubkey"].(string)
			pubkey = strings.TrimSpace(pubkey)
			if pubkey != "" {
				out[pubkey] = struct{}{}
			}
		}
	}
	return out
}

func (g WSGateway) buildDirectMessagesPayload(ctx context.Context, receiver string, sender string, values []json.RawMessage) []any {
	out := make([]any, 0, len(values)+3)
	for _, value := range values {
		out = append(out, value)
	}
	out = append(out, g.buildMetadataEvents(ctx, []string{receiver, sender})...)
	since, until, hasRange := rangeFromEvents(values)
	out = append(out, buildRangeEvent("created_at", since, until, hasRange))
	return out
}
