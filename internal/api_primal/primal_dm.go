package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/nostr"
)

type dmContactDetails struct {
	PeerPubkey    string `json:"peer_pubkey"`
	Cnt           int64  `json:"cnt"`
	LatestAt      int64  `json:"latest_at"`
	LatestEventID string `json:"latest_event_id"`
}

func isDirectMessagesRequest(filters []any) bool {
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			continue
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			continue
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			continue
		}
		name, _ := cacheArgs[0].(string)
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "get_directmsgs", "directmsg_count", "directmsg_count_2", "reset_directmsg_count", "reset_directmsg_counts", "get_directmsg_contacts":
			return true
		}
	}
	return false
}

func parseDMLiveSubscription(subID string, filters []any) (dmLiveSubscription, bool) {
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			continue
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			continue
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			continue
		}
		name, _ := cacheArgs[0].(string)
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "directmsg_count" && name != "directmsg_count_2" {
			continue
		}
		kwargs := map[string]any{}
		if len(cacheArgs) > 1 {
			if m, ok := cacheArgs[1].(map[string]any); ok {
				kwargs = m
			}
		}
		receiver, _ := kwargs["pubkey"].(string)
		receiver = strings.TrimSpace(receiver)
		if receiver == "" {
			return dmLiveSubscription{}, false
		}
		if err := validatePubkeyHex(receiver); err != nil {
			return dmLiveSubscription{}, false
		}
		sender, _ := kwargs["sender"].(string)
		sender = strings.TrimSpace(sender)
		if sender != "" {
			if err := validatePubkeyHex(sender); err != nil {
				return dmLiveSubscription{}, false
			}
		}
		return dmLiveSubscription{
			SubID:    subID,
			Kind:     name,
			Receiver: receiver,
			Sender:   sender,
		}, true
	}
	return dmLiveSubscription{}, false
}

func hasOnlyDMLiveFilters(filters []any) bool {
	if len(filters) == 0 {
		return false
	}
	for _, rawFilter := range filters {
		filter, ok := rawFilter.(map[string]any)
		if !ok {
			return false
		}
		cacheRaw, ok := filter["cache"]
		if !ok {
			return false
		}
		cacheArgs, ok := cacheRaw.([]any)
		if !ok || len(cacheArgs) == 0 {
			return false
		}
		name, _ := cacheArgs[0].(string)
		name = strings.ToLower(strings.TrimSpace(name))
		if name != "directmsg_count" && name != "directmsg_count_2" {
			return false
		}
	}
	return true
}

func validatePubkeyHex(pubkey string) error {
	pubkey = strings.TrimSpace(pubkey)
	if len(pubkey) != 64 {
		return errors.New("invalid pubkey")
	}
	for _, r := range pubkey {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			continue
		}
		if r >= 'A' && r <= 'F' {
			continue
		}
		return fmt.Errorf("invalid pubkey")
	}
	return nil
}

func parseAndValidateDMResetAuth(kwargs map[string]any) (receiver string, sender string, err error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", "", errors.New("event from the future")
	}
	receiver = strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", "", err
	}
	sender, _ = kwargs["peer_pubkey"].(string)
	if strings.TrimSpace(sender) == "" {
		sender, _ = kwargs["sender"].(string)
	}
	if err := validatePubkeyHex(sender); err != nil {
		return "", "", err
	}
	return receiver, sender, nil
}

func parseAndValidateDMResetAllAuth(kwargs map[string]any) (string, error) {
	eventFromUser, ok := kwargs["event_from_user"]
	if !ok {
		return "", errors.New("event_from_user is required")
	}
	payload, err := json.Marshal(eventFromUser)
	if err != nil {
		return "", errors.New("event_from_user is malformed")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() {
		return "", errors.New("verification failed")
	}
	now := time.Now().Unix()
	if result.Event.CreatedAt <= now-300 {
		return "", errors.New("event is too old")
	}
	if result.Event.CreatedAt >= now+300 {
		return "", errors.New("event from the future")
	}
	receiver := strings.TrimSpace(result.Event.Pubkey)
	if err := validatePubkeyHex(receiver); err != nil {
		return "", err
	}
	return receiver, nil
}

func (g WSGateway) resolveDMCountFrame(ctx context.Context, sub dmLiveSubscription) ([]any, error) {
	count, err := g.query.GetDirectMessageCount(ctx, sub.Receiver, sub.Sender)
	if err != nil {
		return nil, err
	}
	if sub.Kind == "directmsg_count_2" {
		return []any{"EVENT", sub.SubID, buildDirectMessageCount2Event(count)}, nil
	}
	return []any{"EVENT", sub.SubID, buildDirectMessageCountEvent(count)}, nil
}

func buildDirectMessageCountEvent(count int64) map[string]any {
	return map[string]any{
		"kind": primalKindDirectMsgCount,
		"cnt":  count,
	}
}

func buildDirectMessageCount2Event(count int64) map[string]any {
	contentRaw, _ := json.Marshal(count)
	return map[string]any{
		"kind":    primalKindDirectMsgCount2,
		"content": string(contentRaw),
	}
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
