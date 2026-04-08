package api_primal

import (
	"context"
	"encoding/json"
	"strings"
)

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
