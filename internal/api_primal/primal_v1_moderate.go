package api_primal

import (
	"context"
	"encoding/json"
	"strings"
)

func (g WSGateway) moderatePrimalEvents(ctx context.Context, kwargs map[string]any, events []any) []any {
	muted := map[string]struct{}{}
	viewer := g.feedViewer(kwargs, kwargs)
	if strings.TrimSpace(viewer) != "" {
		if list, err := g.query.GetMuteList(ctx, viewer); err == nil {
			for _, pubkey := range list {
				pubkey = strings.ToLower(strings.TrimSpace(pubkey))
				if pubkey != "" {
					muted[pubkey] = struct{}{}
				}
			}
		}
	}
	exempt := ""
	notes, author := notesMode(kwargs)
	if notes == "authored" {
		exempt = strings.ToLower(strings.TrimSpace(author))
	}
	originalIDs := map[string]struct{}{}
	ids := make([]string, 0, len(events))
	for _, event := range events {
		if id := identifyEvent(event).id; id != "" {
			originalIDs[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	deleted := map[string]struct{}{}
	if rows, err := g.query.DeletedEventIDs(ctx, ids); err == nil {
		for _, id := range rows {
			deleted[id] = struct{}{}
		}
	}
	out := make([]any, 0, len(events))
	for _, event := range events {
		identity := identifyEvent(event)
		if identity.id != "" {
			if _, ok := deleted[identity.id]; ok {
				continue
			}
		}
		if identity.pubkey != "" && identity.pubkey != exempt {
			if _, ok := muted[identity.pubkey]; ok {
				continue
			}
		}
		if pruned, ok := prunePubkeyMap(event, muted); ok {
			event = pruned
		}
		out = append(out, event)
	}
	return pruneRangeElements(out, originalIDs, deleted)
}

func notesMode(kwargs map[string]any) (string, string) {
	notes := strings.ToLower(strings.TrimSpace(stringValue(kwargs["notes"])))
	author := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	raw := strings.TrimSpace(stringValue(kwargs["spec"]))
	if raw == "" {
		return notes, author
	}
	var spec map[string]any
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return notes, author
	}
	if notes == "" {
		notes = strings.ToLower(strings.TrimSpace(stringValue(spec["notes"])))
	}
	if specAuthor := strings.TrimSpace(stringValue(spec["pubkey"])); specAuthor != "" {
		author = specAuthor
	}
	return notes, author
}

type eventIdentity struct {
	id     string
	pubkey string
	kind   int
}

func identifyEvent(event any) eventIdentity {
	switch typed := event.(type) {
	case json.RawMessage:
		return identityFromRaw(typed)
	case []byte:
		return identityFromRaw(typed)
	case map[string]any:
		kind, _ := kindOf(typed["kind"])
		pubkey := strings.ToLower(strings.TrimSpace(stringValue(typed["pubkey"])))
		id := strings.TrimSpace(stringValue(typed["id"]))
		return eventIdentity{id: id, pubkey: pubkey, kind: kind}
	default:
		return eventIdentity{}
	}
}

func identityFromRaw(raw json.RawMessage) eventIdentity {
	var payload struct {
		ID     string `json:"id"`
		Pubkey string `json:"pubkey"`
		Kind   int    `json:"kind"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return eventIdentity{}
	}
	return eventIdentity{
		id:     strings.TrimSpace(payload.ID),
		pubkey: strings.ToLower(strings.TrimSpace(payload.Pubkey)),
		kind:   payload.Kind,
	}
}

func prunePubkeyMap(event any, muted map[string]struct{}) (any, bool) {
	item, ok := event.(map[string]any)
	if !ok || len(muted) == 0 {
		return event, false
	}
	kind, ok := kindOf(item["kind"])
	if !ok || (kind != primalKindUserScore && kind != primalKindUserFollowers && kind != primalKindDirectMsgCounts) {
		return event, false
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(stringValue(item["content"])), &body); err != nil || body == nil {
		return event, false
	}
	changed := false
	for pubkey := range body {
		if _, ok := muted[strings.ToLower(pubkey)]; ok {
			delete(body, pubkey)
			changed = true
		}
	}
	if !changed {
		return event, false
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return event, false
	}
	item["content"] = string(raw)
	return item, true
}

func pruneRangeElements(events []any, originalIDs map[string]struct{}, deleted map[string]struct{}) []any {
	present := map[string]struct{}{}
	for _, event := range events {
		if id := identifyEvent(event).id; id != "" {
			present[id] = struct{}{}
		}
	}
	if len(events) == 0 {
		return events
	}
	last, ok := events[len(events)-1].(map[string]any)
	if !ok {
		return events
	}
	kind, ok := kindOf(last["kind"])
	if !ok || kind != primalKindRange {
		return events
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(stringValue(last["content"])), &payload); err != nil || payload == nil {
		return events
	}
	rawElements, _ := payload["elements"].([]any)
	if rawElements == nil {
		return events
	}
	kept := make([]any, 0, len(rawElements))
	for _, element := range rawElements {
		id, _ := element.(string)
		if _, ok := deleted[id]; ok {
			continue
		}
		if _, wasEvent := originalIDs[id]; wasEvent {
			if _, stillPresent := present[id]; !stillPresent {
				continue
			}
		}
		kept = append(kept, id)
	}
	payload["elements"] = kept
	encoded, err := json.Marshal(payload)
	if err != nil {
		return events
	}
	last["content"] = string(encoded)
	events[len(events)-1] = last
	return events
}
