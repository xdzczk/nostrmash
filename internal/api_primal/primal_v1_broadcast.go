package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/nostr"
)

const localBroadcastRelay = "wss://local.nostrmash"

func (g WSGateway) primalV1Broadcast(ctx context.Context, kwargs map[string]any) ([]any, error) {
	events := broadcastEvents(kwargs["events"])
	if len(events) == 0 {
		if one := kwargs["event"]; one != nil {
			events = []any{one}
		}
	}
	if len(events) == 0 {
		return nil, errors.New("error")
	}
	relays := toStringSlice(kwargs["relays"])
	results := make([]any, 0, len(events))
	for _, rawEvent := range events {
		payload, err := json.Marshal(rawEvent)
		if err != nil {
			return nil, errors.New("error")
		}
		validated := nostr.ParseAndValidate(payload, nostr.Options{})
		if !validated.Valid() || validated.Event == nil {
			return nil, errors.New("error")
		}
		event := validated.Event
		if g.opts.Persister != nil {
			_ = g.opts.Persister.PersistCanonical(ctx, model.Event{
				ID:        event.ID,
				Pubkey:    event.Pubkey,
				CreatedAt: event.CreatedAt,
				Kind:      event.Kind,
				Sig:       event.Sig,
				Content:   event.Content,
				RawJSON:   validated.RawJSON,
			}, event.Tags, localBroadcastRelay)
		}
		responses := make([]any, 0, len(relays)+1)
		responses = append(responses, []any{localBroadcastRelay, []any{"OK", event.ID, true, ""}})
		if g.opts.Publisher != nil && len(relays) > 0 {
			for _, result := range g.opts.Publisher.Publish(ctx, relays, validated.RawJSON) {
				responses = append(responses, []any{result.Relay, result.Reply})
			}
		}
		results = append(results, map[string]any{
			"event_id":  event.ID,
			"responses": responses,
		})
	}
	return []any{kindObject(primalKindBroadcastResult, results)}, nil
}

func (g WSGateway) primalV1BroadcastReply(ctx context.Context, kwargs map[string]any) ([]any, error) {
	rawEvent := kwargs["event"]
	if rawEvent == nil {
		return nil, errors.New("error")
	}
	payload, err := json.Marshal(rawEvent)
	if err != nil {
		return nil, errors.New("error")
	}
	validated := nostr.ParseAndValidate(payload, nostr.Options{})
	if !validated.Valid() || validated.Event == nil {
		return nil, errors.New("error")
	}
	targets := make([]json.RawMessage, 0)
	for _, tag := range validated.Event.Tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		if raw, err := g.query.GetEventByID(ctx, tag[1]); err == nil && len(raw) > 0 {
			targets = append(targets, raw)
		}
	}
	if len(targets) == 0 {
		targets = []json.RawMessage{validated.RawJSON}
	}
	relays := g.writeRelays(ctx, validated.Event.Pubkey)
	results := make([]any, 0, len(targets))
	for _, raw := range targets {
		responses := []any{[]any{localBroadcastRelay, []any{"OK", eventIDFromRaw(raw), true, ""}}}
		if g.opts.Publisher != nil && len(relays) > 0 {
			for _, result := range g.opts.Publisher.Publish(ctx, relays, raw) {
				responses = append(responses, []any{result.Relay, result.Reply})
			}
		}
		results = append(results, map[string]any{
			"event_id":  eventIDFromRaw(raw),
			"responses": responses,
		})
	}
	return []any{kindObject(primalKindBroadcastResult, results)}, nil
}

func (g WSGateway) writeRelays(ctx context.Context, pubkey string) []string {
	tags, err := g.relayTagsForPubkey(ctx, pubkey)
	if err != nil {
		return nil
	}
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		fields, ok := tag.([]any)
		if !ok || len(fields) < 2 {
			continue
		}
		url, _ := fields[1].(string)
		marker := ""
		if len(fields) >= 3 {
			marker, _ = fields[2].(string)
		}
		if marker == "read" || strings.TrimSpace(url) == "" {
			continue
		}
		out = append(out, url)
	}
	return out
}

func broadcastEvents(raw any) []any {
	switch typed := raw.(type) {
	case []any:
		return typed
	case nil:
		return nil
	default:
		return []any{typed}
	}
}
