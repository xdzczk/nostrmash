package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) primalV1SetProtocol(context.Context, map[string]any) ([]any, error) {
	return []any{}, nil
}

func (g WSGateway) primalV1ClientConfig(context.Context, map[string]any) ([]any, error) {
	content := strings.TrimSpace(string(g.opts.ClientConfig))
	if content == "" {
		content = "null"
	}
	return []any{kindContent(primalKindClientConfig, content)}, nil
}

func (g WSGateway) primalV1DefaultAppSettings(_ context.Context, kwargs map[string]any) ([]any, error) {
	client := strings.TrimSpace(stringValue(kwargs["client"]))
	if client == "" {
		client = "primal-ios"
	}
	return []any{settingsEvent(client, g.opts.DefaultSettings)}, nil
}

func (g WSGateway) primalV1AppSettings(ctx context.Context, kwargs map[string]any) ([]any, error) {
	event, err := signedSettingsEvent(kwargs)
	if err != nil {
		return nil, err
	}
	stored, client, err := g.query.GetAppSettings(ctx, event.Pubkey)
	if err != nil && !query.IsNotFound(err) && !query.IsUnsupportedCapability(err) {
		return nil, errors.New("error")
	}
	if len(stored) == 0 {
		stored = g.opts.DefaultSettings
	}
	if strings.TrimSpace(client) == "" {
		client = settingsClient(event)
	}
	return []any{settingsEvent(client, stored)}, nil
}

func (g WSGateway) primalV1SetAppSettings(ctx context.Context, kwargs map[string]any) ([]any, error) {
	event, err := signedSettingsEvent(kwargs)
	if err != nil {
		return nil, err
	}
	next := json.RawMessage(event.Content)
	if !json.Valid(next) {
		return nil, errors.New("error")
	}
	current, _, err := g.query.GetAppSettings(ctx, event.Pubkey)
	if err != nil && !query.IsNotFound(err) && !query.IsUnsupportedCapability(err) {
		return nil, errors.New("error")
	}
	if len(current) == 0 {
		current = g.opts.DefaultSettings
	}
	merged := mergeJSONObjects(current, next)
	client := settingsClient(event)
	if err := g.query.PutAppSettings(ctx, event.Pubkey, client, merged); err != nil {
		return nil, errors.New("error")
	}
	return []any{settingsEvent(client, merged)}, nil
}

func (g WSGateway) primalV1AppSubsettings(ctx context.Context, kwargs map[string]any) ([]any, error) {
	return g.readSubsettings(ctx, kwargs, false)
}

func (g WSGateway) primalV1DefaultAppSubsettings(ctx context.Context, kwargs map[string]any) ([]any, error) {
	return g.readSubsettings(ctx, kwargs, true)
}

func (g WSGateway) readSubsettings(ctx context.Context, kwargs map[string]any, defaults bool) ([]any, error) {
	event, err := signedSettingsEvent(kwargs)
	if err != nil {
		return nil, err
	}
	subkey := strings.TrimSpace(stringValue(kwargs["subkey"]))
	if subkey == "" {
		subkey = strings.TrimSpace(stringValue(kwargs["settings_subkey"]))
	}
	if !knownSubkey(subkey) {
		return nil, errors.New("error")
	}
	if defaults {
		return []any{subsettingsEvent(subkey, json.RawMessage("[]"))}, nil
	}
	stored, err := g.query.GetAppSubsettings(ctx, event.Pubkey, subkey)
	if query.IsNotFound(err) || query.IsUnsupportedCapability(err) || len(stored) == 0 {
		stored = json.RawMessage("[]")
	} else if err != nil {
		return nil, errors.New("error")
	}
	return []any{subsettingsEvent(subkey, stored)}, nil
}

func (g WSGateway) primalV1SetAppSubsettings(ctx context.Context, kwargs map[string]any) ([]any, error) {
	event, err := signedSettingsEvent(kwargs)
	if err != nil {
		return nil, err
	}
	subkey := strings.TrimSpace(stringValue(kwargs["subkey"]))
	if !knownSubkey(subkey) {
		return nil, errors.New("error")
	}
	next := json.RawMessage(event.Content)
	if !json.Valid(next) {
		return nil, errors.New("error")
	}
	if err := g.query.PutAppSubsettings(ctx, event.Pubkey, subkey, next); err != nil {
		return nil, errors.New("error")
	}
	return []any{subsettingsEvent(subkey, next)}, nil
}

func (g WSGateway) primalV1DefaultRelays(context.Context, map[string]any) ([]any, error) {
	raw, err := json.Marshal(g.opts.DefaultRelays)
	if err != nil || g.opts.DefaultRelays == nil {
		raw = []byte("[]")
	}
	return []any{kindContent(primalKindDefaultRelays, string(raw))}, nil
}

func (g WSGateway) primalV1NetStats(ctx context.Context, _ map[string]any) ([]any, error) {
	stats, err := g.query.GetNetworkStats(ctx)
	if err != nil {
		return nil, errors.New("error")
	}
	return []any{kindObject(primalKindNetStats, stats)}, nil
}

func settingsEvent(client string, settings json.RawMessage) map[string]any {
	if len(settings) == 0 {
		settings = json.RawMessage("null")
	}
	return map[string]any{
		"kind":    primalKindDefaultSettings,
		"content": string(settings),
		"tags":    []any{[]any{"d", client}},
	}
}

func subsettingsEvent(subkey string, settings json.RawMessage) map[string]any {
	if len(settings) == 0 {
		settings = json.RawMessage("[]")
	}
	return map[string]any{
		"kind":    primalKindSubsettings,
		"content": string(settings),
		"tags":    []any{[]any{"d", subkey}},
	}
}

func knownSubkey(subkey string) bool {
	switch subkey {
	case "user-home-feeds", "user-reads-feeds":
		return true
	default:
		return false
	}
}

func settingsClient(event *nostr.Event) string {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "d" && strings.TrimSpace(tag[1]) != "" {
			return tag[1]
		}
	}
	return "primal-ios"
}

func signedSettingsEvent(kwargs map[string]any) (*nostr.Event, error) {
	raw, ok := kwargs["settings_event"]
	if !ok {
		raw, ok = kwargs["event_from_user"]
	}
	if !ok {
		return nil, errors.New("error")
	}
	return parseSignedUserEvent(raw, appSettingsKind)
}

func parseSignedUserEvent(raw any, requiredKind int) (*nostr.Event, error) {
	payload, err := json.Marshal(raw)
	if err != nil {
		return nil, errors.New("error")
	}
	result := nostr.ParseAndValidate(payload, nostr.Options{})
	if !result.Valid() || result.Event == nil {
		return nil, errors.New("error")
	}
	if requiredKind != 0 && result.Event.Kind != requiredKind {
		return nil, errors.New("error")
	}
	if err := validatePubkeyHex(result.Event.Pubkey); err != nil {
		return nil, errors.New("error")
	}
	return result.Event, nil
}
