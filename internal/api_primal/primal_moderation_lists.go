package api_primal

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) getModerationListEvents(ctx context.Context, pubkey string, listKind moderationListKind) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return []json.RawMessage{}, nil
	}
	specs := moderationListSpecs(listKind)
	out := make([]json.RawMessage, 0, len(specs))
	for _, spec := range specs {
		event, ok, err := g.getModerationReplaceableEvent(ctx, pubkey, spec.kind, spec.dTag)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, event)
		}
	}
	return out, nil
}

func moderationListSpecs(listKind moderationListKind) []moderationListSpec {
	switch listKind {
	case moderationListMute:
		return []moderationListSpec{
			{kind: 10000, dTag: ""},
			{kind: 30000, dTag: "mute"},
		}
	case moderationListMutelists:
		return []moderationListSpec{
			{kind: 30000, dTag: "mutelists"},
		}
	case moderationListAllowlist:
		return []moderationListSpec{
			{kind: 30000, dTag: "allowlist"},
			{kind: 10001, dTag: ""},
		}
	default:
		return []moderationListSpec{}
	}
}

func (g WSGateway) getModerationReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, bool, error) {
	event, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, dTag)
	if err == nil {
		return event, true, nil
	}
	if query.IsNotFound(err) {
		return nil, false, nil
	}
	if query.IsUnsupportedCapability(err) {
		// Compatibility: keep historical empty-list behavior for moderation list calls.
		return nil, false, nil
	}
	return nil, false, err
}
