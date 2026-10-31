package api

import (
	"context"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// attachTopZaps adds the largest zap onto note maps. Missing support is ignored
// so a list still renders when the reader cannot serve zap ads.
func (h Handlers) attachTopZaps(ctx context.Context, items []map[string]any) {
	if len(items) == 0 {
		return
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if id := noteItemEventID(item); id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return
	}
	ads, err := h.service.TopZapAdsByEventIDs(ctx, ids)
	if err != nil || len(ads) == 0 {
		return
	}
	for _, item := range items {
		ad, ok := readmodel.LookupTopZap(ads, noteItemEventID(item))
		if !ok || ad.Sats <= 0 || strings.TrimSpace(ad.SenderPubkey) == "" {
			continue
		}
		item["top_zap"] = readmodel.TopZapPayload(ad)
	}
}

func noteItemEventID(item map[string]any) string {
	for _, key := range []string{"id", "event_id", "root_event_id"} {
		value, ok := item[key].(string)
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
