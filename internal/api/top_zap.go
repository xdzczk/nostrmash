package api

import (
	"context"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// attachTopZaps adds the largest zap onto note maps, then the kind-1 quote
// count. Missing support is ignored so a list still renders.
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
	if err == nil && len(ads) > 0 {
		for _, item := range items {
			ad, ok := readmodel.LookupTopZap(ads, noteItemEventID(item))
			if !ok || ad.Sats <= 0 || strings.TrimSpace(ad.SenderPubkey) == "" {
				continue
			}
			item["top_zap"] = readmodel.TopZapPayload(ad)
		}
	}
	h.attachQuoteCounts(ctx, items)
}

func (h Handlers) attachQuoteCounts(ctx context.Context, items []map[string]any) {
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
	counts, err := h.service.QuoteCountsByEventIDs(ctx, ids)
	if err != nil || len(counts) == 0 {
		return
	}
	for _, item := range items {
		id := noteItemEventID(item)
		count := counts[id]
		if count == 0 {
			count = counts[strings.ToLower(id)]
		}
		if count > 0 {
			item["quote_count"] = count
		}
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
