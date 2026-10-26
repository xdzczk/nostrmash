package api_primal

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (g WSGateway) enrichFeedEvents(ctx context.Context, viewer string, posts []json.RawMessage, orderBy string, since any, until any) []any {
	elements := make([]string, 0, len(posts))
	authors := make([]string, 0, len(posts))
	seenAuthors := map[string]struct{}{}
	refs := make([]string, 0)
	seenRefs := map[string]struct{}{}
	for _, raw := range posts {
		if id := eventIDFromRaw(raw); id != "" {
			elements = append(elements, id)
		}
		g.rememberPubkey(&authors, seenAuthors, pubkeyFromRaw(raw), 50)
		if len(seenAuthors) < 50 {
			for _, pubkey := range tagValuesFromRawEvent(raw, "p") {
				g.rememberPubkey(&authors, seenAuthors, pubkey, 50)
			}
		}
		for _, ref := range tagValuesFromRawEvent(raw, "e") {
			ref = strings.TrimSpace(ref)
			if ref == "" {
				continue
			}
			if _, ok := seenRefs[ref]; ok {
				continue
			}
			if len(refs) >= 20 {
				break
			}
			seenRefs[ref] = struct{}{}
			refs = append(refs, ref)
		}
	}
	if since == nil && until == nil {
		since, until = createdRange(posts)
	}

	out := make([]any, 0, len(posts)+len(authors)+8)
	for _, raw := range posts {
		out = append(out, raw)
	}
	out = append(out, g.buildMetadataEvents(ctx, authors)...)
	out = append(out, g.referencedEvents(ctx, refs)...)
	out = append(out, g.zapEnrichment(ctx, elements)...)
	for _, id := range elements {
		out = append(out, noteStatsEvent(id, query.EventCounts{}))
	}
	if counts, err := g.query.GetEventCountsBatch(ctx, elements); err == nil {
		for i := range out {
			event, ok := out[i].(map[string]any)
			if !ok {
				continue
			}
			kind, ok := kindOf(event["kind"])
			if !ok || kind != primalKindNoteStats {
				continue
			}
			var body struct {
				EventID string `json:"event_id"`
			}
			_ = json.Unmarshal([]byte(stringValue(event["content"])), &body)
			if row, ok := counts[body.EventID]; ok {
				out[i] = noteStatsEvent(body.EventID, row)
			}
		}
	}
	if strings.TrimSpace(viewer) != "" {
		if actions, err := g.query.GetViewerActions(ctx, viewer, elements); err == nil {
			for _, action := range actions {
				if !action.Liked && !action.Replied && !action.Reposted && !action.Zapped {
					continue
				}
				out = append(out, kindObject(primalKindNoteActions, map[string]any{
					"event_id":         action.EventID,
					"replied":          action.Replied,
					"liked":            action.Liked,
					"reposted":         action.Reposted,
					"zapped":           action.Zapped,
					"voted_for_option": nil,
				}))
			}
		}
	}
	out = append(out, g.mediaEnrichment(ctx, posts)...)
	if len(authors) > 0 {
		counts := map[string]int64{}
		if loaded, err := g.query.GetFollowerCounts(ctx, authors); err == nil {
			counts = loaded
		}
		out = append(out, followerMapEvent(primalKindUserScore, counts, authors))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, authors))
	}
	if hints, err := g.query.GetRelayHints(ctx, elements); err == nil && len(hints) > 0 {
		out = append(out, kindObject(primalKindRelayHints, hints))
	}
	out = append(out, buildPrimalRange(orderBy, since, until, elements))
	return out
}

func (g WSGateway) rememberPubkey(out *[]string, seen map[string]struct{}, pubkey string, limit int) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" || len(seen) >= limit {
		return
	}
	if _, ok := seen[pubkey]; ok {
		return
	}
	seen[pubkey] = struct{}{}
	*out = append(*out, pubkey)
}

func (g WSGateway) referencedEvents(ctx context.Context, ids []string) []any {
	if len(ids) == 0 {
		return nil
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil
	}
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		raw, ok := found[id]
		if !ok {
			continue
		}
		out = append(out, map[string]any{
			"kind":    primalKindMentions,
			"pubkey":  pubkeyFromRaw(raw),
			"content": string(raw),
		})
	}
	return out
}

func (g WSGateway) zapEnrichment(ctx context.Context, ids []string) []any {
	if len(ids) == 0 {
		return nil
	}
	zaps, err := g.query.GetTopEventZaps(ctx, ids, 5)
	if err != nil || len(zaps) == 0 {
		return nil
	}
	receiptIDs := make([]string, 0, len(zaps))
	for _, zap := range zaps {
		if zap.ZapReceiptID != "" {
			receiptIDs = append(receiptIDs, zap.ZapReceiptID)
		}
	}
	receipts := map[string]json.RawMessage{}
	if len(receiptIDs) > 0 {
		if found, err := g.query.GetEventBatch(ctx, receiptIDs); err == nil {
			receipts = found
		}
	}
	out := make([]any, 0, len(zaps)*2)
	for _, zap := range zaps {
		out = append(out, kindObject(primalKindPostZaps, map[string]any{
			"event_id":       zap.EventID,
			"created_at":     zap.CreatedAt,
			"sender":         zap.Sender,
			"receiver":       zap.Receiver,
			"amount_sats":    zap.AmountSats,
			"zap_receipt_id": zap.ZapReceiptID,
		}))
		if raw, ok := receipts[zap.ZapReceiptID]; ok {
			out = append(out, raw)
		}
	}
	return out
}

func noteStatsEvent(id string, counts query.EventCounts) map[string]any {
	return kindObject(primalKindNoteStats, map[string]any{
		"event_id":   id,
		"likes":      counts.ReactionCount,
		"replies":    counts.ReplyCount,
		"mentions":   0,
		"reposts":    counts.RepostCount,
		"zaps":       counts.ZapCount,
		"satszapped": counts.ZapMSats / 1000,
		"score":      0,
		"score24h":   0,
		"bookmarks":  0,
	})
}

func followerMapEvent(kind int, counts map[string]int64, pubkeys []string) map[string]any {
	body := make(map[string]int64, len(pubkeys))
	for _, pubkey := range pubkeys {
		body[pubkey] = counts[pubkey]
	}
	return kindObject(kind, body)
}

func createdRange(posts []json.RawMessage) (any, any) {
	var since int64
	var until int64
	found := false
	for _, raw := range posts {
		created, ok := createdAtFromRaw(raw)
		if !ok {
			continue
		}
		if !found {
			since, until, found = created, created, true
			continue
		}
		if created < since {
			since = created
		}
		if created > until {
			until = created
		}
	}
	if !found {
		return nil, nil
	}
	return since, until
}

func (g WSGateway) mediaEnrichment(ctx context.Context, posts []json.RawMessage) []any {
	out := make([]any, 0)
	for _, raw := range posts {
		resources := mediaResourcesFromEvent(raw)
		if len(resources) == 0 {
			continue
		}
		urls := make([]string, 0, len(resources))
		for _, resource := range resources {
			urls = append(urls, resource.URL)
		}
		stored := map[string]readmodel.MediaMetadata{}
		if rows, err := g.query.MediaMetadataByURLs(ctx, urls); err == nil {
			for _, row := range rows {
				stored[row.URL] = row
			}
		}
		links := map[string]readmodel.LinkMetadata{}
		if rows, err := g.query.LinkMetadataByURLs(ctx, urls); err == nil {
			for _, row := range rows {
				links[row.URL] = row
			}
		}
		merged := make([]map[string]any, 0, len(resources))
		for _, resource := range resources {
			item := map[string]any{"url": resource.URL}
			if row, ok := stored[resource.URL]; ok {
				item["mimetype"] = row.Mimetype
				item["width"] = row.Width
				item["height"] = row.Height
				item["duration"] = row.Duration
				item["blurhash"] = row.Blurhash
			} else {
				if resource.Mimetype != "" {
					item["mimetype"] = resource.Mimetype
				}
				if resource.Width > 0 {
					item["width"] = resource.Width
				}
				if resource.Height > 0 {
					item["height"] = resource.Height
				}
			}
			merged = append(merged, item)
			if link, ok := links[resource.URL]; ok && (link.Title != "" || link.Description != "" || link.Image != "") {
				out = append(out, kindObject(primalKindWebPreview, map[string]any{
					"url":         link.URL,
					"title":       link.Title,
					"description": link.Description,
					"image":       link.Image,
					"icon_url":    link.IconURL,
					"event_id":    eventIDFromRaw(raw),
				}))
			}
		}
		out = append(out, kindObject(primalKindMediaMetadata, map[string]any{
			"resources":  merged,
			"thumbnails": map[string]any{},
			"event_id":   eventIDFromRaw(raw),
		}))
	}
	return out
}
