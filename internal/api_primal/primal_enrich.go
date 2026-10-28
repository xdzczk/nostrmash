package api_primal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

const (
	// A feed page can quote once per note, and each quoted note can quote again.
	maxReferencedEvents = 64
	// Quote authors are required to render an embed, so they must not lose to p-tags.
	maxFeedAuthors = 100
)

// nostr:note1 and nostr:nevent1 are how Primal marks an embedded note in content.
var nostrEventRefPattern = regexp.MustCompile(`(?i)(?:nostr:)?((?:note|nevent)1[02-9ac-hj-np-z]{6,})`)

func (g WSGateway) enrichFeedEvents(ctx context.Context, viewer string, posts []json.RawMessage, orderBy string, since any, until any) []any {
	elements := make([]string, 0, len(posts))
	authors := make([]string, 0, len(posts))
	seenAuthors := map[string]struct{}{}
	pageIDs := map[string]struct{}{}
	for _, raw := range posts {
		if id := strings.TrimSpace(eventIDFromRaw(raw)); id != "" {
			elements = append(elements, id)
			pageIDs[id] = struct{}{}
			if canonical := canonicalEventID(id); canonical != "" {
				pageIDs[canonical] = struct{}{}
			}
		}
		g.rememberPubkey(&authors, seenAuthors, pubkeyFromRaw(raw), maxFeedAuthors)
	}
	// Primal renders "Mentioned note not found" unless the quoted event is
	// returned as kind 10000107 AND the quoted author's kind 0 is in the same
	// page. Quote targets (q tags, e-mention/quote, nostr:note1/nevent1) are
	// collected before reply parents so a page of replies cannot crowd them out.
	refIDs, refAuthors := collectReferencedEventIDs(posts, pageIDs, maxReferencedEvents)
	referenced := g.loadReferencedEvents(ctx, refIDs)
	if len(referenced) > 0 && len(refIDs) < maxReferencedEvents {
		skip := make(map[string]struct{}, len(pageIDs)+len(refIDs))
		for id := range pageIDs {
			skip[id] = struct{}{}
		}
		for _, id := range refIDs {
			skip[id] = struct{}{}
		}
		nestedIDs, nestedAuthors := quoteReferences(referenced, skip, maxReferencedEvents-len(refIDs))
		refAuthors = append(refAuthors, nestedAuthors...)
		referenced = append(referenced, g.loadReferencedEvents(ctx, nestedIDs)...)
	}
	for _, pubkey := range refAuthors {
		g.rememberPubkey(&authors, seenAuthors, pubkey, maxFeedAuthors)
	}
	for _, raw := range referenced {
		g.rememberPubkey(&authors, seenAuthors, pubkeyFromRaw(raw), maxFeedAuthors)
	}
	for _, raw := range posts {
		for _, pubkey := range tagValuesFromRawEvent(raw, "p") {
			g.rememberPubkey(&authors, seenAuthors, pubkey, maxFeedAuthors)
		}
	}
	if since == nil && until == nil {
		since, until = createdRange(posts)
	}

	out := make([]any, 0, len(posts)+len(authors)+len(referenced)+8)
	for _, raw := range posts {
		out = append(out, raw)
	}
	out = append(out, g.buildMetadataEvents(ctx, authors)...)
	out = append(out, mentionEvents(referenced)...)
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

func (g WSGateway) loadReferencedEvents(ctx context.Context, ids []string) []json.RawMessage {
	if len(ids) == 0 {
		return nil
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil
	}
	out := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		raw, ok := found[id]
		if !ok || len(raw) == 0 {
			continue
		}
		out = append(out, raw)
	}
	return out
}

func mentionEvents(raws []json.RawMessage) []any {
	if len(raws) == 0 {
		return nil
	}
	out := make([]any, 0, len(raws))
	for _, raw := range raws {
		out = append(out, map[string]any{
			"kind":    primalKindMentions,
			"pubkey":  pubkeyFromRaw(raw),
			"content": string(raw),
		})
	}
	return out
}

func collectReferencedEventIDs(posts []json.RawMessage, skip map[string]struct{}, limit int) ([]string, []string) {
	ids, authors := quoteReferences(posts, skip, limit)
	if len(ids) >= limit {
		return ids, authors
	}
	seen := make(map[string]struct{}, len(skip)+len(ids))
	for id := range skip {
		seen[id] = struct{}{}
	}
	for _, id := range ids {
		seen[id] = struct{}{}
	}
	for _, raw := range posts {
		for _, ref := range tagValuesFromRawEvent(raw, "e") {
			if len(ids) >= limit {
				return ids, authors
			}
			id := canonicalEventID(ref)
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids, authors
}

func quoteReferences(events []json.RawMessage, skip map[string]struct{}, limit int) ([]string, []string) {
	ids := make([]string, 0)
	authors := make([]string, 0)
	seenIDs := make(map[string]struct{}, len(skip))
	for id := range skip {
		seenIDs[id] = struct{}{}
	}
	seenAuthors := map[string]struct{}{}
	addID := func(raw string) {
		if len(ids) >= limit {
			return
		}
		id := canonicalEventID(raw)
		if id == "" {
			return
		}
		if _, ok := seenIDs[id]; ok {
			return
		}
		seenIDs[id] = struct{}{}
		ids = append(ids, id)
	}
	addAuthor := func(raw string) {
		pubkey := canonicalPubkey(raw)
		if pubkey == "" {
			return
		}
		if _, ok := seenAuthors[pubkey]; ok {
			return
		}
		seenAuthors[pubkey] = struct{}{}
		authors = append(authors, pubkey)
	}
	for _, raw := range events {
		forEachTag(raw, func(fields []string) {
			if len(fields) < 2 {
				return
			}
			switch strings.ToLower(fields[0]) {
			case "q":
				addID(fields[1])
				if len(fields) >= 4 {
					addAuthor(fields[3])
				}
			case "e":
				if len(fields) >= 4 && isQuoteMarker(fields[3]) {
					addID(fields[1])
				}
			}
		})
		for _, match := range nostrEventRefPattern.FindAllStringSubmatch(contentFromRaw(raw), -1) {
			if len(match) > 1 {
				addID(match[1])
			}
		}
	}
	return ids, authors
}

func forEachTag(raw json.RawMessage, fn func(fields []string)) {
	var payload struct {
		Tags []any `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return
	}
	for _, rawTag := range payload.Tags {
		items, ok := rawTag.([]any)
		if !ok || len(items) < 2 {
			continue
		}
		fields := make([]string, len(items))
		for i, item := range items {
			text, _ := item.(string)
			fields[i] = strings.TrimSpace(text)
		}
		fn(fields)
	}
}

func contentFromRaw(raw json.RawMessage) string {
	var payload struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return ""
	}
	return payload.Content
}

func isQuoteMarker(marker string) bool {
	switch strings.ToLower(strings.TrimSpace(marker)) {
	case "mention", "quote":
		return true
	default:
		return false
	}
}

func canonicalEventID(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	lower = strings.TrimPrefix(lower, "nostr:")
	if strings.HasPrefix(lower, "note1") || strings.HasPrefix(lower, "nevent1") {
		decoded, err := decodeNIP19(lower)
		if err != nil {
			return ""
		}
		data, _ := decoded["data"].(string)
		return strings.ToLower(strings.TrimSpace(data))
	}
	if len(lower) == 64 {
		if _, err := hex.DecodeString(lower); err == nil {
			return lower
		}
	}
	return value
}

func canonicalPubkey(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	lower := strings.ToLower(value)
	lower = strings.TrimPrefix(lower, "nostr:")
	if strings.HasPrefix(lower, "npub1") || strings.HasPrefix(lower, "nprofile1") {
		decoded, err := decodeNIP19(lower)
		if err != nil {
			return ""
		}
		// npub and nprofile both carry the pubkey in TLV type 0, which
		// decodeNIP19 reports as "data". nprofile may also set "pubkey".
		if data, _ := decoded["data"].(string); strings.TrimSpace(data) != "" {
			return strings.ToLower(strings.TrimSpace(data))
		}
		pubkey, _ := decoded["pubkey"].(string)
		return strings.ToLower(strings.TrimSpace(pubkey))
	}
	if len(lower) == 64 {
		if _, err := hex.DecodeString(lower); err == nil {
			return lower
		}
	}
	return ""
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
