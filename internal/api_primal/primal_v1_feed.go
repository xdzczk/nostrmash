package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) primalV1MegaFeed(ctx context.Context, kwargs map[string]any) ([]any, error) {
	spec, err := mergeFeedSpec(kwargs)
	if err != nil {
		return nil, err
	}
	return g.routeMegaFeed(ctx, kwargs, spec)
}

func (g WSGateway) primalV1AuthorReplies(ctx context.Context, kwargs map[string]any) ([]any, error) {
	kwargs["id"] = "feed"
	kwargs["notes"] = "replies"
	return g.primalV1MegaFeed(ctx, kwargs)
}

func mergeFeedSpec(kwargs map[string]any) (map[string]any, error) {
	merged := make(map[string]any, len(kwargs)+4)
	for key, value := range kwargs {
		merged[key] = value
	}
	raw, _ := kwargs["spec"].(string)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return merged, nil
	}
	var spec map[string]any
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return nil, errors.New("error")
	}
	for key, value := range spec {
		merged[key] = value
	}
	return merged, nil
}

func (g WSGateway) routeMegaFeed(ctx context.Context, kwargs map[string]any, spec map[string]any) ([]any, error) {
	id := strings.ToLower(strings.TrimSpace(stringValue(spec["id"])))
	kind := strings.ToLower(strings.TrimSpace(stringValue(spec["kind"])))
	notes := strings.ToLower(strings.TrimSpace(stringValue(spec["notes"])))
	if notes == "" {
		notes = "follows"
	}
	if id == "" && strings.TrimSpace(stringValue(kwargs["pubkey"])) != "" && strings.TrimSpace(stringValue(kwargs["spec"])) == "" {
		id = "feed"
		notes = "authored"
		spec["pubkey"] = kwargs["pubkey"]
	}
	viewer := g.feedViewer(kwargs, spec)
	limit := toBoundedPositiveInt(firstPresent(kwargs["limit"], spec["limit"]), 20, 100)
	offset := toBoundedNonNegativeInt(firstPresent(kwargs["offset"], spec["offset"]), 0, 10000)
	since := toInt64(firstPresent(kwargs["since"], spec["since"]), 0)
	until := toInt64(firstPresent(kwargs["until"], spec["until"]), 0)
	kinds := toIntSlice(kwargs["kinds"])
	if len(kinds) == 0 {
		kinds = toIntSlice(spec["kinds"])
	}
	if stringValue(spec["dvm_pubkey"]) != "" && stringValue(spec["dvm_id"]) != "" {
		return g.emptyFeedPage(), nil
	}
	if id == "nostr-reads-feed" || id == "reads-feed" || kind == "reads" || containsKind(kinds, 30023) {
		return g.readsPage(ctx, viewer, spec, limit, offset)
	}
	switch id {
	case "global-trending", "most-zapped", "all-notes":
		return g.scoredFeedPage(ctx, viewer, id, spec, kwargs, kinds, limit)
	case "advsearch":
		return g.searchFeedPage(ctx, viewer, spec, kwargs, limit, offset, until)
	}
	if id == "video-reels" {
		return g.videoReelsPage(ctx, kwargs, spec, limit)
	}
	if strings.HasPrefix(id, "explore") || isStubbedFeed(id) {
		return g.emptyFeedPage(), nil
	}
	if id == "latest" || (kind == "notes" && id == "latest") {
		if err := requireViewer(viewer); err != nil {
			return nil, err
		}
		if len(kinds) == 0 {
			kinds = []int{1, 6}
		}
		return g.followsFeedPage(ctx, viewer, kinds, since, until, limit, offset, boolValue(spec["include_replies"], false), false)
	}
	if id == "feed" || kind == "notes" {
		switch notes {
		case "follows":
			if err := requireViewer(viewer); err != nil {
				return nil, err
			}
			if len(kinds) == 0 {
				kinds = []int{1, 6}
			}
			return g.followsFeedPage(ctx, viewer, kinds, since, until, limit, offset, boolValue(spec["include_replies"], false), false)
		case "authored":
			author := strings.TrimSpace(stringValue(spec["pubkey"]))
			if author == "" {
				author = viewer
			}
			if err := validatePubkeyHex(author); err != nil {
				return nil, errors.New("error")
			}
			if len(kinds) == 0 {
				kinds = []int{1}
			}
			return g.authorFeedPage(ctx, viewer, []string{author}, kinds, since, until, limit, offset, false, false, false)
		case "replies":
			author := strings.TrimSpace(stringValue(spec["pubkey"]))
			if author == "" {
				author = viewer
			}
			if err := validatePubkeyHex(author); err != nil {
				return nil, errors.New("error")
			}
			if len(kinds) == 0 {
				kinds = []int{1}
			}
			return g.authorFeedPage(ctx, viewer, []string{author}, kinds, since, until, limit, offset, true, true, false)
		case "user_media_thumbnails":
			author := strings.TrimSpace(stringValue(spec["pubkey"]))
			if err := validatePubkeyHex(author); err != nil {
				return nil, errors.New("error")
			}
			return g.authorFeedPage(ctx, viewer, []string{author}, []int{1, 20, 21, 22, 34235, 34236}, since, until, limit, offset, false, false, true)
		case "bookmarks":
			return g.bookmarkNotesPage(ctx, viewer, strings.TrimSpace(stringValue(spec["pubkey"])), limit)
		default:
			if kind == "notes" {
				return []any{}, nil
			}
		}
	}
	if id == "search" {
		return nil, errors.New("error")
	}
	return []any{}, nil
}

func (g WSGateway) primalV1Thread(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID := strings.TrimSpace(stringValue(kwargs["event_id"]))
	if eventID == "" {
		return nil, errors.New("error")
	}
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	view, err := g.query.GetThreadWindow(ctx, query.ThreadWindowRequest{
		EventID:  eventID,
		Limit:    limit,
		Offset:   offset,
		MaxDepth: 1,
	})
	if err != nil {
		return nil, errors.New("error")
	}
	viewer := g.feedViewer(kwargs, kwargs)
	kinds := toIntSlice(kwargs["kinds"])
	replies := filterRawEvents(view.Replies, kinds, toInt64(kwargs["since"], 0), toInt64(kwargs["until"], 0))
	out := g.enrichFeedEvents(ctx, viewer, replies, "created_at", nil, nil)
	if _, ok := kwargs["include_parent_posts"]; ok && !boolValue(kwargs["include_parent_posts"], true) {
		return out, nil
	}
	parents := make([]json.RawMessage, 0, len(view.Ancestors)+1)
	parents = append(parents, view.Ancestors...)
	if len(view.Event) > 0 {
		parents = append(parents, view.Event)
	}
	out = append(out, g.enrichFeedEvents(ctx, viewer, parents, "created_at", nil, nil)...)
	return out, nil
}

func (g WSGateway) primalV1Events(ctx context.Context, kwargs map[string]any) ([]any, error) {
	ids := toStringSlice(kwargs["event_ids"])
	if len(ids) == 0 {
		ids = toStringSlice(kwargs["events"])
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil, errors.New("error")
	}
	raws := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := found[id]; ok {
			raws = append(raws, raw)
		}
	}
	if boolValue(kwargs["extended_response"], false) {
		return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), raws, "created_at", nil, nil), nil
	}
	return rawMessagesToAny(raws), nil
}

func (g WSGateway) primalV1Bookmarks(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	return g.bookmarkNotesPage(ctx, g.feedViewer(kwargs, kwargs), pubkey, limit)
}

func (g WSGateway) primalV1Highlights(ctx context.Context, kwargs map[string]any) ([]any, error) {
	events, err := g.resolveHighlightsResponse(ctx, kwargs)
	if err != nil {
		return nil, err
	}
	return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), jsonRawEvents(events), "created_at", nil, nil), nil
}

func (g WSGateway) primalV1LongFormFeed(ctx context.Context, kwargs map[string]any) ([]any, error) {
	events, err := g.resolveLongFormContentFeed(ctx, kwargs)
	if err != nil {
		return nil, err
	}
	return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), jsonRawEvents(events), "created_at", nil, nil), nil
}

func (g WSGateway) primalV1LongFormThread(ctx context.Context, kwargs map[string]any) ([]any, error) {
	events, err := g.resolveLongFormContentThreadView(ctx, kwargs)
	if err != nil {
		return nil, err
	}
	return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), jsonRawEvents(events), "created_at", nil, nil), nil
}

func (g WSGateway) followsFeedPage(ctx context.Context, viewer string, kinds []int, since, until int64, limit, offset int, includeReplies, hasImage bool) ([]any, error) {
	list, err := g.query.GetContactList(ctx, viewer)
	if err != nil {
		return nil, errors.New("error")
	}
	authors := make([]string, 0)
	for pubkey := range parseContactListPubkeys(list.ContactsJSONRaw) {
		authors = append(authors, pubkey)
	}
	if len(authors) == 0 {
		return g.emptyFeedPage(), nil
	}
	return g.authorFeedPage(ctx, viewer, authors, kinds, since, until, limit, offset, includeReplies, false, hasImage)
}

func (g WSGateway) authorFeedPage(ctx context.Context, viewer string, authors []string, kinds []int, since, until int64, limit, offset int, includeReplies, onlyReplies, hasImage bool) ([]any, error) {
	fetchLimit := limit * 2
	if fetchLimit > 100 {
		fetchLimit = 100
	}
	if fetchLimit < limit {
		fetchLimit = limit
	}
	rows, err := g.query.GetFeedEvents(ctx, query.FeedEventsRequest{
		Authors:        authors,
		Kinds:          kinds,
		Since:          since,
		Until:          until,
		Limit:          fetchLimit,
		Offset:         offset,
		IncludeReplies: includeReplies,
		OnlyReplies:    onlyReplies,
		HasImage:       hasImage,
	})
	if err != nil && query.IsUnsupportedCapability(err) && len(authors) == 1 && offset == 0 && !hasImage {
		if onlyReplies {
			rows, err = g.query.GetAuthorReplies(ctx, authors[0], limit)
		} else {
			rows, err = g.query.GetAuthorEvents(ctx, authors[0], limit)
		}
		if err == nil {
			rows = filterRawEvents(rows, kinds, since, until)
		}
	}
	if err != nil {
		return nil, errors.New("error")
	}
	rows = g.repairReposts(ctx, rows)
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return g.enrichFeedEvents(ctx, viewer, rows, "created_at", nil, nil), nil
}

func (g WSGateway) scoredFeedPage(ctx context.Context, viewer string, id string, spec map[string]any, kwargs map[string]any, kinds []int, limit int) ([]any, error) {
	if id == "all-notes" {
		if len(kinds) == 0 {
			kinds = []int{1}
		}
		offset := toBoundedNonNegativeInt(firstPresent(kwargs["offset"], spec["offset"]), 0, 10000)
		since := toInt64(firstPresent(kwargs["since"], spec["since"]), 0)
		until := toInt64(firstPresent(kwargs["until"], spec["until"]), 0)
		return g.authorFeedPage(ctx, viewer, nil, kinds, since, until, limit, offset, false, false, false)
	}
	hours := trendingHours(spec)
	scoreColumn := "score_24h"
	if hours > 24 {
		scoreColumn = "score_7d"
	}
	var maxRank *int64
	if _, ok := kwargs["until"]; ok {
		cursor := toInt64(kwargs["until"], 0)
		maxRank = &cursor
	}
	// The iOS client sends offset 1 as "skip the boundary row", not as a page index.
	offset := 0
	if maxRank != nil {
		offset = toBoundedNonNegativeInt(kwargs["offset"], 0, 100)
	}
	notes, err := g.query.GetPrimalTrendingNotes(ctx, scoreColumn, time.Now().Add(-time.Duration(hours)*time.Hour).Unix(), limit, offset, maxRank, strings.Contains(id, "zapped"))
	if err != nil {
		return nil, errors.New("error")
	}
	ids := make([]string, 0, len(notes))
	for _, note := range notes {
		ids = append(ids, note.EventID)
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil, errors.New("error")
	}
	raws := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		raw, ok := found[id]
		if !ok {
			continue
		}
		if len(kinds) > 0 && !containsKind(kinds, rawKind(raw)) {
			continue
		}
		raws = append(raws, raw)
	}
	var since any
	var until any
	if len(notes) > 0 {
		until = int64(notes[0].Score)
		since = int64(notes[len(notes)-1].Score)
	}
	return g.enrichFeedEvents(ctx, viewer, raws, "score", since, until), nil
}

func (g WSGateway) searchFeedPage(ctx context.Context, viewer string, spec map[string]any, kwargs map[string]any, limit, offset int, until int64) ([]any, error) {
	text := strings.TrimSpace(stringValue(spec["query"]))
	if text == "" {
		text = strings.TrimSpace(stringValue(kwargs["query"]))
	}
	if strings.Contains(strings.ToLower(text), "lolicon") || text == "" {
		return g.emptyFeedPage(), nil
	}
	if strings.HasPrefix(strings.ToLower(text), "kind:0") {
		return g.primalV1UserSearch(ctx, map[string]any{"query": strings.TrimSpace(text[6:]), "limit": limit})
	}
	rows, err := g.query.SearchNotes(ctx, query.NotesSearchParams{
		Query:           text,
		Limit:           limit,
		Offset:          offset,
		Sort:            "latest",
		BeforeCreatedAt: until,
	})
	if err != nil {
		return nil, errors.New("error")
	}
	return g.enrichFeedEvents(ctx, viewer, rows, "created_at", nil, nil), nil
}

func (g WSGateway) readsPage(ctx context.Context, viewer string, spec map[string]any, limit, offset int) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(spec["pubkey"]))
	if pubkey != "" {
		rows, err := g.query.GetLongForm(ctx, pubkey, limit)
		if err != nil {
			return nil, errors.New("error")
		}
		return g.enrichFeedEvents(ctx, viewer, rows, "created_at", nil, nil), nil
	}
	notes, err := g.query.GetTrendingLongForm(ctx, 7*24*time.Hour, query.DiscoveryScope{}, limit, offset)
	if err != nil {
		return nil, errors.New("error")
	}
	ids := make([]string, 0, len(notes))
	for _, note := range notes {
		ids = append(ids, note.EventID)
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil, errors.New("error")
	}
	raws := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := found[id]; ok {
			raws = append(raws, raw)
		}
	}
	return g.enrichFeedEvents(ctx, viewer, raws, "created_at", nil, nil), nil
}

func (g WSGateway) bookmarkNotesPage(ctx context.Context, viewer string, pubkey string, limit int) ([]any, error) {
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	lists, err := g.query.GetBookmarks(ctx, pubkey, 5)
	if err != nil {
		return nil, errors.New("error")
	}
	ids := make([]string, 0)
	for _, raw := range lists {
		ids = append(ids, tagValuesFromRawEvent(raw, "e")...)
	}
	if len(ids) > limit {
		ids = ids[:limit]
	}
	if len(ids) == 0 {
		return g.enrichFeedEvents(ctx, viewer, lists, "created_at", nil, nil), nil
	}
	found, err := g.query.GetEventBatch(ctx, ids)
	if err != nil {
		return nil, errors.New("error")
	}
	raws := make([]json.RawMessage, 0, len(ids))
	for _, id := range ids {
		if raw, ok := found[id]; ok {
			raws = append(raws, raw)
		}
	}
	return g.enrichFeedEvents(ctx, viewer, raws, "created_at", nil, nil), nil
}

func (g WSGateway) emptyFeedPage() []any {
	return []any{buildPrimalRange("created_at", nil, nil, []string{})}
}

func (g WSGateway) feedViewer(kwargs map[string]any, spec map[string]any) string {
	for _, value := range []any{kwargs["user_pubkey"], spec["user_pubkey"]} {
		pubkey := strings.ToLower(strings.TrimSpace(stringValue(value)))
		if pubkey != "" {
			return pubkey
		}
	}
	return strings.ToLower(strings.TrimSpace(g.opts.ServerPubkey))
}

func requireViewer(pubkey string) error {
	if err := validatePubkeyHex(pubkey); err != nil {
		return errors.New("error")
	}
	return nil
}

func firstPresent(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func containsKind(kinds []int, kind int) bool {
	for _, item := range kinds {
		if item == kind {
			return true
		}
	}
	return false
}

func isStubbedFeed(id string) bool {
	switch id {
	case "wide-net-notes", "strange-but-popular", "engaging-discussions", "only-tweets", "bookmarked-by-follows":
		return true
	default:
		return false
	}
}

func filterRawEvents(rows []json.RawMessage, kinds []int, since, until int64) []json.RawMessage {
	if len(kinds) == 0 && since == 0 && until == 0 {
		return rows
	}
	out := make([]json.RawMessage, 0, len(rows))
	for _, raw := range rows {
		if len(kinds) > 0 && !containsKind(kinds, rawKind(raw)) {
			continue
		}
		created, ok := createdAtFromRaw(raw)
		if ok && since > 0 && created < since {
			continue
		}
		if ok && until > 0 && created > until {
			continue
		}
		out = append(out, raw)
	}
	return out
}

func rawKind(raw json.RawMessage) int {
	var payload struct {
		Kind int `json:"kind"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0
	}
	return payload.Kind
}

func trendingHours(spec map[string]any) int {
	if hours := knownTrendingHour(spec["hours"]); hours > 0 {
		return hours
	}
	if hours := knownTrendingHour(spec["since"]); hours > 0 {
		return hours
	}
	switch strings.ToLower(strings.TrimSpace(stringValue(spec["timeframe"]))) {
	case "1h", "trending_1h":
		return 1
	case "4h", "trending_4h":
		return 4
	case "12h", "trending_12h":
		return 12
	case "48h", "trending_48h":
		return 48
	case "7d", "trending_7d", "168h":
		return 168
	default:
		return 24
	}
}

func knownTrendingHour(value any) int {
	switch toInt(value, 0) {
	case 1, 4, 12, 24, 48, 168:
		return toInt(value, 0)
	default:
		return 0
	}
}

func (g WSGateway) repairReposts(ctx context.Context, rows []json.RawMessage) []json.RawMessage {
	targets := make([]string, 0)
	needs := make([]int, 0)
	for i, raw := range rows {
		if repostTarget(raw) == "" || repostContentReady(raw) {
			continue
		}
		needs = append(needs, i)
		targets = append(targets, repostTarget(raw))
	}
	found := map[string]json.RawMessage{}
	if len(targets) > 0 {
		if loaded, err := g.query.GetEventBatch(ctx, targets); err == nil {
			found = loaded
		}
	}
	out := make([]json.RawMessage, 0, len(rows))
	pending := map[int]struct{}{}
	for _, index := range needs {
		pending[index] = struct{}{}
	}
	for i, raw := range rows {
		if _, ok := pending[i]; !ok {
			out = append(out, raw)
			continue
		}
		target, ok := found[repostTarget(raw)]
		if !ok {
			continue
		}
		rewritten := rewriteRepostContent(raw, target)
		if rewritten == nil {
			continue
		}
		out = append(out, rewritten)
	}
	return out
}

func repostContentReady(raw json.RawMessage) bool {
	var event struct {
		Kind    int    `json:"kind"`
		Content string `json:"content"`
	}
	if json.Unmarshal(raw, &event) != nil || event.Kind != 6 {
		return true
	}
	var inner struct {
		ID string `json:"id"`
	}
	return json.Unmarshal([]byte(event.Content), &inner) == nil && inner.ID != ""
}

func repostTarget(raw json.RawMessage) string {
	var event struct {
		Kind int `json:"kind"`
	}
	if json.Unmarshal(raw, &event) != nil || event.Kind != 6 {
		return ""
	}
	ids := tagValuesFromRawEvent(raw, "e")
	if len(ids) == 0 {
		return ""
	}
	return ids[0]
}

func rewriteRepostContent(raw json.RawMessage, target json.RawMessage) json.RawMessage {
	var event map[string]any
	if json.Unmarshal(raw, &event) != nil {
		return nil
	}
	event["content"] = string(target)
	encoded, err := json.Marshal(event)
	if err != nil {
		return nil
	}
	return encoded
}

func jsonRawEvents(events []any) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		switch typed := event.(type) {
		case json.RawMessage:
			out = append(out, typed)
		case []byte:
			out = append(out, json.RawMessage(typed))
		}
	}
	return out
}
