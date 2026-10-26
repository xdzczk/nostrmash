package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) primalV1UserSearch(ctx context.Context, kwargs map[string]any) ([]any, error) {
	text := strings.TrimSpace(stringValue(kwargs["query"]))
	if text == "" || strings.Contains(strings.ToLower(text), "lolicon") {
		return []any{}, nil
	}
	limit := toBoundedPositiveInt(kwargs["limit"], 10, 100)
	if pubkey := query.CanonicalizePubkey(text); pubkey != "" && strings.HasPrefix(strings.ToLower(text), "npub1") {
		return g.primalV1UserInfos(ctx, map[string]any{"pubkeys": []string{pubkey}})
	}
	profiles, err := g.query.SearchProfiles(ctx, query.ProfileSearchParams{Query: text, Limit: limit})
	if err != nil {
		return nil, errors.New("error")
	}
	if restrict := strings.TrimSpace(stringValue(kwargs["pubkey"])); restrict != "" {
		list, listErr := g.query.GetContactList(ctx, restrict)
		if listErr != nil {
			return nil, errors.New("error")
		}
		follows := parseContactListPubkeys(list.ContactsJSONRaw)
		filtered := make([]query.Profile, 0, len(profiles))
		for _, profile := range profiles {
			if _, ok := follows[strings.ToLower(profile.Pubkey)]; ok {
				filtered = append(filtered, profile)
			}
		}
		profiles = filtered
	}
	pubkeys := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		pubkeys = append(pubkeys, profile.Pubkey)
	}
	counts := map[string]int64{}
	if loaded, err := g.query.GetFollowerCounts(ctx, pubkeys); err == nil {
		counts = loaded
		sort.SliceStable(profiles, func(i, j int) bool {
			return counts[profiles[i].Pubkey] > counts[profiles[j].Pubkey]
		})
	}
	out := make([]any, 0, len(profiles)+2)
	ordered := make([]string, 0, len(profiles))
	for _, profile := range profiles {
		out = append(out, g.kind0Event(ctx, profile))
		ordered = append(ordered, strings.ToLower(profile.Pubkey))
	}
	if len(ordered) > 0 {
		out = append(out, followerMapEvent(primalKindUserScore, counts, ordered))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, ordered))
	}
	return out, nil
}

func (g WSGateway) primalV1Search(ctx context.Context, kwargs map[string]any) ([]any, error) {
	return g.searchFeedPage(ctx, g.feedViewer(kwargs, kwargs), kwargs, kwargs, toBoundedPositiveInt(kwargs["limit"], 20, 100), toBoundedNonNegativeInt(kwargs["offset"], 0, 10000), toInt64(kwargs["until"], 0))
}

func (g WSGateway) primalV1Followers(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	rows, err := g.query.GetFollowers(ctx, pubkey, limit)
	if err != nil {
		return nil, errors.New("error")
	}
	return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), rows, "created_at", nil, nil), nil
}

func (g WSGateway) primalV1Mentions(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	rows, err := g.query.GetMentions(ctx, pubkey, limit)
	if err != nil {
		return nil, errors.New("error")
	}
	return g.enrichFeedEvents(ctx, g.feedViewer(kwargs, kwargs), rows, "created_at", nil, nil), nil
}

func (g WSGateway) primalV1Zaps(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	if pubkey == "" {
		pubkey = strings.TrimSpace(stringValue(kwargs["receiver"]))
	}
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	bySats := strings.Contains(strings.ToLower(stringValue(kwargs["order_by"])), "sats") || boolValue(kwargs["by_sats"], false)
	var rows []json.RawMessage
	var err error
	if eventID := strings.TrimSpace(stringValue(kwargs["event_id"])); eventID != "" {
		rows, err = g.query.GetEventZapsBySats(ctx, eventID, limit)
	} else if bySats {
		rows, err = g.query.GetUserZapsBySats(ctx, pubkey, limit)
	} else {
		rows, err = g.query.GetZaps(ctx, pubkey, limit)
	}
	if err != nil {
		return nil, errors.New("error")
	}
	return g.zapPage(ctx, g.feedViewer(kwargs, kwargs), rows), nil
}

func (g WSGateway) zapPage(ctx context.Context, viewer string, rows []json.RawMessage) []any {
	out := make([]any, 0, len(rows)*2+4)
	authors := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	elements := make([]string, 0, len(rows))
	for _, raw := range rows {
		out = append(out, raw)
		if id := eventIDFromRaw(raw); id != "" {
			elements = append(elements, id)
		}
		g.rememberPubkey(&authors, seen, pubkeyFromRaw(raw), 50)
		for _, pubkey := range tagValuesFromRawEvent(raw, "p") {
			g.rememberPubkey(&authors, seen, pubkey, 50)
		}
		out = append(out, kindObject(primalKindPostZaps, zapBody(raw)))
	}
	out = append(out, g.buildMetadataEvents(ctx, authors)...)
	if len(authors) > 0 {
		counts := map[string]int64{}
		if loaded, err := g.query.GetFollowerCounts(ctx, authors); err == nil {
			counts = loaded
		}
		out = append(out, followerMapEvent(primalKindUserScore, counts, authors))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, authors))
	}
	_ = viewer
	since, until := createdRange(rows)
	out = append(out, buildPrimalRange("created_at", since, until, elements))
	return out
}

func zapBody(raw json.RawMessage) map[string]any {
	amount := int64(0)
	for _, tag := range tagPairs(raw) {
		if len(tag) >= 2 && tag[0] == "amount" {
			amount = toInt64(tag[1], 0) / 1000
		}
	}
	receiver := ""
	if values := tagValuesFromRawEvent(raw, "p"); len(values) > 0 {
		receiver = values[0]
	}
	target := ""
	if values := tagValuesFromRawEvent(raw, "e"); len(values) > 0 {
		target = values[0]
	}
	created, _ := createdAtFromRaw(raw)
	return map[string]any{
		"event_id":       target,
		"created_at":     created,
		"sender":         pubkeyFromRaw(raw),
		"receiver":       receiver,
		"amount_sats":    amount,
		"zap_receipt_id": eventIDFromRaw(raw),
	}
}

func tagPairs(raw json.RawMessage) [][]string {
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	return payload.Tags
}

func (g WSGateway) primalV1DMContacts(ctx context.Context, kwargs map[string]any) ([]any, error) {
	if strings.TrimSpace(stringValue(kwargs["pubkey"])) == "" {
		kwargs["pubkey"] = stringValue(kwargs["user_pubkey"])
	}
	out, err := g.cacheDispatchDirectMessageContacts(ctx, kwargs)
	if err != nil {
		return nil, err
	}
	elements := []string{}
	for _, event := range out {
		item, ok := event.(map[string]any)
		if !ok {
			continue
		}
		kind, ok := kindOf(item["kind"])
		if !ok || kind != primalKindDirectMsgCounts {
			continue
		}
		elements = objectKeysInOrder(stringValue(item["content"]))
		break
	}
	return attachRangeElements(out, elements), nil
}

func (g WSGateway) primalV1DMMessages(ctx context.Context, kwargs map[string]any) ([]any, error) {
	if strings.TrimSpace(stringValue(kwargs["pubkey"])) == "" {
		if receiver := strings.TrimSpace(stringValue(kwargs["receiver"])); receiver != "" {
			kwargs["pubkey"] = receiver
		}
	}
	out, err := g.cacheDispatchDirectMessages(ctx, kwargs)
	if err != nil {
		return nil, err
	}
	return attachRangeElements(out, rawEventIDs(out)), nil
}
