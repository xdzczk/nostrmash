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

func (g WSGateway) primalV1EventActions(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID := strings.TrimSpace(stringValue(kwargs["event_id"]))
	if eventID == "" {
		return []any{}, nil
	}
	kind := toInt(kwargs["kind"], 0)
	if kind != 6 && kind != 7 {
		return []any{}, nil
	}
	limit := toBoundedPositiveInt(kwargs["limit"], 50, 200)
	pubkeys, err := g.query.GetEventActionPubkeys(ctx, eventID, kind, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			return []any{}, nil
		}
		return nil, errors.New("error")
	}
	return g.authorEvents(ctx, pubkeys), nil
}

func (g WSGateway) authorEvents(ctx context.Context, pubkeys []string) []any {
	if len(pubkeys) == 0 {
		return []any{}
	}
	known := map[string]query.Profile{}
	if infos, err := g.query.GetUserInfos(ctx, pubkeys); err == nil {
		for _, profile := range infos.Profiles {
			known[strings.ToLower(strings.TrimSpace(profile.Pubkey))] = profile
		}
	}
	out := make([]any, 0, len(pubkeys)+2)
	ordered := make([]string, 0, len(pubkeys))
	seen := map[string]struct{}{}
	for _, pubkey := range pubkeys {
		pubkey = strings.ToLower(strings.TrimSpace(pubkey))
		if pubkey == "" {
			continue
		}
		if _, ok := seen[pubkey]; ok {
			continue
		}
		seen[pubkey] = struct{}{}
		ordered = append(ordered, pubkey)
		if profile, ok := known[pubkey]; ok {
			out = append(out, g.kind0Event(ctx, profile))
			continue
		}
		out = append(out, stubKind0(pubkey))
	}
	counts := map[string]int64{}
	if loaded, err := g.query.GetFollowerCounts(ctx, ordered); err == nil {
		counts = loaded
	}
	if len(ordered) > 0 {
		out = append(out, followerMapEvent(primalKindUserScore, counts, ordered))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, ordered))
	}
	return out
}

func stubKind0(pubkey string) map[string]any {
	return map[string]any{
		"id":         "",
		"pubkey":     pubkey,
		"created_at": 0,
		"kind":       0,
		"tags":       []any{},
		"content":    "{}",
		"sig":        "",
	}
}

func (g WSGateway) zapPage(ctx context.Context, viewer string, rows []json.RawMessage) []any {
	out := make([]any, 0, len(rows)*2+4)
	authors := make([]string, 0, len(rows))
	seen := map[string]struct{}{}
	elements := make([]string, 0, len(rows))
	for _, raw := range rows {
		receipt, body, actors := expandZapRow(raw)
		if len(receipt) > 0 {
			out = append(out, receipt)
			if id := eventIDFromRaw(receipt); id != "" {
				elements = append(elements, id)
			}
		}
		for _, pubkey := range actors {
			g.rememberPubkey(&authors, seen, pubkey, 100)
		}
		out = append(out, kindObject(primalKindPostZaps, body))
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

func expandZapRow(raw json.RawMessage) (json.RawMessage, map[string]any, []string) {
	var wrapper struct {
		EventID  string          `json:"event_id"`
		Sender   string          `json:"sender_pubkey"`
		Receiver string          `json:"receiver_pubkey"`
		Target   string          `json:"target_event_id"`
		Sats     int64           `json:"sats"`
		Created  int64           `json:"created_at"`
		Event    json.RawMessage `json:"event"`
		Kind     int             `json:"kind"`
		ID       string          `json:"id"`
	}
	if err := json.Unmarshal(raw, &wrapper); err != nil || len(wrapper.Event) == 0 || string(wrapper.Event) == "null" || wrapper.Kind != 0 || wrapper.ID != "" {
		body := zapBody(raw)
		actors := []string{stringValue(body["sender"])}
		actors = append(actors, tagValuesFromRawEvent(raw, "p")...)
		return raw, body, actors
	}
	receipt := wrapper.Event
	if receipt[0] == '"' {
		var encoded string
		if json.Unmarshal(receipt, &encoded) == nil {
			receipt = json.RawMessage(encoded)
		}
	}
	sender := zapperPubkey(receipt)
	if sender == "" {
		sender = strings.TrimSpace(wrapper.Sender)
	}
	if sender == "" {
		sender = pubkeyFromRaw(receipt)
	}
	receiver := strings.TrimSpace(wrapper.Receiver)
	if receiver == "" {
		if values := tagValuesFromRawEvent(receipt, "p"); len(values) > 0 {
			receiver = values[0]
		}
	}
	target := strings.TrimSpace(wrapper.Target)
	if target == "" {
		if values := tagValuesFromRawEvent(receipt, "e"); len(values) > 0 {
			target = values[0]
		}
	}
	amount := wrapper.Sats
	if amount == 0 {
		amount = toInt64(zapBody(receipt)["amount_sats"], 0)
	}
	receiptID := strings.TrimSpace(wrapper.EventID)
	if receiptID == "" {
		receiptID = eventIDFromRaw(receipt)
	}
	created := wrapper.Created
	if created == 0 {
		created, _ = createdAtFromRaw(receipt)
	}
	body := map[string]any{
		"event_id":       target,
		"created_at":     created,
		"sender":         sender,
		"receiver":       receiver,
		"amount_sats":    amount,
		"zap_receipt_id": receiptID,
	}
	return receipt, body, []string{sender, receiver}
}

func zapperPubkey(receipt json.RawMessage) string {
	for _, tag := range tagPairs(receipt) {
		if len(tag) < 2 || tag[0] != "description" {
			continue
		}
		var request struct {
			Pubkey string `json:"pubkey"`
		}
		if json.Unmarshal([]byte(tag[1]), &request) != nil {
			continue
		}
		if pubkey := strings.TrimSpace(request.Pubkey); pubkey != "" {
			return pubkey
		}
	}
	return ""
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
