package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) primalV1UserProfile(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.ToLower(strings.TrimSpace(stringValue(kwargs["pubkey"])))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	summary, err := g.query.GetProfilePublicSummary(ctx, pubkey)
	if err != nil {
		return nil, errors.New("error")
	}
	out := []any{g.kind0Event(ctx, summary.Profile)}
	stats := map[string]any{
		"pubkey":               pubkey,
		"follows_count":        summary.Stats.FollowingCount,
		"followers_count":      summary.Stats.FollowerCount,
		"note_count":           summary.Stats.NoteCount,
		"long_form_note_count": int64(0),
		"reply_count":          summary.Stats.ReplyCount,
		"time_joined":          nil,
		"relay_count":          0,
		"total_zap_count":      int64(0),
		"total_satszapped":     int64(0),
		"media_count":          0,
		"content_zap_count":    int64(0),
	}
	if count, err := g.query.CountAuthorKind(ctx, pubkey, 30023); err == nil {
		stats["long_form_note_count"] = count
	}
	if oldest, ok, err := g.query.GetOldestCreatedAt(ctx, pubkey); err == nil && ok {
		stats["time_joined"] = oldest
	}
	if relays, err := g.relayTagsForPubkey(ctx, pubkey); err == nil {
		stats["relay_count"] = len(relays)
	}
	if zaps, err := g.query.GetZapTotals(ctx, pubkey); err == nil {
		stats["total_zap_count"] = zaps.Count
		stats["total_satszapped"] = zaps.Sats
	}
	out = append(out, kindObject(primalKindUserStats, stats))
	return out, nil
}

func (g WSGateway) primalV1UserInfos(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkeys := toStringSlice(kwargs["pubkeys"])
	if len(pubkeys) == 0 {
		if pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"])); pubkey != "" {
			pubkeys = []string{pubkey}
		}
	}
	normalized := make([]string, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.ToLower(strings.TrimSpace(pubkey))
		if err := validatePubkeyHex(pubkey); err != nil {
			continue
		}
		normalized = append(normalized, pubkey)
	}
	if len(normalized) == 0 {
		return []any{}, nil
	}
	infos, err := g.query.GetUserInfos(ctx, normalized)
	if err != nil {
		return nil, errors.New("error")
	}
	out := make([]any, 0, len(infos.Profiles)+2)
	present := make([]string, 0, len(infos.Profiles))
	for _, profile := range infos.Profiles {
		out = append(out, g.kind0Event(ctx, profile))
		if profile.Pubkey != "" {
			present = append(present, strings.ToLower(profile.Pubkey))
		}
	}
	counts := map[string]int64{}
	if loaded, err := g.query.GetFollowerCounts(ctx, present); err == nil {
		counts = loaded
	}
	if len(present) > 0 {
		out = append(out, followerMapEvent(primalKindUserScore, counts, present))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, present))
	}
	return out, nil
}

func (g WSGateway) kind0Event(ctx context.Context, profile query.Profile) any {
	if id := strings.TrimSpace(profile.MetadataEventID); id != "" {
		if raw, err := g.query.GetEventByID(ctx, id); err == nil && len(raw) > 0 {
			return raw
		}
	}
	content := strings.TrimSpace(string(profile.ProfileJSON))
	if content == "" {
		content = "{}"
	}
	return map[string]any{
		"id":         profile.MetadataEventID,
		"pubkey":     profile.Pubkey,
		"created_at": profile.MetadataCreatedAt,
		"kind":       0,
		"tags":       []any{},
		"content":    content,
		"sig":        "",
	}
}

func (g WSGateway) primalV1IsUserFollowing(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	userPubkey := strings.TrimSpace(stringValue(kwargs["user_pubkey"]))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	if err := validatePubkeyHex(userPubkey); err != nil {
		return nil, errors.New("error")
	}
	follows, err := g.query.IsUserFollowing(ctx, pubkey, userPubkey)
	if err != nil {
		return nil, errors.New("error")
	}
	content := "false"
	if follows {
		content = "true"
	}
	return []any{kindContent(primalKindFollowingUser, content)}, nil
}

func (g WSGateway) primalV1ContactList(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	raws, err := g.query.GetRecentEventsByKindAndPubkey(ctx, 3, pubkey, 1)
	if err != nil {
		return nil, errors.New("error")
	}
	if len(raws) == 0 {
		return []any{}, nil
	}
	extended := true
	if _, ok := kwargs["extended_response"]; ok {
		extended = boolValue(kwargs["extended_response"], true)
	}
	out := []any{raws[0]}
	if !extended {
		return out, nil
	}
	follows := tagValuesFromRawEvent(raws[0], "p")
	if len(follows) > 100 {
		follows = follows[:100]
	}
	out = append(out, g.buildMetadataEvents(ctx, follows)...)
	counts := map[string]int64{}
	if loaded, err := g.query.GetFollowerCounts(ctx, follows); err == nil {
		counts = loaded
	}
	if len(follows) > 0 {
		out = append(out, followerMapEvent(primalKindUserScore, counts, follows))
		out = append(out, followerMapEvent(primalKindUserFollowers, counts, follows))
	}
	return out, nil
}

func (g WSGateway) primalV1UserRelays(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	event, err := g.userRelayEvent(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	if event == nil {
		return []any{}, nil
	}
	return []any{event}, nil
}

func (g WSGateway) primalV1UserRelays2(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkeys := toStringSlice(kwargs["pubkeys"])
	out := make([]any, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		pubkey = strings.TrimSpace(pubkey)
		if err := validatePubkeyHex(pubkey); err != nil {
			continue
		}
		event, err := g.userRelayEvent(ctx, pubkey)
		if err != nil || event == nil {
			continue
		}
		out = append(out, event)
	}
	return out, nil
}

func (g WSGateway) userRelayEvent(ctx context.Context, pubkey string) (map[string]any, error) {
	tags, err := g.relayTagsForPubkey(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return map[string]any{
		"kind":   primalKindRelays,
		"pubkey": pubkey,
		"tags":   tags,
	}, nil
}

func (g WSGateway) relayTagsForPubkey(ctx context.Context, pubkey string) ([]any, error) {
	raws, err := g.query.GetRecentEventsByKindAndPubkey(ctx, 10002, pubkey, 1)
	if err != nil && !query.IsUnsupportedCapability(err) {
		return nil, errors.New("error")
	}
	if len(raws) > 0 {
		tags := relayTagsFromEvent(raws[0])
		if len(tags) > 0 {
			return tags, nil
		}
	}
	lists, err := g.query.GetRecentEventsByKindAndPubkey(ctx, 3, pubkey, 1)
	if err != nil || len(lists) == 0 {
		return []any{}, nil
	}
	var payload struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(lists[0], &payload); err != nil {
		return []any{}, nil
	}
	var relays map[string]map[string]bool
	if err := json.Unmarshal([]byte(payload.Content), &relays); err != nil {
		return []any{}, nil
	}
	urls := make([]string, 0, len(relays))
	for url := range relays {
		urls = append(urls, url)
	}
	sort.Strings(urls)
	tags := make([]any, 0, len(urls))
	for _, url := range urls {
		flags := relays[url]
		switch {
		case flags["read"] && flags["write"]:
			tags = append(tags, []any{"r", url})
		case flags["read"]:
			tags = append(tags, []any{"r", url, "read"})
		case flags["write"]:
			tags = append(tags, []any{"r", url, "write"})
		default:
			tags = append(tags, []any{"r", url})
		}
	}
	return tags, nil
}

func relayTagsFromEvent(raw json.RawMessage) []any {
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil
	}
	out := make([]any, 0)
	for _, tag := range payload.Tags {
		if len(tag) < 2 || tag[0] != "r" || strings.TrimSpace(tag[1]) == "" {
			continue
		}
		item := make([]any, 0, len(tag))
		for _, field := range tag {
			item = append(item, field)
		}
		out = append(out, item)
	}
	return out
}

func (g WSGateway) primalV1ReplaceableEvent(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	kind := toInt(kwargs["kind"], 0)
	if kind <= 0 {
		return nil, errors.New("error")
	}
	raws, err := g.query.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, 1)
	if err == nil && len(raws) > 0 {
		return []any{raws[0]}, nil
	}
	raw, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, kind, strings.TrimSpace(stringValue(kwargs["identifier"])))
	if err != nil {
		if query.IsNotFound(err) {
			return []any{}, nil
		}
		return nil, errors.New("error")
	}
	if len(raw) == 0 {
		return []any{}, nil
	}
	return []any{raw}, nil
}

func (g WSGateway) primalV1FollowList(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.TrimSpace(stringValue(kwargs["pubkey"]))
	identifier := strings.TrimSpace(stringValue(kwargs["identifier"]))
	if err := validatePubkeyHex(pubkey); err != nil || identifier == "" {
		return nil, errors.New("error")
	}
	raw, err := g.query.GetParameterizedReplaceableEvent(ctx, pubkey, 39089, identifier)
	if err != nil {
		if query.IsNotFound(err) {
			return []any{}, nil
		}
		return nil, errors.New("error")
	}
	out := []any{raw}
	infos, err := g.primalV1UserInfos(ctx, map[string]any{"pubkeys": tagValuesFromRawEvent(raw, "p")})
	if err == nil {
		out = append(out, infos...)
	}
	return out, nil
}

func (g WSGateway) primalV1RecommendedUsers(ctx context.Context, _ map[string]any) ([]any, error) {
	if len(g.opts.RecommendedUsers) == 0 {
		return []any{}, nil
	}
	return g.primalV1UserInfos(ctx, map[string]any{"pubkeys": g.opts.RecommendedUsers})
}

func (g WSGateway) primalV1BlossomServers(context.Context, map[string]any) ([]any, error) {
	content := "null"
	if len(g.opts.BlossomServers) > 0 {
		raw, err := json.Marshal(g.opts.BlossomServers)
		if err == nil {
			content = string(raw)
		}
	}
	return []any{kindContent(primalKindBlossomServers, content)}, nil
}
