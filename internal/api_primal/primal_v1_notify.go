package api_primal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (g WSGateway) primalV1Notifications(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.ToLower(strings.TrimSpace(stringValue(kwargs["pubkey"])))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	limit := toBoundedPositiveInt(kwargs["limit"], 50, 50)
	offset := toBoundedNonNegativeInt(kwargs["offset"], 0, 10000)
	rows, err := g.query.ListNotifications(ctx, pubkey, notificationTypes(kwargs), toInt64(kwargs["since"], 0), toInt64(kwargs["until"], 0), limit, offset)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			return []any{}, nil
		}
		return nil, errors.New("error")
	}
	out := make([]any, 0, len(rows)+8)
	pubkeys := make([]string, 0)
	seen := map[string]struct{}{}
	refIDs := make([]string, 0)
	for _, row := range rows {
		body := map[string]any{}
		if len(row.Args) > 0 {
			_ = json.Unmarshal(row.Args, &body)
		}
		body["created_at"] = row.CreatedAt
		body["pubkey"] = row.Pubkey
		body["type"] = row.Type
		encoded, _ := json.Marshal(body)
		sum := sha256.Sum256(encoded)
		body["id"] = hex.EncodeToString(sum[:])
		out = append(out, kindObject(primalKindNotification, body))
		g.rememberPubkey(&pubkeys, seen, stringField(body, "user"), 50)
		g.rememberPubkey(&pubkeys, seen, stringField(body, "follower"), 50)
		if id := stringField(body, "event_id"); id != "" {
			refIDs = append(refIDs, id)
		}
		if id := stringField(body, "reply"); id != "" {
			refIDs = append(refIDs, id)
		}
	}
	for _, pubkey := range pubkeys {
		out = append(out, kindObject(primalKindUserStats, map[string]any{
			"pubkey":          pubkey,
			"followers_count": int64(0),
		}))
	}
	if counts, err := g.query.GetFollowerCounts(ctx, pubkeys); err == nil {
		for i := range out {
			event, ok := out[i].(map[string]any)
			if !ok {
				continue
			}
			kind, ok := kindOf(event["kind"])
			if !ok || kind != primalKindUserStats {
				continue
			}
			var body struct {
				Pubkey string `json:"pubkey"`
			}
			_ = json.Unmarshal([]byte(stringValue(event["content"])), &body)
			if count, ok := counts[body.Pubkey]; ok {
				out[i] = kindObject(primalKindUserStats, map[string]any{
					"pubkey":          body.Pubkey,
					"followers_count": count,
				})
			}
		}
	}
	if len(refIDs) > 0 {
		found, err := g.query.GetEventBatch(ctx, refIDs)
		if err == nil {
			raws := make([]json.RawMessage, 0, len(refIDs))
			for _, id := range refIDs {
				if raw, ok := found[id]; ok {
					raws = append(raws, raw)
				}
			}
			out = append(out, g.enrichFeedEvents(ctx, pubkey, raws, "created_at", nil, nil)...)
		}
	}
	return out, nil
}

func (g WSGateway) primalV1NotificationsSeen(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.ToLower(strings.TrimSpace(stringValue(kwargs["pubkey"])))
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	seen, err := g.query.NotificationsSeen(ctx, pubkey)
	if err != nil && !query.IsNotFound(err) && !query.IsUnsupportedCapability(err) {
		return nil, errors.New("error")
	}
	return []any{kindContent(primalKindSeenTimestamp, strconv.FormatInt(seen, 10))}, nil
}

func (g WSGateway) primalV1SetNotificationsSeen(ctx context.Context, kwargs map[string]any) ([]any, error) {
	raw, ok := kwargs["event_from_user"]
	if !ok {
		return nil, errors.New("error")
	}
	event, err := parseSignedUserEvent(raw, 0)
	if err != nil {
		return nil, err
	}
	if err := g.query.MarkNotificationsSeen(ctx, event.Pubkey, event.CreatedAt); err != nil {
		return nil, errors.New("error")
	}
	return []any{kindContent(primalKindSeenTimestamp, strconv.FormatInt(event.CreatedAt, 10))}, nil
}

func (g WSGateway) primalV1NotificationCounts(ctx context.Context, kwargs map[string]any) ([]any, error) {
	pubkey := strings.ToLower(strings.TrimSpace(stringValue(kwargs["pubkey"])))
	if pubkey == "" {
		pubkey = strings.ToLower(strings.TrimSpace(stringValue(kwargs["user_pubkey"])))
	}
	if err := validatePubkeyHex(pubkey); err != nil {
		return nil, errors.New("error")
	}
	version := 1
	if boolValue(kwargs["version2"], false) {
		version = 2
	}
	frame, ok := g.notificationCountFrame(ctx, notifyLiveSubscription{Pubkey: pubkey, Version: version})
	if !ok {
		return []any{}, nil
	}
	event, _ := frame.([]any)
	if len(event) >= 3 {
		return []any{event[2]}, nil
	}
	return []any{}, nil
}

func (g WSGateway) notificationCountFrame(ctx context.Context, sub notifyLiveSubscription) (any, bool) {
	counts, err := g.query.NotificationCounts(ctx, sub.Pubkey)
	if err != nil || len(counts) == 0 {
		return nil, false
	}
	if sub.Version == 2 {
		return []any{"EVENT", sub.SubID, notificationStats2(sub.Pubkey, counts)}, true
	}
	return []any{"EVENT", sub.SubID, notificationStats(sub.Pubkey, counts)}, true
}

func notificationStats(pubkey string, counts []readmodel.NotificationCount) map[string]any {
	out := map[string]any{"kind": primalKindNotifStats, "pubkey": pubkey}
	for _, count := range counts {
		out[strconv.Itoa(count.Type)] = count.Cnt
	}
	return out
}

func notificationStats2(pubkey string, counts []readmodel.NotificationCount) map[string]any {
	body := map[string]any{}
	for _, count := range counts {
		body[strconv.Itoa(count.Type)] = count.Cnt
	}
	raw, _ := json.Marshal(body)
	return map[string]any{
		"kind":    primalKindNotifStats2,
		"pubkey":  pubkey,
		"content": string(raw),
	}
}

func notificationTypes(kwargs map[string]any) []int {
	switch strings.ToLower(strings.TrimSpace(stringValue(kwargs["type_group"]))) {
	case "zaps":
		return []int{3}
	case "replies":
		return []int{6, 601}
	case "mentions":
		return []int{7, 8}
	case "reposts":
		return []int{5}
	default:
		return nil
	}
}

func stringField(body map[string]any, key string) string {
	value, _ := body[key].(string)
	return strings.TrimSpace(value)
}
