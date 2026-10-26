package api_primal

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) primalV1Empty(context.Context, map[string]any) ([]any, error) {
	return []any{}, nil
}

func (g WSGateway) primalV1MembershipStatus(context.Context, map[string]any) ([]any, error) {
	return []any{kindObject(primalKindPremiumState, map[string]any{
		"premium": false,
		"tier":    "free",
	})}, nil
}

func (g WSGateway) primalV1ZapsBySats(ctx context.Context, kwargs map[string]any) ([]any, error) {
	kwargs["by_sats"] = true
	return g.primalV1Zaps(ctx, kwargs)
}

func (g WSGateway) primalV1EventZaps(ctx context.Context, kwargs map[string]any) ([]any, error) {
	if strings.TrimSpace(stringValue(kwargs["event_id"])) == "" {
		kwargs["event_id"] = kwargs["event"]
	}
	kwargs["by_sats"] = true
	return g.primalV1Zaps(ctx, kwargs)
}

func (g WSGateway) primalV1NotificationCounts2(ctx context.Context, kwargs map[string]any) ([]any, error) {
	kwargs["version2"] = true
	return g.primalV1NotificationCounts(ctx, kwargs)
}

func (g WSGateway) primalV1TrendingHashtags(ctx context.Context, kwargs map[string]any) ([]any, error) {
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	rows, err := g.query.GetTrendingHashtags(ctx, 24*time.Hour, query.DiscoveryScope{}, limit, 0)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			return []any{kindObject(primalKindPopularHashtags, map[string]any{"hashtags": []any{}})}, nil
		}
		return nil, errors.New("error")
	}
	return []any{kindObject(primalKindPopularHashtags, map[string]any{"hashtags": rows})}, nil
}

func (g WSGateway) primalV1NIP19Decode(_ context.Context, kwargs map[string]any) ([]any, error) {
	value := strings.TrimSpace(stringValue(kwargs["value"]))
	if value == "" {
		value = strings.TrimSpace(stringValue(kwargs["nip19"]))
	}
	value = strings.TrimPrefix(strings.ToLower(value), "nostr:")
	decoded, err := decodeNIP19(value)
	if err != nil {
		return nil, errors.New("error")
	}
	return []any{kindObject(primalKindNIP19Decode, decoded)}, nil
}

func decodeNIP19(value string) (map[string]any, error) {
	hrp, words, err := bech32.DecodeNoLimit(value)
	if err != nil {
		return nil, err
	}
	data, err := bech32.ConvertBits(words, 5, 8, false)
	if err != nil {
		return nil, err
	}
	switch hrp {
	case "npub":
		if len(data) != 32 {
			return nil, errors.New("error")
		}
		return map[string]any{"type": "npub", "data": hex.EncodeToString(data)}, nil
	case "note":
		if len(data) != 32 {
			return nil, errors.New("error")
		}
		return map[string]any{"type": "note", "data": hex.EncodeToString(data)}, nil
	case "nprofile":
		return decodeTLV(hrp, data, 0), nil
	case "nevent":
		return decodeTLV(hrp, data, 0), nil
	case "naddr":
		return decodeTLV(hrp, data, 0), nil
	default:
		return nil, errors.New("error")
	}
}

func decodeTLV(hrp string, data []byte, _ int) map[string]any {
	out := map[string]any{"type": hrp}
	relays := make([]string, 0)
	for i := 0; i+2 <= len(data); {
		tlvType := data[i]
		tlvLen := int(data[i+1])
		i += 2
		if tlvLen < 0 || i+tlvLen > len(data) {
			break
		}
		value := data[i : i+tlvLen]
		i += tlvLen
		switch tlvType {
		case 0:
			if hrp == "naddr" {
				out["identifier"] = string(value)
			} else if len(value) == 32 {
				out["data"] = hex.EncodeToString(value)
			}
		case 1:
			relays = append(relays, string(value))
		case 2:
			if len(value) == 32 {
				out["pubkey"] = hex.EncodeToString(value)
			}
		case 3:
			if len(value) == 4 {
				kind := int(value[0])<<24 | int(value[1])<<16 | int(value[2])<<8 | int(value[3])
				out["kind"] = kind
			}
		}
	}
	if len(relays) > 0 {
		out["relays"] = relays
	}
	return out
}

func (g WSGateway) primalV1NoteMentionsCount(ctx context.Context, kwargs map[string]any) ([]any, error) {
	eventID := strings.TrimSpace(stringValue(kwargs["event_id"]))
	if eventID == "" {
		return []any{kindObject(primalKindNoteStats, map[string]any{"event_id": "", "mentions": 0})}, nil
	}
	counts, err := g.query.GetEventCountsBatch(ctx, []string{eventID})
	mentions := int64(0)
	if err == nil {
		mentions = counts[eventID].ReplyCount
	}
	return []any{kindObject(primalKindNoteStats, map[string]any{"event_id": eventID, "mentions": mentions})}, nil
}
