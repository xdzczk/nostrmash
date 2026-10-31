package readmodel

import (
	"encoding/json"
	"fmt"
	"strings"
)

// TopZapAd is the largest zap on a note, used as a zapvertisement.
type TopZapAd struct {
	EventID      string
	Sats         int64
	SenderPubkey string
	Name         string
	DisplayName  string
	Picture      string
	Comment      string
}

// TopZapPayload is the public JSON shape for one top zap.
func TopZapPayload(ad TopZapAd) map[string]any {
	payload := map[string]any{
		"sats":          ad.Sats,
		"sender_pubkey": ad.SenderPubkey,
	}
	if ad.Name != "" {
		payload["name"] = ad.Name
	}
	if ad.DisplayName != "" {
		payload["display_name"] = ad.DisplayName
	}
	if ad.Picture != "" {
		payload["picture"] = ad.Picture
	}
	if ad.Comment != "" {
		payload["comment"] = ad.Comment
	}
	return payload
}

// LookupTopZap finds an ad by event id, ignoring hex case.
func LookupTopZap(ads map[string]TopZapAd, eventID string) (TopZapAd, bool) {
	if len(ads) == 0 {
		return TopZapAd{}, false
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return TopZapAd{}, false
	}
	if ad, ok := lookupReadyTopZap(ads, eventID); ok {
		return ad, true
	}
	if ad, ok := lookupReadyTopZap(ads, strings.ToLower(eventID)); ok {
		return ad, true
	}
	return TopZapAd{}, false
}

func lookupReadyTopZap(ads map[string]TopZapAd, key string) (TopZapAd, bool) {
	ad, ok := ads[key]
	if !ok || ad.Sats <= 0 || strings.TrimSpace(ad.SenderPubkey) == "" {
		return TopZapAd{}, false
	}
	return ad, true
}

// MergeTopZapIntoRaw attaches top_zap onto an event object.
func MergeTopZapIntoRaw(raw json.RawMessage, ad TopZapAd) (json.RawMessage, error) {
	if len(raw) == 0 || ad.Sats <= 0 || strings.TrimSpace(ad.SenderPubkey) == "" {
		return raw, nil
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, fmt.Errorf("decode event raw for top zap: %w", err)
	}
	if payload == nil {
		return raw, nil
	}
	payload["top_zap"] = TopZapPayload(ad)
	out, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode event raw after top zap: %w", err)
	}
	return out, nil
}
