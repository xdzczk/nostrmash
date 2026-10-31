package read

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// TopZapAdsByEventIDs returns the largest zap for each event, if one exists.
func (s *Read) TopZapAdsByEventIDs(ctx context.Context, ids []string) (map[string]readmodel.TopZapAd, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	trimmed := uniqueTrimmedIDs(ids)
	if len(trimmed) == 0 {
		return map[string]readmodel.TopZapAd{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (zr.event_id)
			zr.event_id,
			zr.sender_pubkey,
			zr.amount_sats,
			COALESCE(e.raw_json::text, ''),
			COALESCE(p.profile_json::text, '')
		FROM zap_receipts zr
		JOIN events e ON e.id = zr.zap_receipt_id
		LEFT JOIN profiles_latest p ON p.pubkey = zr.sender_pubkey
		WHERE zr.event_id = ANY($1::text[])
		  AND zr.amount_sats > 0
		  AND zr.sender_pubkey <> ''
		ORDER BY zr.event_id, zr.amount_sats DESC, zr.created_at DESC, zr.zap_receipt_id DESC
	`, topZapLookupIDs(trimmed))
	if err != nil {
		return nil, fmt.Errorf("top zap ads: %w", err)
	}
	defer rows.Close()

	ads := make([]readmodel.TopZapAd, 0)
	for rows.Next() {
		var eventID, sender, receiptJSON, profileJSON string
		var sats int64
		if err := rows.Scan(&eventID, &sender, &sats, &receiptJSON, &profileJSON); err != nil {
			return nil, fmt.Errorf("scan top zap ad: %w", err)
		}
		ad := buildTopZapAd(eventID, sender, sats, json.RawMessage(receiptJSON), json.RawMessage(profileJSON))
		if ad.EventID == "" || ad.Sats <= 0 || ad.SenderPubkey == "" {
			continue
		}
		ads = append(ads, ad)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top zap ads: %w", err)
	}
	ads = s.hydrateTopZapProfiles(ctx, ads)

	out := make(map[string]readmodel.TopZapAd)
	byLower := make(map[string]readmodel.TopZapAd)
	for _, ad := range ads {
		indexTopZapAd(out, ad.EventID, ad)
		byLower[strings.ToLower(ad.EventID)] = ad
	}
	for _, id := range trimmed {
		ad, ok := byLower[strings.ToLower(id)]
		if !ok {
			continue
		}
		indexTopZapAd(out, id, ad)
	}
	return out, nil
}

// topZapLookupIDs returns each id and its lowercase form. Both are queried
// so a case-insensitive match can still use the event_id index.
func topZapLookupIDs(ids []string) []string {
	out := make([]string, 0, len(ids)*2)
	seen := make(map[string]struct{}, len(ids)*2)
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		for _, candidate := range []string{id, strings.ToLower(id)} {
			if candidate == "" {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			out = append(out, candidate)
		}
	}
	return out
}

func indexTopZapAd(out map[string]readmodel.TopZapAd, eventID string, ad readmodel.TopZapAd) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || ad.Sats <= 0 || strings.TrimSpace(ad.SenderPubkey) == "" {
		return
	}
	out[eventID] = ad
	out[strings.ToLower(eventID)] = ad
}

// AttachTopZaps adds top_zap onto event payloads. A lookup failure leaves the events unchanged.
func (s *Read) AttachTopZaps(ctx context.Context, events []json.RawMessage) []json.RawMessage {
	if s == nil || s.pool == nil || len(events) == 0 {
		return events
	}
	ids := make([]string, 0, len(events))
	for _, raw := range events {
		if id := eventIDFromRaw(raw); id != "" {
			ids = append(ids, id)
		}
	}
	ads, err := s.TopZapAdsByEventIDs(ctx, ids)
	if err != nil || len(ads) == 0 {
		return events
	}
	out := make([]json.RawMessage, len(events))
	for i, raw := range events {
		ad, ok := readmodel.LookupTopZap(ads, eventIDFromRaw(raw))
		if !ok {
			out[i] = raw
			continue
		}
		merged, mergeErr := readmodel.MergeTopZapIntoRaw(raw, ad)
		if mergeErr != nil || len(merged) == 0 {
			out[i] = raw
			continue
		}
		out[i] = merged
	}
	return out
}

func applyTopZapsToMap(ctx context.Context, s *Read, byID map[string]json.RawMessage) map[string]json.RawMessage {
	if s == nil || len(byID) == 0 {
		return byID
	}
	type indexed struct {
		id  string
		raw json.RawMessage
	}
	items := make([]indexed, 0, len(byID))
	for id, raw := range byID {
		items = append(items, indexed{id: id, raw: raw})
	}
	raws := make([]json.RawMessage, len(items))
	for i, item := range items {
		raws[i] = item.raw
	}
	attached := s.AttachTopZaps(ctx, raws)
	if len(attached) != len(items) {
		return byID
	}
	out := make(map[string]json.RawMessage, len(byID))
	for i, item := range items {
		out[item.id] = attached[i]
	}
	return out
}

func (s *Read) finishRawEvents(ctx context.Context, rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) ([]json.RawMessage, error) {
	events, err := scanRawText(rows)
	if err != nil {
		return nil, err
	}
	return s.AttachTopZaps(ctx, events), nil
}

func buildTopZapAd(eventID, sender string, sats int64, receipt, profileJSON json.RawMessage) readmodel.TopZapAd {
	columnSender := strings.TrimSpace(sender)
	ad := readmodel.TopZapAd{
		EventID:      strings.TrimSpace(eventID),
		Sats:         sats,
		SenderPubkey: columnSender,
	}
	if ad.EventID == "" || sats <= 0 || ad.SenderPubkey == "" {
		return readmodel.TopZapAd{}
	}
	// The receipt pubkey is the lightning service that published kind 9735.
	// The person who zapped is the zap-request author, then the NIP-57 P tag.
	if zapper := zapperPubkey(receipt, columnSender); zapper != "" {
		ad.SenderPubkey = zapper
	}
	_, comment := extractZapRequestDetails(receipt)
	ad.Comment = cleanZapText(comment, 160)
	if strings.EqualFold(ad.SenderPubkey, columnSender) {
		applyTopZapProfile(&ad, profileJSON)
	}
	return ad
}

// zapperPubkey returns the human who sent the zap. The kind-9735 event pubkey
// is only a fallback, because that key often belongs to the wallet service.
func zapperPubkey(receipt json.RawMessage, fallback string) string {
	fallback = strings.TrimSpace(fallback)
	if len(receipt) == 0 {
		return fallback
	}
	var event struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(receipt, &event); err != nil {
		return fallback
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "description" {
			continue
		}
		var request struct {
			Pubkey string `json:"pubkey"`
		}
		if err := json.Unmarshal([]byte(tag[1]), &request); err != nil {
			continue
		}
		if pubkey := canonicalPubkey(request.Pubkey); pubkey != "" {
			return pubkey
		}
	}
	if pubkey := canonicalPubkey(firstTagValueFromTags(event.Tags, "P")); pubkey != "" {
		return pubkey
	}
	if pubkey := canonicalPubkey(fallback); pubkey != "" {
		return pubkey
	}
	return fallback
}

func canonicalPubkey(value string) string {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return ""
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return ""
		}
	}
	return strings.ToLower(value)
}

func (s *Read) hydrateTopZapProfiles(ctx context.Context, ads []readmodel.TopZapAd) []readmodel.TopZapAd {
	if s == nil || s.pool == nil || len(ads) == 0 {
		return ads
	}
	ids := make([]string, 0, len(ads))
	for _, ad := range ads {
		if ad.Name != "" || ad.DisplayName != "" || ad.Picture != "" {
			continue
		}
		if ad.SenderPubkey == "" {
			continue
		}
		ids = append(ids, ad.SenderPubkey)
	}
	if len(ids) == 0 {
		return ads
	}
	profiles, err := s.profileJSONByPubkeys(ctx, ids)
	if err != nil || len(profiles) == 0 {
		return ads
	}
	for i := range ads {
		raw, ok := profiles[strings.ToLower(ads[i].SenderPubkey)]
		if !ok {
			raw, ok = profiles[ads[i].SenderPubkey]
		}
		if !ok {
			continue
		}
		applyTopZapProfile(&ads[i], raw)
	}
	return ads
}

func (s *Read) profileJSONByPubkeys(ctx context.Context, pubkeys []string) (map[string]json.RawMessage, error) {
	ids := topZapLookupIDs(pubkeys)
	if len(ids) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT pubkey, COALESCE(profile_json::text, '')
		FROM profiles_latest
		WHERE pubkey = ANY($1::text[])
	`, ids)
	if err != nil {
		return nil, fmt.Errorf("top zap profiles: %w", err)
	}
	defer rows.Close()

	out := make(map[string]json.RawMessage)
	for rows.Next() {
		var pubkey, profileJSON string
		if err := rows.Scan(&pubkey, &profileJSON); err != nil {
			return nil, fmt.Errorf("scan top zap profile: %w", err)
		}
		if strings.TrimSpace(profileJSON) == "" || profileJSON == "null" {
			continue
		}
		raw := json.RawMessage(profileJSON)
		out[pubkey] = raw
		out[strings.ToLower(pubkey)] = raw
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read top zap profiles: %w", err)
	}
	return out, nil
}

func applyTopZapProfile(ad *readmodel.TopZapAd, profileJSON json.RawMessage) {
	if ad == nil || len(profileJSON) == 0 || string(profileJSON) == "null" {
		return
	}
	var profile struct {
		Name        string `json:"name"`
		DisplayName string `json:"display_name"`
		Picture     string `json:"picture"`
	}
	if err := json.Unmarshal(profileJSON, &profile); err != nil {
		return
	}
	ad.Name = cleanZapText(profile.Name, 80)
	ad.DisplayName = cleanZapText(profile.DisplayName, 80)
	ad.Picture = cleanZapPicture(profile.Picture)
}

func cleanZapText(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	// Collapse whitespace first so newlines stay word breaks, then drop
	// leftover control characters and collapse any gap that removal left.
	value = strings.Join(strings.Fields(value), " ")
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	room := maxRunes - 1
	if room < 1 {
		room = 1
	}
	return strings.TrimSpace(string(runes[:room])) + "…"
}

func cleanZapPicture(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || utf8.RuneCountInString(value) > 2048 {
		return ""
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return ""
		}
	}
	lower := strings.ToLower(value)
	if strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "http://") {
		return value
	}
	return ""
}
