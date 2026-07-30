package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/xdzczk/nostrmash/internal/query"
)

type profileIdentityFields struct {
	Pubkey      string
	Npub        string
	Name        string
	DisplayName string
	Picture     string
	About       string
	NIP05       string
	LUD16       string
	Website     string
}

type profileIdentityJSON struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Picture     string `json:"picture"`
	About       string `json:"about"`
	NIP05       string `json:"nip05"`
	LUD16       string `json:"lud16"`
	Website     string `json:"website"`
}

func (h Handlers) resolveProfileIdentities(
	ctx context.Context,
	pubkeys []string,
) (map[string]profileIdentityFields, error) {
	normalized := queryNormalizeUniqueStrings(pubkeys)
	if len(normalized) == 0 {
		return map[string]profileIdentityFields{}, nil
	}
	resolved, err := h.service.GetProfiles(ctx, normalized)
	if err != nil {
		return nil, err
	}
	out := make(map[string]profileIdentityFields, len(resolved.Profiles))
	for _, profile := range resolved.Profiles {
		out[profile.Pubkey] = profileIdentityFieldsFromProfile(profile)
	}
	return out, nil
}

func profileIdentityFieldsFromProfile(profile query.Profile) profileIdentityFields {
	fields := profileIdentityFields{
		Pubkey: profile.Pubkey,
		Npub:   encodeNpub(profile.Pubkey),
	}
	if len(profile.ProfileJSON) == 0 {
		return fields
	}
	var payload profileIdentityJSON
	if err := json.Unmarshal(profile.ProfileJSON, &payload); err != nil {
		return fields
	}
	fields.Name = payload.Name
	fields.DisplayName = payload.DisplayName
	fields.Picture = payload.Picture
	fields.About = payload.About
	fields.NIP05 = payload.NIP05
	fields.LUD16 = payload.LUD16
	fields.Website = payload.Website
	return fields
}

func applyProfileIdentity(item map[string]any, identity profileIdentityFields) map[string]any {
	if identity.Pubkey != "" {
		item["pubkey"] = identity.Pubkey
	}
	if identity.Npub != "" {
		item["npub"] = identity.Npub
	}
	if identity.Name != "" {
		item["name"] = identity.Name
	}
	if identity.DisplayName != "" {
		item["display_name"] = identity.DisplayName
	}
	if identity.Picture != "" {
		item["picture"] = identity.Picture
	}
	if identity.About != "" {
		item["about"] = identity.About
	}
	if identity.NIP05 != "" {
		item["nip05"] = identity.NIP05
	}
	if identity.LUD16 != "" {
		item["lud16"] = identity.LUD16
	}
	if identity.Website != "" {
		item["website"] = identity.Website
	}
	return item
}

func buildDiscoveryProfileItems(
	rows []query.TrendingProfile,
	identities map[string]profileIdentityFields,
) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, profile := range rows {
		item := map[string]any{
			"pubkey":                     profile.Pubkey,
			"score":                      profile.Score,
			"recent_post_count":          profile.RecentPostCount,
			"recent_reply_count":         profile.RecentReplyCount,
			"recent_engagement_received": profile.RecentEngagementReceived,
			"recent_new_followers":       profile.RecentNewFollowers,
			"recent_zap_volume_msats":    profile.RecentZapVolumeMSats,
			"recent_active_days":         profile.RecentActiveDays,
			"recent_activity_at":         profile.RecentActivityAt,
			"ranking":                    buildProfileRanking(profile, index+1),
		}
		if npub := encodeNpub(profile.Pubkey); npub != "" {
			item["npub"] = npub
		}
		if identity, ok := identities[profile.Pubkey]; ok {
			applyProfileIdentity(item, identity)
		}
		items = append(items, item)
	}
	return items
}

func buildDiscoveryNoteItems(
	rows []query.TrendingNote,
	identities map[string]profileIdentityFields,
) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, note := range rows {
		item := buildTrendingNoteItem(note)
		item["ranking"] = buildNoteRanking(note, index+1)
		if identity, ok := identities[note.AuthorPubkey]; ok {
			item["author"] = applyProfileIdentity(map[string]any{}, identity)
		}
		items = append(items, item)
	}
	return items
}

func buildTrendingNoteItem(note query.TrendingNote) map[string]any {
	item := map[string]any{
		"id":             note.EventID,
		"event_id":       note.EventID,
		"pubkey":         note.AuthorPubkey,
		"author_pubkey":  note.AuthorPubkey,
		"created_at":     note.CreatedAt,
		"content":        note.Content,
		"language":       note.Language,
		"reply_count":    note.ReplyCount,
		"repost_count":   note.RepostCount,
		"reaction_count": note.ReactionCount,
		"zap_count":      note.ZapCount,
		"zap_msats":      note.ZapMSats,
		"score":          note.Score,
	}
	item["preview"] = buildNotePreviewPayload(note.EventID, note.Content)
	return item
}

func encodeNpub(pubkey string) string {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return ""
	}
	data, err := hex.DecodeString(pubkey)
	if err != nil {
		return ""
	}
	converted, err := bech32.ConvertBits(data, 8, 5, true)
	if err != nil {
		return ""
	}
	encoded, err := bech32.Encode("npub", converted)
	if err != nil {
		return ""
	}
	return encoded
}

func queryNormalizeUniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
