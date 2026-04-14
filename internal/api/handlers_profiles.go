package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

type profileResponse struct {
	Pubkey            string          `json:"pubkey"`
	MetadataEventID   string          `json:"metadata_event_id"`
	MetadataCreatedAt int64           `json:"metadata_created_at"`
	Profile           json.RawMessage `json:"profile"`
}

type profileSummaryStatsResponse struct {
	FollowerCount    int64  `json:"follower_count"`
	FollowingCount   int64  `json:"following_count"`
	NoteCount        int64  `json:"note_count"`
	ReplyCount       int64  `json:"reply_count"`
	RecentActivityAt *int64 `json:"recent_activity_at,omitempty"`
}

type profilePublicSummaryResponse struct {
	Pubkey             string                      `json:"pubkey"`
	MetadataEventID    string                      `json:"metadata_event_id"`
	MetadataCreatedAt  int64                       `json:"metadata_created_at"`
	Profile            json.RawMessage             `json:"profile"`
	Stats              profileSummaryStatsResponse `json:"stats"`
	Hero               profileHeroResponse         `json:"hero"`
	RecentNotes        []json.RawMessage           `json:"recent_notes"`
	RecentNotePreviews []map[string]any            `json:"recent_note_previews"`
	RelatedDiscovery   profileRelatedDiscovery     `json:"related_discovery"`
	IdentityDetails    profileIdentityDetails      `json:"identity_details"`
}

type profileHeroResponse struct {
	Avatar      string                      `json:"avatar,omitempty"`
	DisplayName string                      `json:"display_name,omitempty"`
	Handle      string                      `json:"handle,omitempty"`
	Bio         string                      `json:"bio,omitempty"`
	Counters    profileSummaryStatsResponse `json:"counters"`
	Metadata    profileHeroMetadataStrip    `json:"metadata"`
	Actions     []profileHeroAction         `json:"actions"`
}

type profileHeroMetadataStrip struct {
	NpubOrPubkey profileMetadataValue  `json:"npub_or_pubkey"`
	Website      *profileMetadataValue `json:"website,omitempty"`
	LUD16        *profileMetadataValue `json:"lud16,omitempty"`
}

type profileHeroAction struct {
	ID    string `json:"id"`
	Label string `json:"label"`
	Href  string `json:"href"`
}

type profileRelatedDiscovery struct {
	RelatedProfiles []map[string]any `json:"related_profiles"`
	RisingProfiles  []map[string]any `json:"rising_profiles"`
}

type profileIdentityDetails struct {
	Title  string                 `json:"title"`
	Fields []profileMetadataField `json:"fields"`
}

type profileMetadataField struct {
	Key   string               `json:"key"`
	Label string               `json:"label"`
	Value profileMetadataValue `json:"value"`
}

type profileMetadataValue struct {
	Raw       string `json:"raw"`
	Display   string `json:"display"`
	Copyable  bool   `json:"copyable"`
	Truncated bool   `json:"truncated"`
}

func (h Handlers) GetProfileByPubkey(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	profile, err := h.service.GetProfile(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, profileResponse{
		Pubkey:            profile.Pubkey,
		MetadataEventID:   profile.MetadataEventID,
		MetadataCreatedAt: profile.MetadataCreatedAt,
		Profile:           profile.ProfileJSON,
	})
}

func (h Handlers) GetProfileTopics(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window := strings.TrimSpace(r.URL.Query().Get("window"))
	items, windowDays, err := h.service.GetAuthorTopicStats(r.Context(), pubkey, window, limit)
	if err != nil {
		if strings.Contains(err.Error(), "window must be one of") {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey": pubkey,
		"window": formatWindowDays(windowDays),
		"items":  items,
	})
}

type batchProfilesRequest struct {
	Pubkeys []string `json:"pubkeys"`
}

type batchProfilesResponse struct {
	Profiles       []profileResponse `json:"profiles"`
	MissingPubkeys []string          `json:"missing_pubkeys"`
}

func (h Handlers) BatchGetProfiles(w http.ResponseWriter, r *http.Request) {
	var req batchProfilesRequest
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req, false) {
		return
	}
	if len(req.Pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must not be empty")
		return
	}
	if len(req.Pubkeys) > h.maxBatchSize {
		writeError(
			r.Context(),
			w,
			http.StatusBadRequest,
			"batch_limit_exceeded",
			"requested pubkeys exceed maximum batch size",
		)
		return
	}

	normalizedPubkeys := make([]string, 0, len(req.Pubkeys))
	seen := make(map[string]struct{}, len(req.Pubkeys))
	for _, pubkey := range req.Pubkeys {
		trimmed := strings.TrimSpace(pubkey)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		normalizedPubkeys = append(normalizedPubkeys, trimmed)
	}
	if len(normalizedPubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must include at least one non-empty value")
		return
	}

	profiles, err := h.service.GetProfiles(r.Context(), normalizedPubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	resp := batchProfilesResponse{
		Profiles:       make([]profileResponse, 0, len(profiles.Profiles)),
		MissingPubkeys: append([]string(nil), profiles.MissingPubkeys...),
	}
	for _, profile := range profiles.Profiles {
		resp.Profiles = append(resp.Profiles, profileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	slices.Sort(resp.MissingPubkeys)
	writeJSON(w, http.StatusOK, resp)
}

func (h Handlers) GetProfilePublicSummary(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	summary, err := h.service.GetProfilePublicSummary(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	identity := profileIdentityFieldsFromProfile(summary.Profile)
	recentNotes, recentNotePreviews := h.loadProfileRecentNotes(r, summary.Profile.Pubkey)
	relatedDiscovery := h.loadProfileRelatedDiscovery(r, summary.Profile.Pubkey)
	heroCounters := profileSummaryStatsResponse{
		FollowerCount:    summary.Stats.FollowerCount,
		FollowingCount:   summary.Stats.FollowingCount,
		NoteCount:        summary.Stats.NoteCount,
		ReplyCount:       summary.Stats.ReplyCount,
		RecentActivityAt: summary.Stats.RecentActivityAt,
	}
	identityDetails := profileIdentityDetails{
		Title:  "Identity details",
		Fields: buildIdentityDetailFields(summary.Profile, identity),
	}
	writeJSON(w, http.StatusOK, profilePublicSummaryResponse{
		Pubkey:            summary.Profile.Pubkey,
		MetadataEventID:   summary.Profile.MetadataEventID,
		MetadataCreatedAt: summary.Profile.MetadataCreatedAt,
		Profile:           summary.Profile.ProfileJSON,
		Stats: profileSummaryStatsResponse{
			FollowerCount:    summary.Stats.FollowerCount,
			FollowingCount:   summary.Stats.FollowingCount,
			NoteCount:        summary.Stats.NoteCount,
			ReplyCount:       summary.Stats.ReplyCount,
			RecentActivityAt: summary.Stats.RecentActivityAt,
		},
		Hero: profileHeroResponse{
			Avatar:      identity.Picture,
			DisplayName: chooseProfileDisplayName(identity),
			Handle:      chooseProfileHandle(identity),
			Bio:         truncateBio(identity.About),
			Counters:    heroCounters,
			Metadata: profileHeroMetadataStrip{
				NpubOrPubkey: npubOrPubkeyValue(identity, summary.Profile.Pubkey),
				Website:      optionalMetadataValue(identity.Website, 48, true),
				LUD16:        optionalMetadataValue(identity.LUD16, 48, true),
			},
			Actions: []profileHeroAction{
				{
					ID:    "recent_notes",
					Label: "Recent notes",
					Href:  "/api/v1/authors/" + summary.Profile.Pubkey + "/events?limit=20",
				},
				{
					ID:    "related_profiles",
					Label: "Related profiles",
					Href:  "/api/v1/discovery/profiles/" + summary.Profile.Pubkey + "/related?limit=20",
				},
				{
					ID:    "rising_profiles",
					Label: "Rising profiles",
					Href:  "/api/v1/discovery/profiles/rising?window=7d&limit=20",
				},
			},
		},
		RecentNotes:        recentNotes,
		RecentNotePreviews: recentNotePreviews,
		RelatedDiscovery:   relatedDiscovery,
		IdentityDetails:    identityDetails,
	})
}

func (h Handlers) loadProfileRecentNotes(r *http.Request, pubkey string) ([]json.RawMessage, []map[string]any) {
	notes, err := h.service.GetAuthorEvents(r.Context(), pubkey, 20)
	if err != nil {
		return []json.RawMessage{}, []map[string]any{}
	}
	return notes, buildRawNotePreviewItems(notes)
}

func (h Handlers) loadProfileRelatedDiscovery(r *http.Request, pubkey string) profileRelatedDiscovery {
	out := profileRelatedDiscovery{
		RelatedProfiles: []map[string]any{},
		RisingProfiles:  []map[string]any{},
	}
	related, err := h.service.GetRelatedProfiles(r.Context(), pubkey, 8)
	if err == nil {
		relatedPubkeys := make([]string, 0, len(related))
		for _, profile := range related {
			relatedPubkeys = append(relatedPubkeys, profile.Pubkey)
		}
		identities, identityErr := h.resolveProfileIdentities(r.Context(), relatedPubkeys)
		if identityErr == nil {
			out.RelatedProfiles = buildRelatedProfileItems(related, identities)
		} else {
			out.RelatedProfiles = buildRelatedProfileItems(related, map[string]profileIdentityFields{})
		}
	}
	rising, err := h.service.GetRisingProfiles(r.Context(), 7*24*time.Hour, 8, 0)
	if err == nil {
		risingPubkeys := make([]string, 0, len(rising))
		for _, profile := range rising {
			risingPubkeys = append(risingPubkeys, profile.Pubkey)
		}
		identities, identityErr := h.resolveProfileIdentities(r.Context(), risingPubkeys)
		if identityErr == nil {
			out.RisingProfiles = buildDiscoveryProfileItems(rising, identities)
		} else {
			out.RisingProfiles = buildDiscoveryProfileItems(rising, map[string]profileIdentityFields{})
		}
	}
	return out
}

func buildRelatedProfileItems(rows []query.RelatedProfile, identities map[string]profileIdentityFields) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for _, profile := range rows {
		item := map[string]any{
			"pubkey":                 profile.Pubkey,
			"topic_overlap":          profile.TopicOverlap,
			"reply_adjacency":        profile.ReplyAdjacency,
			"interaction_adjacency":  profile.InteractionAdjacency,
			"quote_repost_adjacency": profile.QuoteRepostAdjacency,
			"reasons":                profile.Reasons,
			"score":                  profile.Score,
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

func chooseProfileDisplayName(identity profileIdentityFields) string {
	if strings.TrimSpace(identity.DisplayName) != "" {
		return identity.DisplayName
	}
	if strings.TrimSpace(identity.Name) != "" {
		return identity.Name
	}
	return ""
}

func chooseProfileHandle(identity profileIdentityFields) string {
	if strings.TrimSpace(identity.NIP05) != "" {
		return identity.NIP05
	}
	if strings.TrimSpace(identity.Name) != "" {
		return identity.Name
	}
	return ""
}

func truncateBio(value string) string {
	bio := strings.TrimSpace(value)
	if bio == "" {
		return ""
	}
	display, _ := truncateForDisplay(bio, 200)
	return display
}

func npubOrPubkeyValue(identity profileIdentityFields, pubkey string) profileMetadataValue {
	if strings.TrimSpace(identity.Npub) != "" {
		display, truncated := truncateForDisplay(identity.Npub, 32)
		return profileMetadataValue{
			Raw:       identity.Npub,
			Display:   display,
			Copyable:  true,
			Truncated: truncated,
		}
	}
	display, truncated := truncateForDisplay(pubkey, 24)
	return profileMetadataValue{
		Raw:       pubkey,
		Display:   display,
		Copyable:  true,
		Truncated: truncated,
	}
}

func optionalMetadataValue(value string, maxDisplay int, copyable bool) *profileMetadataValue {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	display, truncated := truncateForDisplay(trimmed, maxDisplay)
	return &profileMetadataValue{
		Raw:       trimmed,
		Display:   display,
		Copyable:  copyable,
		Truncated: truncated,
	}
}

func buildIdentityDetailFields(profile query.Profile, identity profileIdentityFields) []profileMetadataField {
	fields := []profileMetadataField{
		{
			Key:   "npub_or_pubkey",
			Label: "Npub / Pubkey",
			Value: npubOrPubkeyValue(identity, profile.Pubkey),
		},
	}
	appendIfPresent := func(key, label, value string, maxDisplay int, copyable bool) {
		if metadataValue := optionalMetadataValue(value, maxDisplay, copyable); metadataValue != nil {
			fields = append(fields, profileMetadataField{
				Key:   key,
				Label: label,
				Value: *metadataValue,
			})
		}
	}
	appendIfPresent("display_name", "Display name", identity.DisplayName, 64, false)
	appendIfPresent("name", "Name", identity.Name, 64, false)
	appendIfPresent("nip05", "NIP-05", identity.NIP05, 64, true)
	appendIfPresent("website", "Website", identity.Website, 72, true)
	appendIfPresent("lud16", "LUD-16", identity.LUD16, 64, true)
	appendIfPresent("about", "About", identity.About, 160, true)
	appendIfPresent("metadata_event_id", "Metadata event id", profile.MetadataEventID, 32, true)
	if profile.MetadataCreatedAt > 0 {
		appendIfPresent("metadata_created_at", "Metadata created at", strconvFormatInt(profile.MetadataCreatedAt), 32, false)
	}
	return fields
}

func truncateForDisplay(value string, maxLen int) (string, bool) {
	value = strings.TrimSpace(value)
	if maxLen <= 0 || len(value) <= maxLen {
		return value, false
	}
	if maxLen <= 7 {
		return value[:maxLen], true
	}
	head := (maxLen - 1) / 2
	tail := maxLen - head - 1
	return value[:head] + "..." + value[len(value)-tail:], true
}

func strconvFormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}
