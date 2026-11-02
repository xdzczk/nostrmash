package api

import (
	"context"
	"net/http"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetBookmarkedNotes(w http.ResponseWriter, r *http.Request) {
	lang, limit, offset, ok := h.parseRankListPage(w, r)
	if !ok {
		return
	}
	h.writeRankedNotes(w, r, "bookmarked", "bookmarked_notes", "all", lang, limit, offset, func(ctx context.Context) ([]query.TrendingNote, error) {
		return h.service.GetBookmarkedNotes(ctx, discoveryScope(lang), limit, offset)
	}, func(notes []query.TrendingNote) []map[string]any {
		return buildBookmarkedNoteItems(notes)
	})
}

func (h Handlers) GetMostZappedNotes(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseRankingWindow(r, "7d")
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	lang, limit, offset, ok := h.parseRankListPage(w, r)
	if !ok {
		return
	}
	h.writeRankedNotes(w, r, "zapped", "zapped_notes_"+windowLabel, windowLabel, lang, limit, offset, func(ctx context.Context) ([]query.TrendingNote, error) {
		return h.service.GetMostZappedNotes(ctx, window, discoveryScope(lang), limit, offset)
	}, func(notes []query.TrendingNote) []map[string]any {
		return buildZappedNoteItems(notes)
	})
}

func (h Handlers) GetMostFollowedProfiles(w http.ResponseWriter, r *http.Request) {
	if _, err := parseOptionalDiscoveryLanguage(r); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, offset, ok := parseRankLimits(w, r)
	if !ok {
		return
	}
	h.writeRankedProfiles(w, r, "followed", "followed_profiles", "all", "", limit, offset, nil, func(ctx context.Context) ([]query.TrendingProfile, error) {
		return h.service.GetMostFollowedProfiles(ctx, limit, offset)
	}, buildFollowedProfileItems)
}

func (h Handlers) GetMostSupportedProfiles(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseRankingWindow(r, "30d")
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if _, err := parseOptionalDiscoveryLanguage(r); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, offset, ok := parseRankLimits(w, r)
	if !ok {
		return
	}
	h.writeRankedProfiles(w, r, "supported", "supported_profiles_"+windowLabel, windowLabel, "", limit, offset, map[string]any{
		"sender_cap_sats": derivation.SupportSenderCapSats,
	}, func(ctx context.Context) ([]query.TrendingProfile, error) {
		return h.service.GetMostSupportedProfiles(ctx, window, limit, offset)
	}, buildSupportedProfileItems)
}

func (h Handlers) parseRankListPage(w http.ResponseWriter, r *http.Request) (string, int, int, bool) {
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return "", 0, 0, false
	}
	limit, offset, ok := parseRankLimits(w, r)
	if !ok {
		return "", 0, 0, false
	}
	return lang, limit, offset, true
}

func parseRankLimits(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return 0, 0, false
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return 0, 0, false
	}
	return limit, offset, true
}

func (h Handlers) writeRankedNotes(
	w http.ResponseWriter,
	r *http.Request,
	surface, cacheName, windowLabel, lang string,
	limit, offset int,
	fetch func(context.Context) ([]query.TrendingNote, error),
	build func([]query.TrendingNote) []map[string]any,
) {
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, cacheName, discoveryCacheParams(map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang))
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		notes, notesErr := fetch(ctx)
		degraded := false
		if notesErr != nil {
			if query.IsUnsupportedCapability(notesErr) {
				return nil, notesErr
			}
			recordDiscoveryDegrade(ctx, cacheName, "backend", notesErr, nil)
			notes = nil
			degraded = true
		}
		pubkeys := make([]string, 0, len(notes))
		for _, note := range notes {
			pubkeys = append(pubkeys, note.AuthorPubkey)
		}
		identities, identitiesErr := h.resolveProfileIdentities(ctx, pubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(ctx, cacheName, "profile_identities", identitiesErr, nil)
			identities = map[string]profileIdentityFields{}
			degraded = true
		}
		payload := build(notes)
		for index := range payload {
			if identity, ok := identities[notes[index].AuthorPubkey]; ok {
				payload[index]["author"] = applyProfileIdentity(map[string]any{}, identity)
			}
		}
		h.attachTopZaps(ctx, payload)
		payloadResponse := map[string]any{
			"surface":     surface,
			"window":      windowLabel,
			"notes":       payload,
			"consistency": "eventual",
		}
		if degraded {
			payloadResponse["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(payload))
		h.addDiscoveryTrustMetadata(payloadResponse)
		h.attachDiscoveryLanguage(ctx, payloadResponse, lang)
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", surface+" notes are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) writeRankedProfiles(
	w http.ResponseWriter,
	r *http.Request,
	surface, cacheName, windowLabel, lang string,
	limit, offset int,
	extra map[string]any,
	fetch func(context.Context) ([]query.TrendingProfile, error),
	build func([]query.TrendingProfile, map[string]profileIdentityFields) []map[string]any,
) {
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, cacheName, discoveryCacheParams(map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang))
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		rows, rowsErr := fetch(ctx)
		degraded := false
		if rowsErr != nil {
			if query.IsUnsupportedCapability(rowsErr) {
				return nil, rowsErr
			}
			recordDiscoveryDegrade(ctx, cacheName, "backend", rowsErr, nil)
			rows = nil
			degraded = true
		}
		pubkeys := make([]string, 0, len(rows))
		for _, profile := range rows {
			pubkeys = append(pubkeys, profile.Pubkey)
		}
		identities, identitiesErr := h.resolveProfileIdentities(ctx, pubkeys)
		if identitiesErr != nil {
			recordDiscoveryDegrade(ctx, cacheName, "profile_identities", identitiesErr, nil)
			identities = map[string]profileIdentityFields{}
			degraded = true
		}
		profiles := build(rows, identities)
		payload := map[string]any{
			"surface":     surface,
			"window":      windowLabel,
			"profiles":    profiles,
			"consistency": "eventual",
		}
		for key, value := range extra {
			payload[key] = value
		}
		if degraded {
			payload["degraded"] = true
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payload, windowLabel, &computedAt, len(profiles))
		h.addDiscoveryTrustMetadata(payload)
		h.attachDiscoveryLanguage(ctx, payload, lang)
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", surface+" profiles are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func stockRanking(rank int, score float64, code, metric, unit string, sample int64) query.DiscoveryItemRanking {
	return query.DiscoveryItemRanking{
		Rank:       rank,
		Score:      score,
		Reasons:    []query.DiscoveryReason{discoveryReason(code, metric, score, unit)},
		Confidence: discoveryConfidence(sample),
	}
}

func buildBookmarkedNoteItems(notes []query.TrendingNote) []map[string]any {
	items := make([]map[string]any, 0, len(notes))
	for index, note := range notes {
		item := buildTrendingNoteItem(note)
		item["bookmark_count"] = note.BookmarkCount
		item["ranking"] = stockRanking(index+1, float64(note.BookmarkCount), "saved_by", "bookmark_count", "people", note.BookmarkCount)
		items = append(items, item)
	}
	return items
}

func buildZappedNoteItems(notes []query.TrendingNote) []map[string]any {
	items := make([]map[string]any, 0, len(notes))
	for index, note := range notes {
		item := buildTrendingNoteItem(note)
		item["ranking"] = stockRanking(index+1, note.Score, "zap_volume", "zap_sats", "sats", note.ZapCount)
		items = append(items, item)
	}
	return items
}

func buildFollowedProfileItems(rows []query.TrendingProfile, identities map[string]profileIdentityFields) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, profile := range rows {
		item := baseRankedProfileItem(profile)
		item["ranking"] = stockRanking(index+1, float64(profile.FollowerCount), "audience_size", "follower_count", "followers", profile.FollowerCount)
		if identity, ok := identities[profile.Pubkey]; ok {
			applyProfileIdentity(item, identity)
		}
		items = append(items, item)
	}
	return items
}

func buildSupportedProfileItems(rows []query.TrendingProfile, identities map[string]profileIdentityFields) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, profile := range rows {
		item := baseRankedProfileItem(profile)
		item["capped_sats"] = profile.CappedSats
		item["sats_received"] = profile.RawSats
		item["supporter_count"] = profile.SupporterCount
		item["recent_zap_volume_msats"] = profile.RawSats * 1000
		item["ranking"] = stockRanking(index+1, float64(profile.CappedSats), "supporter_sats", "capped_sats", "sats", profile.SupporterCount)
		if identity, ok := identities[profile.Pubkey]; ok {
			applyProfileIdentity(item, identity)
		}
		items = append(items, item)
	}
	return items
}

func baseRankedProfileItem(profile query.TrendingProfile) map[string]any {
	item := map[string]any{
		"pubkey":         profile.Pubkey,
		"score":          profile.Score,
		"follower_count": profile.FollowerCount,
	}
	if npub := encodeNpub(profile.Pubkey); npub != "" {
		item["npub"] = npub
	}
	if profile.RecentActivityAt != nil {
		item["recent_activity_at"] = profile.RecentActivityAt
	}
	return item
}
