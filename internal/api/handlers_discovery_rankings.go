package api

import (
	"context"
	"net/http"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetBookmarkedNotes(w http.ResponseWriter, r *http.Request) {
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, limit, offset, ok := h.parseRankListPage(w, r, viewer)
	if !ok {
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	h.writeRankedNotes(w, r, "bookmarked", "bookmarked_notes", "all", lang, viewer, limit, offset, func(ctx context.Context) ([]query.TrendingNote, query.PersonalizationMeta, error) {
		if viewer == "" {
			notes, err := h.service.GetBookmarkedNotes(ctx, scope, limit, offset)
			return notes, query.PersonalizationMeta{}, err
		}
		return h.service.GetPersonalizedBookmarkedNotes(ctx, scope, limit, offset)
	}, func(notes []query.TrendingNote) []map[string]any {
		return buildBookmarkedNoteItems(notes)
	}, "followed_savers", "cohort_savers", "people")
}

func (h Handlers) GetMostZappedNotes(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseRankingWindow(r, "7d")
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	lang, limit, offset, ok := h.parseRankListPage(w, r, viewer)
	if !ok {
		return
	}
	scope := discoveryScopeWithViewer(lang, viewer)
	h.writeRankedNotes(w, r, "zapped", "zapped_notes_"+windowLabel, windowLabel, lang, viewer, limit, offset, func(ctx context.Context) ([]query.TrendingNote, query.PersonalizationMeta, error) {
		if viewer == "" {
			notes, err := h.service.GetMostZappedNotes(ctx, window, scope, limit, offset)
			return notes, query.PersonalizationMeta{}, err
		}
		return h.service.GetPersonalizedZappedNotes(ctx, window, scope, limit, offset)
	}, func(notes []query.TrendingNote) []map[string]any {
		return buildZappedNoteItems(notes)
	}, "followed_zappers", "cohort_sats", "sats")
}

func (h Handlers) GetMostFollowedProfiles(w http.ResponseWriter, r *http.Request) {
	if _, err := parseOptionalDiscoveryLanguage(r); err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	limit, offset, ok := parseRankLimits(w, r, viewer)
	if !ok {
		return
	}
	scope := discoveryScopeWithViewer("", viewer)
	h.writeRankedProfiles(w, r, "followed", "followed_profiles", "all", "", viewer, limit, offset, nil, func(ctx context.Context) ([]query.TrendingProfile, query.PersonalizationMeta, error) {
		if viewer == "" {
			rows, err := h.service.GetMostFollowedProfiles(ctx, limit, offset)
			return rows, query.PersonalizationMeta{}, err
		}
		return h.service.GetPersonalizedFollowedProfiles(ctx, scope, limit, offset)
	}, buildFollowedProfileItems, "followed_by", "cohort_follows", "accounts")
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
	viewer, ok := h.parseDiscoveryViewer(w, r)
	if !ok {
		return
	}
	limit, offset, ok := parseRankLimits(w, r, viewer)
	if !ok {
		return
	}
	scope := discoveryScopeWithViewer("", viewer)
	h.writeRankedProfiles(w, r, "supported", "supported_profiles_"+windowLabel, windowLabel, "", viewer, limit, offset, map[string]any{
		"sender_cap_sats": derivation.SupportSenderCapSats,
	}, func(ctx context.Context) ([]query.TrendingProfile, query.PersonalizationMeta, error) {
		if viewer == "" {
			rows, err := h.service.GetMostSupportedProfiles(ctx, window, limit, offset)
			return rows, query.PersonalizationMeta{}, err
		}
		return h.service.GetPersonalizedSupportedProfiles(ctx, window, scope, limit, offset)
	}, buildSupportedProfileItems, "followed_supporters", "cohort_sats", "sats")
}

func (h Handlers) parseRankListPage(w http.ResponseWriter, r *http.Request, viewer string) (string, int, int, bool) {
	lang, err := parseOptionalDiscoveryLanguage(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return "", 0, 0, false
	}
	limit, offset, ok := parseRankLimits(w, r, viewer)
	if !ok {
		return "", 0, 0, false
	}
	return lang, limit, offset, true
}

func parseRankLimits(w http.ResponseWriter, r *http.Request, viewer string) (int, int, bool) {
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return 0, 0, false
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, discoveryOffsetCap(viewer))
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return 0, 0, false
	}
	return limit, offset, true
}

func (h Handlers) writeRankedNotes(
	w http.ResponseWriter,
	r *http.Request,
	surface, cacheName, windowLabel, lang, viewer string,
	limit, offset int,
	fetch func(context.Context) ([]query.TrendingNote, query.PersonalizationMeta, error),
	build func([]query.TrendingNote) []map[string]any,
	reasonCode, reasonMetric, reasonUnit string,
) {
	cachePolicy := h.rankingCachePolicy(cacheName, map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		notes, meta, notesErr := fetch(ctx)
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
		attachPersonalization(payloadResponse, meta)
		personalizeRankingItems(payload, meta, noteCohortEvidence(notes), nil, reasonCode, reasonMetric, reasonUnit)
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
	surface, cacheName, windowLabel, lang, viewer string,
	limit, offset int,
	extra map[string]any,
	fetch func(context.Context) ([]query.TrendingProfile, query.PersonalizationMeta, error),
	build func([]query.TrendingProfile, map[string]profileIdentityFields) []map[string]any,
	reasonCode, reasonMetric, reasonUnit string,
) {
	cachePolicy := h.rankingCachePolicy(cacheName, map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	}, lang, viewer)
	if err := h.servePublicCached(r.Context(), w, cachePolicy, func(ctx context.Context) (map[string]any, error) {
		rows, meta, rowsErr := fetch(ctx)
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
		attachPersonalization(payload, meta)
		personalizeRankingItems(profiles, meta, profileCohortEvidence(rows), nil, reasonCode, reasonMetric, reasonUnit)
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
		ranking := stockRanking(index+1, float64(note.BookmarkCount), "saved_by", "bookmark_count", "people", note.BookmarkCount)
		ranking.Score = note.Score
		item["ranking"] = ranking
		items = append(items, item)
	}
	return items
}

func buildZappedNoteItems(notes []query.TrendingNote) []map[string]any {
	items := make([]map[string]any, 0, len(notes))
	for index, note := range notes {
		item := buildTrendingNoteItem(note)
		ranking := stockRanking(index+1, float64(note.ZapMSats)/1000, "zap_volume", "zap_sats", "sats", note.ZapCount)
		ranking.Score = note.Score
		item["ranking"] = ranking
		items = append(items, item)
	}
	return items
}

func buildFollowedProfileItems(rows []query.TrendingProfile, identities map[string]profileIdentityFields) []map[string]any {
	items := make([]map[string]any, 0, len(rows))
	for index, profile := range rows {
		item := baseRankedProfileItem(profile)
		trusted := profile.TrustedFollowerCount
		item["score"] = profile.Score
		item["trusted_follower_count"] = trusted
		if profile.ViewerFollows {
			item["viewer_follows"] = true
		}
		ranking := stockRanking(index+1, float64(trusted), "audience_size", "follower_count", "followers", trusted)
		ranking.Score = profile.Score
		item["ranking"] = ranking
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
		ranking := stockRanking(index+1, float64(profile.CappedSats), "supporter_sats", "capped_sats", "sats", profile.SupporterCount)
		ranking.Score = profile.Score
		item["ranking"] = ranking
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
