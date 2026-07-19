package api

import (
	"net/http"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrendingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "trending")
}

func (h Handlers) GetRisingProfiles(w http.ResponseWriter, r *http.Request) {
	h.writeDiscoveryProfiles(w, r, "rising")
}

func (h Handlers) GetRelatedProfiles(w http.ResponseWriter, r *http.Request) {
	pubkey := normalizePathPubkey(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "profiles_related", map[string]any{
		"pubkey": pubkey,
		"limit":  limit,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		related, relatedErr := h.service.GetRelatedProfiles(r.Context(), pubkey, limit)
		if relatedErr != nil {
			return nil, relatedErr
		}
		relatedPubkeys := make([]string, 0, len(related))
		for _, profile := range related {
			relatedPubkeys = append(relatedPubkeys, profile.Pubkey)
		}
		identities, identitiesErr := h.resolveProfileIdentities(r.Context(), relatedPubkeys)
		if identitiesErr != nil {
			return nil, identitiesErr
		}
		items := make([]map[string]any, 0, len(related))
		for _, profile := range related {
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
		payload := map[string]any{
			"pubkey":      pubkey,
			"related":     items,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "profile not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "related profiles are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) writeDiscoveryProfiles(w http.ResponseWriter, r *http.Request, surface string) {
	window, windowLabel, err := parseTrendingWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if surface != "trending" && surface != "rising" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "unsupported discovery surface")
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "profiles_"+surface, map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		var profilesRows []query.TrendingProfile
		var rowsErr error
		switch surface {
		case "rising":
			profilesRows, rowsErr = h.service.GetRisingProfiles(r.Context(), window, limit, offset)
		default:
			profilesRows, rowsErr = h.service.GetTrendingProfiles(r.Context(), window, limit, offset)
		}
		if rowsErr != nil {
			return nil, rowsErr
		}
		pubkeys := make([]string, 0, len(profilesRows))
		for _, profile := range profilesRows {
			pubkeys = append(pubkeys, profile.Pubkey)
		}
		identities, identitiesErr := h.resolveProfileIdentities(r.Context(), pubkeys)
		if identitiesErr != nil {
			return nil, identitiesErr
		}
		profiles := buildDiscoveryProfileItems(profilesRows, identities)
		payload := map[string]any{
			"surface":     surface,
			"window":      windowLabel,
			"profiles":    profiles,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "discovery profiles are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}
