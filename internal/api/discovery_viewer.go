package api

import (
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

// parseDiscoveryViewer reads the optional viewer query parameter.
// An empty value keeps the global ranking. A value that is not a hex pubkey
// or npub is a 400. When personalized discovery is disabled, a valid viewer
// is ignored and the handler serves the global ranking.
func (h Handlers) parseDiscoveryViewer(w http.ResponseWriter, r *http.Request) (string, bool) {
	raw := strings.TrimSpace(r.URL.Query().Get("viewer"))
	if raw == "" {
		return "", true
	}
	viewer := query.CanonicalizePubkey(raw)
	if viewer == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "viewer must be a hex pubkey or npub")
		return "", false
	}
	if !h.service.PersonalizedDiscoveryEnabled() {
		return "", true
	}
	return viewer, true
}

func discoveryOffsetCap(viewer string) int {
	if viewer != "" {
		return 500
	}
	return 5000
}

func discoveryScopeWithViewer(lang, viewer string) query.DiscoveryScope {
	return query.DiscoveryScope{Language: lang, Viewer: viewer}
}

func (h Handlers) rankingCachePolicy(endpoint string, params map[string]any, lang, viewer string) publicResponseCachePolicy {
	if params == nil {
		params = map[string]any{}
	}
	family := publicCacheFamilyDiscovery
	if viewer != "" {
		family = publicCacheFamilyPersonalized
		params["viewer"] = viewer
	}
	return h.newPublicCachePolicy(family, endpoint, discoveryCacheParams(params, lang))
}

func attachPersonalization(payload map[string]any, meta query.PersonalizationMeta) {
	if meta.Source == "" {
		return
	}
	payload["personalization"] = map[string]any{
		"viewer":            meta.Viewer,
		"source":            meta.Source,
		"follow_count":      meta.FollowCount,
		"follows_truncated": meta.Truncated,
		"cohort_weight":     meta.CohortWeight,
	}
}

// personalizeRankingItems records why a viewer-scoped row is where it is.
// blendScores replaces the displayed rank score for surfaces whose public
// score stays a raw count. Nil blendScores leaves the score the item builder set.
func personalizeRankingItems(items []map[string]any, meta query.PersonalizationMeta, evidences, blendScores []float64, code, metric, unit string) {
	if meta.Source != query.PersonalizationSourcePersonalized {
		return
	}
	for i := range items {
		ranking, ok := items[i]["ranking"].(query.DiscoveryItemRanking)
		if !ok {
			continue
		}
		if blendScores != nil && i < len(blendScores) {
			ranking.Score = blendScores[i]
		}
		if i < len(evidences) && evidences[i] > 0 {
			ranking.Reasons = append(ranking.Reasons, discoveryReason(code, metric, evidences[i], unit))
		}
		items[i]["ranking"] = ranking
	}
}

func noteCohortEvidence(notes []query.TrendingNote) []float64 {
	out := make([]float64, len(notes))
	for i, note := range notes {
		out[i] = note.CohortEvidence
	}
	return out
}

func profileCohortEvidence(rows []query.TrendingProfile) []float64 {
	out := make([]float64, len(rows))
	for i, row := range rows {
		out[i] = row.CohortEvidence
	}
	return out
}

func hashtagBlend(rows []query.TrendingHashtag) (evidence, scores []float64) {
	evidence = make([]float64, len(rows))
	scores = make([]float64, len(rows))
	for i, row := range rows {
		evidence[i] = row.CohortEvidence
		scores[i] = row.BlendScore
	}
	return evidence, scores
}

func domainBlend(rows []query.DomainSummary) (evidence, scores []float64) {
	evidence = make([]float64, len(rows))
	scores = make([]float64, len(rows))
	for i, row := range rows {
		evidence[i] = row.CohortEvidence
		scores[i] = row.BlendScore
	}
	return evidence, scores
}

func conversationBlend(rows []query.HotConversation) (evidence, scores []float64) {
	evidence = make([]float64, len(rows))
	scores = make([]float64, len(rows))
	for i, row := range rows {
		evidence[i] = row.CohortEvidence
		scores[i] = row.BlendScore
	}
	return evidence, scores
}
