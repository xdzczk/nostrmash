package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrendingHashtags(w http.ResponseWriter, r *http.Request) {
	window, windowLabel, err := parseTrendingHashtagWindow(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 50, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	offset, err := parseBoundedNonNegativeInt(r, "offset", 0, 5000)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_hashtags", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		topics, topicsErr := h.service.GetTrendingHashtags(r.Context(), window, limit, offset)
		if topicsErr != nil {
			return nil, topicsErr
		}
		hashtags := buildDiscoveryHashtagItems(topics)
		payloadResponse := map[string]any{
			"surface":     "trending",
			"hashtags":    hashtags,
			"window":      windowLabel,
			"consistency": "eventual",
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payloadResponse, windowLabel, &computedAt, len(hashtags))
		h.addDiscoveryTrustMetadata(payloadResponse)
		return payloadResponse, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending hashtags are not available on this deployment")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetHashtagSummary(w http.ResponseWriter, r *http.Request) {
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hashtag_summary", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		summary, summaryErr := h.service.GetHashtagSummary(r.Context(), rawHashtag)
		if summaryErr != nil {
			return nil, summaryErr
		}
		payload := map[string]any{
			"hashtag":         summary.Hashtag,
			"latest_event_at": summary.LatestEventAt,
			"activity": map[string]any{
				"24h": map[string]any{
					"event_count":    summary.Activity.Last24h.EventCount,
					"unique_authors": summary.Activity.Last24h.UniqueAuthors,
				},
				"7d": map[string]any{
					"event_count":    summary.Activity.Last7d.EventCount,
					"unique_authors": summary.Activity.Last7d.UniqueAuthors,
				},
				"30d": map[string]any{
					"event_count":    summary.Activity.Last30d.EventCount,
					"unique_authors": summary.Activity.Last30d.UniqueAuthors,
				},
				"all": map[string]any{
					"event_count":    summary.Activity.All.EventCount,
					"unique_authors": summary.Activity.All.UniqueAuthors,
				},
			},
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hashtag summary is not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetHashtagNotes(w http.ResponseWriter, r *http.Request) {
	sort, err := parseHashtagNotesSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, err := parseHashtagNotesWindow(r)
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
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "hashtag_notes", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
		"sort":    sort,
		"window":  window,
		"limit":   limit,
		"offset":  offset,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		notes, notesErr := h.service.GetHashtagNotes(r.Context(), rawHashtag, sort, window, limit, offset)
		if notesErr != nil {
			return nil, notesErr
		}
		payloadNotes := make([]map[string]any, 0, len(notes))
		for _, note := range notes {
			payloadNotes = append(payloadNotes, buildTrendingNoteItem(note))
		}
		payload := map[string]any{
			"hashtag":     rawHashtag,
			"sort":        sort,
			"window":      window,
			"notes":       payloadNotes,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "hashtag notes are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}

func (h Handlers) GetRelatedHashtags(w http.ResponseWriter, r *http.Request) {
	limit, err := parseBoundedPositiveInt(r, "limit", 10, 50)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	rawHashtag := r.PathValue("hashtag")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "related_hashtags", map[string]any{
		"hashtag": normalizeCacheHashtag(rawHashtag),
		"limit":   limit,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		related, relatedErr := h.service.GetRelatedHashtags(r.Context(), rawHashtag, limit)
		if relatedErr != nil {
			return nil, relatedErr
		}
		items := make([]map[string]any, 0, len(related))
		for _, row := range related {
			items = append(items, map[string]any{
				"hashtag":               row.Hashtag,
				"co_occurrence_count":   row.CoOccurrenceCount,
				"co_occurrence_authors": row.CoOccurrenceAuthors,
			})
		}
		payload := map[string]any{
			"hashtag":     rawHashtag,
			"related":     items,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "hashtag not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "related hashtags are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidHashtag) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "hashtag is invalid")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
}
