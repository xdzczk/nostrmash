package api

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (h Handlers) GetTrendingDomains(w http.ResponseWriter, r *http.Request) {
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
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "trending_domains", map[string]any{
		"window": windowLabel,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		rows, rowsErr := h.service.GetTrendingDomains(r.Context(), window, limit, offset)
		if rowsErr != nil {
			return nil, rowsErr
		}
		domains := buildDiscoveryDomainItems(rows, windowLabel)
		payload := map[string]any{
			"surface":     "trending",
			"window":      windowLabel,
			"domains":     domains,
			"consistency": "eventual",
		}
		computedAt := time.Now().UTC()
		addDiscoveryListMeta(payload, windowLabel, &computedAt, len(domains))
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "trending domains are not available on this deployment")
			return
		}
		writeInternalError(r.Context(), w, err)
		return
	}
}

func (h Handlers) GetDomainSummary(w http.ResponseWriter, r *http.Request) {
	rawDomain := r.PathValue("domain")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "domain_summary", map[string]any{
		"domain": strings.ToLower(strings.TrimSpace(rawDomain)),
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		summary, summaryErr := h.service.GetDomainSummary(r.Context(), rawDomain)
		if summaryErr != nil {
			return nil, summaryErr
		}
		recentNotes := make([]map[string]any, 0, len(summary.RecentNotes))
		for _, note := range summary.RecentNotes {
			recentNotes = append(recentNotes, buildTrendingNoteItem(note))
		}
		topNotes := make([]map[string]any, 0, len(summary.TopNotes))
		for _, note := range summary.TopNotes {
			topNotes = append(topNotes, buildTrendingNoteItem(note))
		}
		payload := map[string]any{
			"domain":          summary.Domain,
			"latest_event_at": summary.LatestEventAt,
			"activity": map[string]any{
				"24h": map[string]any{
					"link_count":     summary.Activity.Last24h.LinkCount,
					"note_count":     summary.Activity.Last24h.NoteCount,
					"unique_authors": summary.Activity.Last24h.UniqueAuthors,
				},
				"7d": map[string]any{
					"link_count":     summary.Activity.Last7d.LinkCount,
					"note_count":     summary.Activity.Last7d.NoteCount,
					"unique_authors": summary.Activity.Last7d.UniqueAuthors,
				},
				"30d": map[string]any{
					"link_count":     summary.Activity.Last30d.LinkCount,
					"note_count":     summary.Activity.Last30d.NoteCount,
					"unique_authors": summary.Activity.Last30d.UniqueAuthors,
				},
				"all": map[string]any{
					"link_count":     summary.Activity.All.LinkCount,
					"note_count":     summary.Activity.All.NoteCount,
					"unique_authors": summary.Activity.All.UniqueAuthors,
				},
			},
			"notes": map[string]any{
				"recent": recentNotes,
				"top":    topNotes,
			},
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "domain not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "domain summary is not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidDomain) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "domain is invalid")
			return
		}
		writeInternalError(r.Context(), w, err)
		return
	}
}

func (h Handlers) GetDomainNotes(w http.ResponseWriter, r *http.Request) {
	sort, err := parseDomainNotesSort(r)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	window, err := parseDomainNotesWindow(r)
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
	rawDomain := r.PathValue("domain")
	cachePolicy := h.newPublicCachePolicy(publicCacheFamilyDiscovery, "domain_notes", map[string]any{
		"domain": strings.ToLower(strings.TrimSpace(rawDomain)),
		"sort":   sort,
		"window": window,
		"limit":  limit,
		"offset": offset,
	})
	if err := h.servePublicCached(w, cachePolicy, func() (map[string]any, error) {
		notes, notesErr := h.service.GetDomainNotes(r.Context(), rawDomain, sort, window, limit, offset)
		if notesErr != nil {
			return nil, notesErr
		}
		payloadNotes := make([]map[string]any, 0, len(notes))
		for _, note := range notes {
			payloadNotes = append(payloadNotes, buildTrendingNoteItem(note))
		}
		payload := map[string]any{
			"domain":      rawDomain,
			"sort":        sort,
			"window":      window,
			"notes":       payloadNotes,
			"consistency": "eventual",
		}
		h.addDiscoveryTrustMetadata(payload)
		return payload, nil
	}); err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "domain not found")
			return
		}
		if query.IsUnsupportedCapability(err) {
			writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "domain notes are not available on this deployment")
			return
		}
		if errors.Is(err, query.ErrInvalidDomain) {
			writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "domain is invalid")
			return
		}
		writeInternalError(r.Context(), w, err)
		return
	}
}
