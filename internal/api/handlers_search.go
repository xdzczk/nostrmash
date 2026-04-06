package api

import (
	"net/http"
	"strings"
)

// Search returns a best-effort combined event/profile search.
func (h Handlers) Search(w http.ResponseWriter, r *http.Request) {
	queryText := strings.TrimSpace(r.URL.Query().Get("q"))
	if queryText == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "q is required")
		return
	}
	limit, err := parseBoundedPositiveInt(r, "limit", 20, 100)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	events, err := h.store.SearchEventsByContent(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	profiles, err := h.store.SearchProfiles(r.Context(), queryText, limit)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}

	projectedProfiles := make([]profileResponse, 0, len(profiles))
	for _, profile := range profiles {
		projectedProfiles = append(projectedProfiles, profileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"query":       queryText,
		"events":      events,
		"profiles":    projectedProfiles,
		"consistency": "eventual",
	})
}
