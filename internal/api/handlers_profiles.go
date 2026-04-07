package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

type profileResponse struct {
	Pubkey            string          `json:"pubkey"`
	MetadataEventID   string          `json:"metadata_event_id"`
	MetadataCreatedAt int64           `json:"metadata_created_at"`
	Profile           json.RawMessage `json:"profile"`
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
