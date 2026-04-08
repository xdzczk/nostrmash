package api_primal

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

type primalProfileResponse struct {
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
	writeJSON(w, http.StatusOK, primalProfileResponse{
		Pubkey:            profile.Pubkey,
		MetadataEventID:   profile.MetadataEventID,
		MetadataCreatedAt: profile.MetadataCreatedAt,
		Profile:           profile.ProfileJSON,
	})
}

type primalUserInfosRequest struct {
	Pubkeys []string `json:"pubkeys"`
}

type primalUserInfosResponse struct {
	Profiles       []primalProfileResponse `json:"profiles"`
	MissingPubkeys []string                `json:"missing_pubkeys"`
}

func (h Handlers) BatchGetUserInfos(w http.ResponseWriter, r *http.Request) {
	var req primalUserInfosRequest
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &req) {
		return
	}
	if len(req.Pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must not be empty")
		return
	}
	if len(req.Pubkeys) > h.maxBatchSize {
		writeError(r.Context(), w, http.StatusBadRequest, "batch_limit_exceeded", "requested pubkeys exceed maximum batch size")
		return
	}
	pubkeys := normalizeUniqueValues(req.Pubkeys)
	if len(pubkeys) == 0 {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkeys must include at least one non-empty value")
		return
	}
	profiles, err := h.service.GetProfiles(r.Context(), pubkeys)
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	resp := primalUserInfosResponse{
		Profiles:       make([]primalProfileResponse, 0, len(profiles.Profiles)),
		MissingPubkeys: append([]string(nil), profiles.MissingPubkeys...),
	}
	for _, profile := range profiles.Profiles {
		resp.Profiles = append(resp.Profiles, primalProfileResponse{
			Pubkey:            profile.Pubkey,
			MetadataEventID:   profile.MetadataEventID,
			MetadataCreatedAt: profile.MetadataCreatedAt,
			Profile:           profile.ProfileJSON,
		})
	}
	slices.Sort(resp.MissingPubkeys)
	writeJSON(w, http.StatusOK, resp)
}

func (h Handlers) GetContactList(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	entry, err := h.service.GetContactList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "contact list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       entry.Pubkey,
		"event_id":     entry.EventID,
		"created_at":   entry.CreatedAt,
		"contacts":     entry.ContactsJSONRaw,
		"consistency":  "eventual",
		"projection_v": entry.DerivationVer,
	})
}

func (h Handlers) GetRelayList(w http.ResponseWriter, r *http.Request) {
	pubkey := strings.TrimSpace(r.PathValue("pubkey"))
	if pubkey == "" {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey is required")
		return
	}
	entry, err := h.service.GetRelayList(r.Context(), pubkey)
	if err != nil {
		if query.IsNotFound(err) {
			writeError(r.Context(), w, http.StatusNotFound, "not_found", "relay list not found")
			return
		}
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pubkey":       entry.Pubkey,
		"event_id":     entry.EventID,
		"created_at":   entry.CreatedAt,
		"relays":       entry.RelaysJSONRaw,
		"consistency":  "eventual",
		"projection_v": entry.DerivationVer,
	})
}
