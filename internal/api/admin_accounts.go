package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/xdzczk/nostrmash/internal/account"
	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/store"
)

type adminAccountStateResponse struct {
	Pubkey    string `json:"pubkey"`
	FromState string `json:"from_state"`
	State     string `json:"state"`
	Override  string `json:"override,omitempty"`
}

type adminSetAccountStateRequest struct {
	// Override is the manual override state. Empty string clears the override
	// and lets the derived state take over.
	Override string `json:"override"`
	Reason   string `json:"reason"`
}

// SetAccountState applies (or clears) a manual account-state override.
func (s *adminService) SetAccountState(ctx context.Context, pubkey, stateOverride, reason string) (adminAccountStateResponse, error) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return adminAccountStateResponse{}, fmt.Errorf("pubkey is required")
	}
	stateOverride = strings.ToLower(strings.TrimSpace(stateOverride))
	if stateOverride != "" {
		if _, ok := account.Parse(stateOverride); !ok {
			return adminAccountStateResponse{}, fmt.Errorf("invalid state %q", stateOverride)
		}
	}
	pg := store.NewPostgresStore(s.pool)
	from, err := pg.SetAccountManualOverride(ctx, pubkey, stateOverride, reason)
	if err != nil {
		return adminAccountStateResponse{}, err
	}
	row, err := pg.GetAccountState(ctx, pubkey)
	if err != nil {
		return adminAccountStateResponse{}, err
	}
	if row.State != from {
		metrics.IncAccountStateTransition(row.State)
	}
	resp := adminAccountStateResponse{
		Pubkey:    pubkey,
		FromState: from,
		State:     row.State,
	}
	if row.ManualOverride != nil {
		resp.Override = *row.ManualOverride
	}
	return resp, nil
}

// EnqueueAccountHydration enqueues an authenticated on-demand hydration job.
func (s *adminService) EnqueueAccountHydration(ctx context.Context, pubkey, reason string) (accountHydrateResponse, error) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if pubkey == "" {
		return accountHydrateResponse{}, fmt.Errorf("pubkey is required")
	}
	if !s.hydration.Enabled {
		return accountHydrateResponse{Pubkey: pubkey, Status: "disabled"}, nil
	}
	if reason == "" {
		reason = "admin"
	}
	status, err := enqueueHydration(ctx, s.pool, s.hydration, pubkey, reason, "admin")
	if err != nil {
		return accountHydrateResponse{}, err
	}
	return accountHydrateResponse{Pubkey: pubkey, Status: status}, nil
}

// SetAccountState handles POST /admin/v1/accounts/{pubkey}/state.
func (h AdminHandlers) SetAccountState(w http.ResponseWriter, r *http.Request) {
	pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
	if !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey must be 64 hex characters")
		return
	}
	var req adminSetAccountStateRequest
	if !decodeJSONBodyLimited(w, r, adminBodyLimitBytes, &req, true) {
		return
	}
	resp, err := h.service.SetAccountState(r.Context(), pubkey, req.Override, req.Reason)
	if err != nil {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// HydrateAccount handles POST /admin/v1/accounts/{pubkey}/hydrate (authenticated).
func (h AdminHandlers) HydrateAccount(w http.ResponseWriter, r *http.Request) {
	pubkey, ok := normalizePubkeyParam(r.PathValue("pubkey"))
	if !ok {
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", "pubkey must be 64 hex characters")
		return
	}
	resp, err := h.service.EnqueueAccountHydration(r.Context(), pubkey, "admin")
	if err != nil {
		writeInternalError(r.Context(), w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, resp)
}
