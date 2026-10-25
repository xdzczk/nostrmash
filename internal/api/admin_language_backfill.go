package api

import (
	"context"
	"fmt"
	"net/http"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

func (s *adminService) GetLanguageBackfill(ctx context.Context) (derivation.LanguageBackfillStatus, bool, error) {
	if s.derivation == nil {
		return derivation.LanguageBackfillStatus{}, false, fmt.Errorf("derivation handlers are not configured")
	}
	return s.derivation.LanguageBackfillStatus(ctx)
}

func (s *adminService) StartLanguageBackfill(ctx context.Context) (derivation.LanguageBackfillStatus, error) {
	if s.derivation == nil {
		return derivation.LanguageBackfillStatus{}, fmt.Errorf("derivation handlers are not configured")
	}
	return s.derivation.StartLanguageBackfill(ctx)
}

func (h AdminHandlers) GetLanguageBackfill(w http.ResponseWriter, r *http.Request) {
	status, ok, err := h.service.GetLanguageBackfill(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"phase": "", "status": "idle"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h AdminHandlers) StartLanguageBackfill(w http.ResponseWriter, r *http.Request) {
	status, err := h.service.StartLanguageBackfill(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, status)
}
