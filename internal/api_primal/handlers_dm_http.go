package api_primal

import (
	"net/http"
)

type primalDMEventsResponse struct {
	Events []any `json:"events"`
}

func (h Handlers) dmGateway() WSGateway {
	return WSGateway{query: h.service}
}

func decodePrimalDMRequest(w http.ResponseWriter, r *http.Request) (map[string]any, bool) {
	var kwargs map[string]any
	if !decodeJSONBodyLimited(w, r, publicBatchBodyLimitBytes, &kwargs) {
		return nil, false
	}
	if kwargs == nil {
		kwargs = map[string]any{}
	}
	return kwargs, true
}

func setPrimalDMNoStoreHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
}

func writePrimalDMCompatError(r *http.Request, w http.ResponseWriter, err error) {
	switch err.Error() {
	case "invalid pubkey",
		"invalid relation",
		"event_from_user is required",
		"event_from_user is malformed",
		"verification failed",
		"event is too old",
		"event from the future":
		writeError(r.Context(), w, http.StatusBadRequest, "invalid_request", err.Error())
	case "feature unavailable":
		writeError(r.Context(), w, http.StatusNotImplemented, "feature_unavailable", "feature is not available on this deployment")
	default:
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func (h Handlers) PostDirectMessages(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchDirectMessages(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}

func (h Handlers) PostDirectMessageContacts(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchDirectMessageContacts(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}

func (h Handlers) PostDirectMessageCount(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchDirectMessageCount(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}

func (h Handlers) PostDirectMessageCount2(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchDirectMessageCount2(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}

func (h Handlers) PostResetDirectMessageCount(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchResetDirectMessageCount(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}

func (h Handlers) PostResetDirectMessageCounts(w http.ResponseWriter, r *http.Request) {
	setPrimalDMNoStoreHeaders(w)
	kwargs, ok := decodePrimalDMRequest(w, r)
	if !ok {
		return
	}
	events, err := h.dmGateway().cacheDispatchResetDirectMessageCounts(r.Context(), kwargs)
	if err != nil {
		writePrimalDMCompatError(r, w, err)
		return
	}
	writeJSON(w, http.StatusOK, primalDMEventsResponse{Events: events})
}
