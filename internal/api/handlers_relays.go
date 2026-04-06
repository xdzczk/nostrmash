package api

import (
	"net/http"
	"time"
)

type relayHealthEntry struct {
	RelayURL           string     `json:"relay_url"`
	Mode               string     `json:"mode"`
	FilterGroup        string     `json:"filter_group"`
	Status             string     `json:"status"`
	LatestCheckpointAt time.Time  `json:"latest_checkpoint_at"`
	EOSESeenAt         *time.Time `json:"eose_seen_at,omitempty"`
}

// GetRelaysHealth returns an aggregate view of relay ingest state.
func (h Handlers) GetRelaysHealth(w http.ResponseWriter, r *http.Request) {
	rows, err := h.store.ListRelayHealth(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	relays := make([]relayHealthEntry, 0, len(rows))
	for _, row := range rows {
		relays = append(relays, relayHealthEntry{
			RelayURL:           row.RelayURL,
			Mode:               row.Mode,
			FilterGroup:        row.FilterGroup,
			Status:             row.Status,
			LatestCheckpointAt: row.UpdatedAt.UTC(),
			EOSESeenAt:         row.EOSESeenAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"relays":      relays,
		"consistency": "eventual",
	})
}
