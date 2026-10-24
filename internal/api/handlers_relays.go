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
	Stale              bool       `json:"stale"`
	LastError          *string    `json:"last_error,omitempty"`
	LatestCheckpointAt time.Time  `json:"latest_checkpoint_at"`
	EOSESeenAt         *time.Time `json:"eose_seen_at,omitempty"`
}

// relayHealthStaleAfter is how long a checkpoint may go without an update
// before the health response reports it as stale. Live checkpoints are
// written on events and connection-state changes, not on a timer, so this
// is much longer than the 5s flush interval: a quiet relay stays current,
// and a row abandoned for a day does not keep reading as healthy.
const relayHealthStaleAfter = 24 * time.Hour

// GetRelaysHealth returns an aggregate view of relay ingest state.
// Each row is one ingest checkpoint (relay URL, mode, and filter group),
// not one hostname. mode is the subscription kind (live or backfill), not
// a liveness flag. status stays the stored value; stale is true when the
// checkpoint has not been updated within relayHealthStaleAfter.
func (h Handlers) GetRelaysHealth(w http.ResponseWriter, r *http.Request) {
	rows, err := h.service.GetRelaysHealth(r.Context())
	if err != nil {
		writeError(r.Context(), w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	now := time.Now().UTC()
	relays := make([]relayHealthEntry, 0, len(rows))
	for _, row := range rows {
		relays = append(relays, relayHealthEntry{
			RelayURL:           row.RelayURL,
			Mode:               row.Mode,
			FilterGroup:        row.FilterGroup,
			Status:             row.Status,
			Stale:              relayCheckpointStale(row.UpdatedAt, now),
			LastError:          row.LastError,
			LatestCheckpointAt: row.UpdatedAt.UTC(),
			EOSESeenAt:         row.EOSESeenAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"relays":      relays,
		"total":       len(relays),
		"scope":       "ingest_checkpoints",
		"consistency": "eventual",
	})
}

func relayCheckpointStale(updatedAt, now time.Time) bool {
	if updatedAt.IsZero() {
		return true
	}
	return now.Sub(updatedAt.UTC()) > relayHealthStaleAfter
}
