package store

import (
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// mergeEventCountsIntoRaw attaches eventual engagement counters to a raw event
// payload so list UIs (profile activity, author feeds) can render note cards
// without a second /events/{id}/counts round-trip.
func mergeEventCountsIntoRaw(raw json.RawMessage, counts EventCounts) (json.RawMessage, error) {
	return readmodel.MergeEventCountsIntoRaw(raw, counts)
}
