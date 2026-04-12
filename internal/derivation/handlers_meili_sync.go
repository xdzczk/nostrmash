package derivation

import (
	"context"
	"fmt"
	"strings"
)

// SyncMeilisearch keeps Meilisearch indexes warm after event derivations.
// Sync failures are intentionally non-fatal so projection jobs remain durable
// when Meilisearch is unavailable.
func (h *Handlers) SyncMeilisearch(ctx context.Context, eventID string) error {
	if h == nil || h.pool == nil || h.meili == nil || !h.meili.Enabled() {
		return nil
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}
	if err := h.meili.SyncEvent(ctx, h.pool, eventID); err != nil {
		// Best-effort behavior: do not fail the derivation bundle if sync is down.
		return nil
	}
	return nil
}
