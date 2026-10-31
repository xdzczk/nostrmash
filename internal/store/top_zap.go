package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

// TopZapAdsByEventIDs delegates to the read store. Declaring it here keeps a
// nil embedded *Read from panicking through method promotion.
func (s *PostgresStore) TopZapAdsByEventIDs(ctx context.Context, ids []string) (map[string]readmodel.TopZapAd, error) {
	if s == nil || s.Read == nil {
		return nil, fmt.Errorf("store is not initialized")
	}
	return s.Read.TopZapAdsByEventIDs(ctx, ids)
}

// AttachTopZaps delegates to the read store. A nil embedded *Read leaves the
// events unchanged instead of panicking through method promotion.
func (s *PostgresStore) AttachTopZaps(ctx context.Context, events []json.RawMessage) []json.RawMessage {
	if s == nil || s.Read == nil || len(events) == 0 {
		return events
	}
	return s.Read.AttachTopZaps(ctx, events)
}
