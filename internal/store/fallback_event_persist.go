package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
)

// PersistFallbackEvent saves a relay-fetched raw event into the canonical
// events table so subsequent lookups find it locally. The event is validated
// minimally (must have an id matching eventID) and inserted idempotently.
func (s *PostgresStore) PersistFallbackEvent(ctx context.Context, eventID string, raw json.RawMessage) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("store is not initialized")
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" || len(raw) == 0 {
		return nil
	}

	var envelope struct {
		ID        string `json:"id"`
		Pubkey    string `json:"pubkey"`
		CreatedAt int64  `json:"created_at"`
		Kind      int    `json:"kind"`
		Content   string `json:"content"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return fmt.Errorf("unmarshal fallback event: %w", err)
	}
	if strings.TrimSpace(envelope.ID) != eventID {
		return fmt.Errorf("event id mismatch: envelope %q vs expected %q", envelope.ID, eventID)
	}
	if envelope.Pubkey == "" {
		return nil
	}

	evt := model.Event{
		ID:          eventID,
		Pubkey:      envelope.Pubkey,
		CreatedAt:   envelope.CreatedAt,
		Kind:        envelope.Kind,
		Content:     envelope.Content,
		RawJSON:     raw,
		FirstSeenAt: time.Now().UTC(),
	}
	if err := s.InsertCanonicalEvent(ctx, evt, nil, "fallback:relay", evt.FirstSeenAt); err != nil {
		if isIdempotentConflict(err) {
			return nil
		}
		return fmt.Errorf("persist fallback event: %w", err)
	}
	return nil
}

func isIdempotentConflict(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "duplicate key") ||
		strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "unique constraint")
}
