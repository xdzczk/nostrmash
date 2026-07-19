package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

func (s Service) GetZaps(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, false)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
}

func (s Service) GetHighlights(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 9802, pubkey, limit)
}

func (s Service) GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByEventID; r != nil {
		return r.GetHighlightsByEventID(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("highlights by event id")
}

func (s Service) GetHighlightsByATarget(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	if r := s.capabilities.event.highlightsByATarget; r != nil {
		return r.GetHighlightsByATarget(ctx, kind, pubkey, identifier, limit)
	}
	return nil, unsupportedCapabilityError("highlights by a-target")
}

func (s Service) GetUserZapsBySats(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.userZaps; r != nil {
		return r.GetUserZaps(ctx, pubkey, limit, true)
	}
	return s.GetZaps(ctx, pubkey, limit)
}

func (s Service) GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.event.eventZapsBySats; r != nil {
		return r.GetEventZapsBySats(ctx, eventID, limit)
	}
	return nil, unsupportedCapabilityError("event zaps by sats")
}

func (s Service) GetAuthorSentZaps(ctx context.Context, pubkey string, limit int, cursor *EventCursor) (AuthorZapsResult, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorZapsResult{}, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	if r := s.capabilities.event.authorSentZaps; r != nil {
		zaps, nextCursor, err := r.GetAuthorSentZaps(ctx, pubkey, limit, eventCursorToStore(cursor))
		if err != nil {
			return AuthorZapsResult{}, err
		}
		if zaps == nil {
			zaps = []json.RawMessage{}
		}
		return AuthorZapsResult{
			Pubkey:      pubkey,
			Zaps:        zaps,
			NextCursor:  eventCursorFromStore(nextCursor),
			Consistency: "eventual",
		}, nil
	}

	events, err := s.reader.GetRecentEventsByKindAndPubkey(ctx, 9735, pubkey, limit)
	if err != nil {
		return AuthorZapsResult{}, err
	}
	zaps := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		zaps = append(zaps, buildFallbackAuthorZapRecord(pubkey, event))
	}
	return AuthorZapsResult{
		Pubkey:      pubkey,
		Zaps:        zaps,
		NextCursor:  nil,
		Consistency: "eventual",
	}, nil
}

func (s Service) GetAuthorReactions(ctx context.Context, pubkey string, limit int, cursor *EventCursor) (AuthorReactionsResult, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorReactionsResult{}, fmt.Errorf("pubkey is required")
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	if r := s.capabilities.event.authorReactions; r != nil {
		reactions, nextCursor, err := r.GetAuthorReactions(ctx, pubkey, limit, eventCursorToStore(cursor))
		if err != nil {
			return AuthorReactionsResult{}, err
		}
		if reactions == nil {
			reactions = []json.RawMessage{}
		}
		return AuthorReactionsResult{
			Pubkey:      pubkey,
			Reactions:   reactions,
			NextCursor:  eventCursorFromStore(nextCursor),
			Consistency: "eventual",
		}, nil
	}
	return AuthorReactionsResult{}, unsupportedCapabilityError("author reactions")
}

func buildFallbackAuthorZapRecord(senderPubkey string, event json.RawMessage) json.RawMessage {
	var payload struct {
		ID        string     `json:"id"`
		CreatedAt int64      `json:"created_at"`
		Tags      [][]string `json:"tags"`
	}
	if err := json.Unmarshal(event, &payload); err != nil {
		return event
	}
	receiver := firstTagValue(payload.Tags, "p")
	targetEventID := firstTagValue(payload.Tags, "e")
	msats := parseZapAmountMillisatsFromTag(firstTagValue(payload.Tags, "amount"))
	_, zapText := extractZapRequestDetailsFromEvent(event)
	record := map[string]any{
		"event_id":        payload.ID,
		"sender_pubkey":   senderPubkey,
		"receiver_pubkey": receiver,
		"target_event_id": targetEventID,
		"created_at":      payload.CreatedAt,
		"event":           event,
	}
	if msats > 0 {
		record["msats"] = msats
		record["sats"] = msats / 1000
	}
	if zapText != "" {
		record["zap_text"] = zapText
	}
	out, err := json.Marshal(record)
	if err != nil {
		return event
	}
	return out
}

func firstTagValue(tags [][]string, tagName string) string {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != tagName {
			continue
		}
		value := strings.TrimSpace(tag[1])
		if value != "" {
			return value
		}
	}
	return ""
}

func parseZapAmountMillisatsFromTag(raw string) int64 {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	amount, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || amount <= 0 {
		return 0
	}
	if amount >= 1000 {
		return amount
	}
	return amount * 1000
}

func extractZapRequestDetailsFromEvent(event json.RawMessage) (int64, string) {
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(event, &payload); err != nil {
		return 0, ""
	}
	msats := parseZapAmountMillisatsFromTag(firstTagValue(payload.Tags, "amount"))
	zapText := ""
	for _, tag := range payload.Tags {
		if len(tag) < 2 || tag[0] != "description" {
			continue
		}
		var req struct {
			Content string     `json:"content"`
			Tags    [][]string `json:"tags"`
		}
		if err := json.Unmarshal([]byte(tag[1]), &req); err != nil {
			continue
		}
		zapText = strings.TrimSpace(req.Content)
		if amount := parseZapAmountMillisatsFromTag(firstTagValue(req.Tags, "amount")); amount > 0 {
			msats = amount
		}
	}
	return msats, zapText
}
