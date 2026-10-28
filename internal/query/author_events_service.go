package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/nostr"
	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s Service) GetAuthorEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	events, err := s.reader.GetAuthorRecentEvents(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	return s.maybeAugmentAuthorEventsFromRelays(ctx, pubkey, thinProfileFallbackKinds, normalizeAuthorEventsLimit(limit), events), nil
}

func (s Service) GetAuthorEventsByKind(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if kind < 0 {
		return nil, fmt.Errorf("kind must be >= 0")
	}
	limit = normalizeAuthorEventsLimit(limit)
	var events []json.RawMessage
	var err error
	if r := s.capabilities.event.authorRecentEventsByKind; r != nil {
		events, err = r.GetAuthorRecentEventsByKind(ctx, pubkey, kind, limit)
	} else {
		events, err = s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
	}
	if err != nil {
		return nil, err
	}
	events = s.maybeAugmentAuthorEventsFromRelays(ctx, pubkey, []int{kind}, limit, events)
	if kind == 1 {
		events = omitAuthorReplyEvents(events)
	}
	return events, nil
}

// GetAuthorEventsPage returns one keyset page of an author's recent events.
// A nil kind returns all projected kinds; a nil cursor returns the first
// page. The thin-profile relay fallback only augments the first page: relay
// results are merged into the response while the next cursor still tracks
// the local projection, and newly discovered events are persisted so later
// pages include them.
func (s Service) GetAuthorEventsPage(
	ctx context.Context,
	pubkey string,
	kind *int,
	limit int,
	cursor *EventCursor,
) (AuthorEventsResult, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return AuthorEventsResult{}, fmt.Errorf("pubkey is required")
	}
	if kind != nil && *kind < 0 {
		return AuthorEventsResult{}, fmt.Errorf("kind must be >= 0")
	}
	limit = normalizeAuthorEventsLimit(limit)

	var events []json.RawMessage
	var nextCursor *EventCursor
	var err error
	if r := s.capabilities.event.authorRecentEventsPage; r != nil {
		var storeNext *readmodel.EventOrderCursor
		if kind != nil {
			events, storeNext, err = r.GetAuthorRecentEventsByKindPage(ctx, pubkey, *kind, limit, eventCursorToStore(cursor))
		} else {
			events, storeNext, err = r.GetAuthorRecentEventsPage(ctx, pubkey, limit, eventCursorToStore(cursor))
		}
		nextCursor = eventCursorFromStore(storeNext)
	} else {
		// Legacy readers have no keyset support: serve the first page and
		// never issue a continuation cursor.
		if kind != nil {
			events, err = s.GetAuthorEventsByKind(ctx, pubkey, *kind, limit)
		} else {
			events, err = s.GetAuthorEvents(ctx, pubkey, limit)
		}
		return AuthorEventsResult{
			Pubkey:      pubkey,
			Events:      events,
			Consistency: "eventual",
		}, err
	}
	if err != nil {
		return AuthorEventsResult{}, err
	}
	if cursor == nil {
		fallbackKinds := thinProfileFallbackKinds
		if kind != nil {
			fallbackKinds = []int{*kind}
		}
		events = s.maybeAugmentAuthorEventsFromRelays(ctx, pubkey, fallbackKinds, limit, events)
	}
	if kind != nil && *kind == 1 {
		events = omitAuthorReplyEvents(events)
	}
	return AuthorEventsResult{
		Pubkey:      pubkey,
		Events:      events,
		NextCursor:  nextCursor,
		Consistency: "eventual",
	}, nil
}

func keepAuthorReplyEvents(events []json.RawMessage) []json.RawMessage {
	if len(events) == 0 {
		return events
	}
	kept := make([]json.RawMessage, 0, len(events))
	for _, raw := range events {
		if authorEventIsReply(raw) {
			kept = append(kept, raw)
		}
	}
	return kept
}

// omitAuthorReplyEvents drops kind-1 replies that the store query did not
// already remove, including events merged in from the thin-profile relay
// fallback. A reply is whatever ReplyParentEventID classifies as one: the
// same rule thread_edges uses.
func omitAuthorReplyEvents(events []json.RawMessage) []json.RawMessage {
	if len(events) == 0 {
		return events
	}
	kept := make([]json.RawMessage, 0, len(events))
	for _, raw := range events {
		if authorEventIsReply(raw) {
			continue
		}
		kept = append(kept, raw)
	}
	return kept
}

func authorEventIsReply(raw json.RawMessage) bool {
	var payload struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return false
	}
	return nostr.ReplyParentEventID(payload.Tags) != ""
}

func normalizeAuthorEventsLimit(limit int) int {
	if limit <= 0 {
		return 20
	}
	if limit > 100 {
		return 100
	}
	return limit
}

func (s Service) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	events, err := s.reader.GetAuthorReplies(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	events = s.maybeAugmentAuthorEventsFromRelays(ctx, pubkey, []int{1}, normalizeAuthorEventsLimit(limit), events)
	return keepAuthorReplyEvents(events), nil
}

func (s Service) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, kind, pubkey, limit)
}

func (s Service) GetMentions(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetEventsReferencingPubkey(ctx, pubkey, limit)
}

func (s Service) GetFollowers(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetFollowersByPubkey(ctx, pubkey, limit)
}
