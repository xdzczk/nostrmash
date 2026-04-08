package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (s Service) GetBookmarks(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	type replaceableEventReader interface {
		GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableEventReader); ok {
		latest, err := r.GetParameterizedReplaceableEvent(ctx, pubkey, 10003, "")
		if err == nil {
			return []json.RawMessage{latest}, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return nil, err
		}
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 10003, pubkey, limit)
}

func (s Service) GetLongForm(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableList(ctx, pubkey, 30023, limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 30023, pubkey, limit)
}

func (s Service) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableList(ctx, pubkey, kind, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableListByIdentifier(ctx, pubkey, kind, identifier, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableEvent(ctx, pubkey, kind, dTag)
	}
	return nil, store.ErrNotFound
}

func (s Service) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
	type replaceableReader interface {
		GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(replaceableReader); ok {
		return r.GetParameterizedReplaceableEvents(ctx, kind, dTag, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetLongFormThreadView(
	ctx context.Context,
	pubkey string,
	kind int,
	identifier string,
	limit int,
	maxDepth int,
) (ThreadView, error) {
	event, err := s.GetParameterizedReplaceableEvent(ctx, pubkey, kind, identifier)
	if err != nil {
		return ThreadView{}, err
	}
	var payload struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(event, &payload); err != nil {
		return ThreadView{}, err
	}
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" {
		return ThreadView{}, fmt.Errorf("long form event id is missing")
	}
	return s.GetThreadView(ctx, payload.ID, limit, maxDepth, nil)
}

func (s Service) GetLongFormThreadATagReplies(
	ctx context.Context,
	kind int,
	pubkey string,
	identifier string,
	limit int,
) ([]json.RawMessage, error) {
	type longFormATagRepliesReader interface {
		GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error)
	}
	if kind <= 0 {
		return []json.RawMessage{}, nil
	}
	pubkey = strings.TrimSpace(pubkey)
	identifier = strings.TrimSpace(identifier)
	if pubkey == "" || identifier == "" {
		return []json.RawMessage{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 5000 {
		limit = 5000
	}
	if r, ok := s.rawReader.(longFormATagRepliesReader); ok {
		target := fmt.Sprintf("%d:%s:%s", kind, pubkey, identifier)
		return r.GetEventsByATagAndKind(ctx, 1, target, limit)
	}
	return []json.RawMessage{}, nil
}
