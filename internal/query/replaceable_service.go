package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

func (s Service) GetBookmarks(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	if r := s.capabilities.replaceable.event; r != nil {
		latest, err := r.GetParameterizedReplaceableEvent(ctx, pubkey, 10003, "")
		if err == nil {
			return []json.RawMessage{latest}, nil
		}
		if !errors.Is(err, readmodel.ErrNotFound) && !IsUnsupportedCapability(err) {
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
	if r := s.capabilities.replaceable.list; r != nil {
		return r.GetParameterizedReplaceableList(ctx, pubkey, 30023, limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 30023, pubkey, limit)
}

func (s Service) GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.replaceable.list; r != nil {
		return r.GetParameterizedReplaceableList(ctx, pubkey, kind, limit)
	}
	return nil, unsupportedCapabilityError("parameterized replaceable list")
}

func (s Service) GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.replaceable.listByIdentifier; r != nil {
		return r.GetParameterizedReplaceableListByIdentifier(ctx, pubkey, kind, identifier, limit)
	}
	return nil, unsupportedCapabilityError("parameterized replaceable list by identifier")
}

func (s Service) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	if r := s.capabilities.replaceable.event; r != nil {
		return r.GetParameterizedReplaceableEvent(ctx, pubkey, kind, dTag)
	}
	return nil, unsupportedCapabilityError("parameterized replaceable event")
}

func (s Service) GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.replaceable.events; r != nil {
		return r.GetParameterizedReplaceableEvents(ctx, kind, dTag, limit)
	}
	return nil, unsupportedCapabilityError("parameterized replaceable events")
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
	if r := s.capabilities.replaceable.longFormATagReplies; r != nil {
		target := fmt.Sprintf("%d:%s:%s", kind, pubkey, identifier)
		return r.GetEventsByATagAndKind(ctx, 1, target, limit)
	}
	return nil, unsupportedCapabilityError("long form replies by a-tag")
}
