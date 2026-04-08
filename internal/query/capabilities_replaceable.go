package query

import (
	"context"
	"encoding/json"
)

type parameterizedReplaceableEventCapability interface {
	GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error)
}

type parameterizedReplaceableListCapability interface {
	GetParameterizedReplaceableList(ctx context.Context, pubkey string, kind int, limit int) ([]json.RawMessage, error)
}

type parameterizedReplaceableListByIdentifierCapability interface {
	GetParameterizedReplaceableListByIdentifier(ctx context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error)
}

type parameterizedReplaceableEventsCapability interface {
	GetParameterizedReplaceableEvents(ctx context.Context, kind int, dTag string, limit int) ([]json.RawMessage, error)
}

type eventsByATagAndKindCapability interface {
	GetEventsByATagAndKind(ctx context.Context, kind int, aTagValue string, limit int) ([]json.RawMessage, error)
}

func adaptReplaceableCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(parameterizedReplaceableEventCapability); ok {
		caps.replaceable.event = r
	}
	if r, ok := reader.(parameterizedReplaceableListCapability); ok {
		caps.replaceable.list = r
	}
	if r, ok := reader.(parameterizedReplaceableListByIdentifierCapability); ok {
		caps.replaceable.listByIdentifier = r
	}
	if r, ok := reader.(parameterizedReplaceableEventsCapability); ok {
		caps.replaceable.events = r
	}
	if r, ok := reader.(eventsByATagAndKindCapability); ok {
		caps.replaceable.longFormATagReplies = r
	}
}
