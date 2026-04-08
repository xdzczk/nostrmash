package query

import (
	"context"
	"encoding/json"
)

type isUserFollowingCapability interface {
	IsUserFollowing(ctx context.Context, followerPubkey string, followedPubkey string) (bool, error)
}

type mutualFollowsCapability interface {
	GetMutualFollows(ctx context.Context, leftPubkey string, rightPubkey string, limit int) ([]string, error)
}

func adaptSocialCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(isUserFollowingCapability); ok {
		caps.social.userFollowing = r
	}
	if r, ok := reader.(mutualFollowsCapability); ok {
		caps.social.mutualFollows = r
	}
}

type userZapsCapability interface {
	GetUserZaps(ctx context.Context, pubkey string, limit int, sortBySats bool) ([]json.RawMessage, error)
}

type highlightsByEventIDCapability interface {
	GetHighlightsByEventID(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
}

type highlightsByATargetCapability interface {
	GetHighlightsByATarget(ctx context.Context, kind int, pubkey string, identifier string, limit int) ([]json.RawMessage, error)
}

type eventZapsBySatsCapability interface {
	GetEventZapsBySats(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error)
}

func adaptEventCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(userZapsCapability); ok {
		caps.event.userZaps = r
	}
	if r, ok := reader.(highlightsByEventIDCapability); ok {
		caps.event.highlightsByEventID = r
	}
	if r, ok := reader.(highlightsByATargetCapability); ok {
		caps.event.highlightsByATarget = r
	}
	if r, ok := reader.(eventZapsBySatsCapability); ok {
		caps.event.eventZapsBySats = r
	}
}
