package query

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type eventActionsPageCapability interface {
	ListEventActions(ctx context.Context, eventID string, kind int, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

type authorRepliesPageCapability interface {
	GetAuthorRepliesPage(ctx context.Context, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

type notificationsPageCapability interface {
	ListEngagementNotifications(ctx context.Context, pubkey string, limit int, cursor *readmodel.NotificationCursor) ([]readmodel.EngagementNotification, *readmodel.NotificationCursor, error)
}

type followersPageCapability interface {
	GetFollowersByPubkeyPage(ctx context.Context, targetPubkey string, limit int, cursor *readmodel.FollowerCursor) ([]json.RawMessage, *readmodel.FollowerCursor, error)
}

type mutualFollowsPageCapability interface {
	GetMutualFollowsPage(ctx context.Context, leftPubkey string, rightPubkey string, limit int, afterPubkey string) ([]string, error)
}

type moderationListTagsCapability interface {
	GetModerationListTags(ctx context.Context, pubkey string, kind int) ([]readmodel.ModerationTag, json.RawMessage, error)
}

type recentEventsByKindPageCapability interface {
	GetRecentEventsByKindAndPubkeyPage(ctx context.Context, kind int, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
}

func adaptNativePageCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(eventActionsPageCapability); ok {
		caps.event.actionsPage = r
	}
	if r, ok := reader.(authorRepliesPageCapability); ok {
		caps.event.authorRepliesPage = r
	}
	if r, ok := reader.(notificationsPageCapability); ok {
		caps.event.notificationsPage = r
	}
	if r, ok := reader.(followersPageCapability); ok {
		caps.social.followersPage = r
	}
	if r, ok := reader.(mutualFollowsPageCapability); ok {
		caps.social.mutualFollowsPage = r
	}
	if r, ok := reader.(moderationListTagsCapability); ok {
		caps.moderation.listTags = r
	}
	if r, ok := reader.(recentEventsByKindPageCapability); ok {
		caps.event.recentEventsByKindPage = r
	}
}
