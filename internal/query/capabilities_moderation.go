package query

import "context"

type moderationListByKindCapability interface {
	GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
}

type moderationListByIdentifierCapability interface {
	GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
}

type hiddenByContentModerationCapability interface {
	IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error)
}

func adaptModerationCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(moderationListByKindCapability); ok {
		caps.moderation.listByKind = r
	}
	if r, ok := reader.(moderationListByIdentifierCapability); ok {
		caps.moderation.listByIdentifier = r
	}
	if r, ok := reader.(hiddenByContentModerationCapability); ok {
		caps.moderation.hiddenByContent = r
	}
}
