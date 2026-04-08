package query

import (
	"context"
	"encoding/json"
)

type directMessagesCapability interface {
	GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
}

type dmContactsCapability interface {
	GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error)
}

type dmContactsDetailedCapability interface {
	GetDirectMessageContactsDetailed(ctx context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error)
}

type directMessagesWithRangeCapability interface {
	GetDirectMessagesWithRange(ctx context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error)
}

type dmUnreadCountsCapability interface {
	GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
}

type dmUnreadResetCapability interface {
	ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error
}

type dmCountCapability interface {
	GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error)
}

type dmCountResetCapability interface {
	ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error
	ResetDirectMessageCounts(ctx context.Context, receiver string) error
}

func adaptDMCapabilities(reader any, caps *serviceCapabilities) {
	if r, ok := reader.(directMessagesCapability); ok {
		caps.dm.directMessages = r
	}
	if r, ok := reader.(dmContactsCapability); ok {
		caps.dm.contacts = r
	}
	if r, ok := reader.(dmContactsDetailedCapability); ok {
		caps.dm.contactsDetailed = r
	}
	if r, ok := reader.(directMessagesWithRangeCapability); ok {
		caps.dm.withRange = r
	}
	if r, ok := reader.(dmUnreadCountsCapability); ok {
		caps.dm.unreadCounts = r
	}
	if r, ok := reader.(dmUnreadResetCapability); ok {
		caps.dm.unreadReset = r
	}
	if r, ok := reader.(dmCountCapability); ok {
		caps.dm.count = r
	}
	if r, ok := reader.(dmCountResetCapability); ok {
		caps.dm.directMessageCountOps = r
	}
}
