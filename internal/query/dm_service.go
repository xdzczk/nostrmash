package query

import (
	"context"
	"encoding/json"
)

func (s Service) GetDirectMessages(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.dm.directMessages; r != nil {
		return r.GetDirectMessages(ctx, pubkey, "", limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 4, pubkey, limit)
}

func (s Service) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
	if r := s.capabilities.dm.contacts; r != nil {
		return r.GetDirectMessageContacts(ctx, pubkey, limit)
	}
	return nil, unsupportedCapabilityError("direct message contacts")
}

func (s Service) GetDirectMessageContactsDetailed(
	ctx context.Context,
	pubkey string,
	limit int,
	offset int,
	since int64,
	until int64,
) ([]json.RawMessage, error) {
	if r := s.capabilities.dm.contactsDetailed; r != nil {
		return r.GetDirectMessageContactsDetailed(ctx, pubkey, limit, offset, since, until)
	}
	return nil, unsupportedCapabilityError("direct message contacts detailed")
}

func (s Service) GetDirectMessagesByPeer(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.dm.directMessages; r != nil {
		return r.GetDirectMessages(ctx, pubkey, peer, limit)
	}
	return s.GetDirectMessages(ctx, pubkey, limit)
}

func (s Service) GetDirectMessagesWithRange(
	ctx context.Context,
	pubkey string,
	peer string,
	since int64,
	until int64,
	limit int,
	offset int,
) ([]json.RawMessage, error) {
	if r := s.capabilities.dm.withRange; r != nil {
		return r.GetDirectMessagesWithRange(ctx, pubkey, peer, since, until, limit, offset)
	}
	return s.GetDirectMessagesByPeer(ctx, pubkey, peer, limit)
}

func (s Service) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if r := s.capabilities.dm.unreadCounts; r != nil {
		return r.GetDirectMessageUnreadCounts(ctx, pubkey, limit)
	}
	return nil, unsupportedCapabilityError("direct message unread counts")
}

func (s Service) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
	if r := s.capabilities.dm.unreadReset; r != nil {
		return r.ResetDirectMessageUnread(ctx, pubkey, peer)
	}
	return unsupportedCapabilityError("direct message unread reset")
}

func (s Service) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
	if r := s.capabilities.dm.count; r != nil {
		return r.GetDirectMessageCount(ctx, receiver, sender)
	}
	return 0, unsupportedCapabilityError("direct message count")
}

func (s Service) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
	if r := s.capabilities.dm.directMessageCountOps; r != nil {
		return r.ResetDirectMessageCount(ctx, receiver, sender)
	}
	return unsupportedCapabilityError("direct message count reset")
}

func (s Service) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
	if r := s.capabilities.dm.directMessageCountOps; r != nil {
		return r.ResetDirectMessageCounts(ctx, receiver)
	}
	return unsupportedCapabilityError("direct message counts reset")
}
