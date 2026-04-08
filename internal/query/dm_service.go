package query

import (
	"context"
	"encoding/json"
)

func (s Service) GetDirectMessages(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type directMessagesReader interface {
		GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(directMessagesReader); ok {
		return r.GetDirectMessages(ctx, pubkey, "", limit)
	}
	return s.reader.GetRecentEventsByKindAndPubkey(ctx, 4, pubkey, limit)
}

func (s Service) GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error) {
	type dmContactsReader interface {
		GetDirectMessageContacts(ctx context.Context, pubkey string, limit int) ([]string, error)
	}
	if r, ok := s.rawReader.(dmContactsReader); ok {
		return r.GetDirectMessageContacts(ctx, pubkey, limit)
	}
	return []string{}, nil
}

func (s Service) GetDirectMessageContactsDetailed(
	ctx context.Context,
	pubkey string,
	limit int,
	offset int,
	since int64,
	until int64,
) ([]json.RawMessage, error) {
	type dmContactsDetailedReader interface {
		GetDirectMessageContactsDetailed(ctx context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(dmContactsDetailedReader); ok {
		return r.GetDirectMessageContactsDetailed(ctx, pubkey, limit, offset, since, until)
	}
	return []json.RawMessage{}, nil
}

func (s Service) GetDirectMessagesByPeer(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error) {
	type dmReader interface {
		GetDirectMessages(ctx context.Context, pubkey string, peer string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(dmReader); ok {
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
	type dmReader interface {
		GetDirectMessagesWithRange(ctx context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(dmReader); ok {
		return r.GetDirectMessagesWithRange(ctx, pubkey, peer, since, until, limit, offset)
	}
	return s.GetDirectMessagesByPeer(ctx, pubkey, peer, limit)
}

func (s Service) GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	type dmUnreadReader interface {
		GetDirectMessageUnreadCounts(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error)
	}
	if r, ok := s.rawReader.(dmUnreadReader); ok {
		return r.GetDirectMessageUnreadCounts(ctx, pubkey, limit)
	}
	return []json.RawMessage{}, nil
}

func (s Service) ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error {
	type dmResetReader interface {
		ResetDirectMessageUnread(ctx context.Context, pubkey string, peer string) error
	}
	if r, ok := s.rawReader.(dmResetReader); ok {
		return r.ResetDirectMessageUnread(ctx, pubkey, peer)
	}
	return nil
}

func (s Service) GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error) {
	type dmCountReader interface {
		GetDirectMessageCount(ctx context.Context, receiver string, sender string) (int64, error)
	}
	if r, ok := s.rawReader.(dmCountReader); ok {
		return r.GetDirectMessageCount(ctx, receiver, sender)
	}
	return 0, nil
}

func (s Service) ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error {
	type dmResetReader interface {
		ResetDirectMessageCount(ctx context.Context, receiver string, sender string) error
	}
	if r, ok := s.rawReader.(dmResetReader); ok {
		return r.ResetDirectMessageCount(ctx, receiver, sender)
	}
	return nil
}

func (s Service) ResetDirectMessageCounts(ctx context.Context, receiver string) error {
	type dmResetReader interface {
		ResetDirectMessageCounts(ctx context.Context, receiver string) error
	}
	if r, ok := s.rawReader.(dmResetReader); ok {
		return r.ResetDirectMessageCounts(ctx, receiver)
	}
	return nil
}
