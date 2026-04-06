package query

import (
	"context"
	"errors"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (s Service) GetMuteList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationList(ctx, pubkey, 10000)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetAllowList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationList(ctx context.Context, pubkey string, kind int) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationList(ctx, pubkey, 10001)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetMuteLists(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "mutelists")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) GetIdentifierAllowList(ctx context.Context, pubkey string) ([]string, error) {
	type listReader interface {
		GetModerationListByIdentifier(ctx context.Context, pubkey string, identifier string) ([]string, error)
	}
	if r, ok := s.reader.(listReader); ok {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "allowlist")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return []string{}, nil
}

func (s Service) IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error) {
	type moderationReader interface {
		IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error)
	}
	if r, ok := s.reader.(moderationReader); ok {
		return r.IsHiddenByContentModeration(ctx, viewerPubkey, eventID)
	}
	return false, "", nil
}
