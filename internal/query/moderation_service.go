package query

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/xdzczk/nostrmash/internal/store"
)

func (s Service) GetMuteList(ctx context.Context, pubkey string) ([]string, error) {
	if r := s.capabilities.moderation.listByKind; r != nil {
		values, err := r.GetModerationList(ctx, pubkey, 10000)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return nil, unsupportedCapabilityError("moderation list lookup")
}

// GetMutedBy returns authors who mute the target pubkey (their latest kind:10000
// mute list includes the target as a p-tag).
func (s Service) GetMutedBy(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if r := s.capabilities.moderation.mutedBy; r != nil {
		values, err := r.GetMutedBy(ctx, pubkey, limit)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []json.RawMessage{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return nil, unsupportedCapabilityError("muted-by lookup")
}

func (s Service) GetAllowList(ctx context.Context, pubkey string) ([]string, error) {
	if r := s.capabilities.moderation.listByKind; r != nil {
		values, err := r.GetModerationList(ctx, pubkey, 10001)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return nil, unsupportedCapabilityError("moderation list lookup")
}

func (s Service) GetMuteLists(ctx context.Context, pubkey string) ([]string, error) {
	if r := s.capabilities.moderation.listByIdentifier; r != nil {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "mutelists")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return nil, unsupportedCapabilityError("moderation list by identifier lookup")
}

func (s Service) GetIdentifierAllowList(ctx context.Context, pubkey string) ([]string, error) {
	if r := s.capabilities.moderation.listByIdentifier; r != nil {
		values, err := r.GetModerationListByIdentifier(ctx, pubkey, "allowlist")
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return []string{}, nil
			}
			return nil, err
		}
		return values, nil
	}
	return nil, unsupportedCapabilityError("moderation list by identifier lookup")
}

func (s Service) IsHiddenByContentModeration(ctx context.Context, viewerPubkey string, eventID string) (bool, string, error) {
	if r := s.capabilities.moderation.hiddenByContent; r != nil {
		return r.IsHiddenByContentModeration(ctx, viewerPubkey, eventID)
	}
	return false, "", unsupportedCapabilityError("content moderation visibility lookup")
}
