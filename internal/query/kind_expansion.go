package query

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type PinItem = readmodel.PinItem
type ReportTarget = readmodel.ReportTarget

func (s Service) kindExpansion() kindExpansionCapability {
	return s.capabilities.event.kindExpansion
}

func (s Service) GetAuthorMedia(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("author media")
	}
	items, err := reader.GetAuthorMedia(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []json.RawMessage{}, nil
	}
	return items, nil
}

func (s Service) GetAuthorLive(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("author live")
	}
	items, err := reader.GetAuthorLive(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []json.RawMessage{}, nil
	}
	return items, nil
}

func (s Service) GetAuthorPins(ctx context.Context, pubkey string) ([]PinItem, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("author pins")
	}
	items, err := reader.GetAuthorPins(ctx, pubkey)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []PinItem{}, nil
	}
	return items, nil
}

func (s Service) GetLiveActivities(ctx context.Context, status, followedBy string, limit int) ([]json.RawMessage, error) {
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("live activities")
	}
	items, err := reader.GetLiveActivities(ctx, status, followedBy, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []json.RawMessage{}, nil
	}
	return items, nil
}

func (s Service) GetShortVideos(ctx context.Context, followedBy string, limit int) ([]json.RawMessage, error) {
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("short videos")
	}
	items, err := reader.GetShortVideos(ctx, followedBy, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []json.RawMessage{}, nil
	}
	return items, nil
}

func (s Service) GetEventComments(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("event comments")
	}
	items, err := reader.GetEventComments(ctx, eventID, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []json.RawMessage{}, nil
	}
	return items, nil
}

func (s Service) GetEventReports(ctx context.Context, eventID string, limit int) ([]ReportTarget, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, fmt.Errorf("event id is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("event reports")
	}
	items, err := reader.GetEventReports(ctx, eventID, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []ReportTarget{}, nil
	}
	return items, nil
}

func (s Service) GetUserReports(ctx context.Context, pubkey string, limit int) ([]ReportTarget, error) {
	pubkey = strings.TrimSpace(pubkey)
	if pubkey == "" {
		return nil, fmt.Errorf("pubkey is required")
	}
	reader := s.kindExpansion()
	if reader == nil {
		return nil, unsupportedCapabilityError("user reports")
	}
	items, err := reader.GetUserReports(ctx, pubkey, limit)
	if err != nil {
		return nil, err
	}
	if items == nil {
		return []ReportTarget{}, nil
	}
	return items, nil
}
