package api_primal

import (
	"context"
	"encoding/json"

	"github.com/xdzczk/nostrmash/internal/store"
)

type kindExpansionHooks struct {
	authorMedia    func(context.Context, string, int) ([]json.RawMessage, error)
	authorLive     func(context.Context, string, int) ([]json.RawMessage, error)
	authorPins     func(context.Context, string) ([]store.PinItem, error)
	liveActivities func(context.Context, string, string, int) ([]json.RawMessage, error)
	shortVideos    func(context.Context, string, int) ([]json.RawMessage, error)
	eventComments  func(context.Context, string, int) ([]json.RawMessage, error)
	eventReports   func(context.Context, string, int) ([]store.ReportTarget, error)
	userReports    func(context.Context, string, int) ([]store.ReportTarget, error)
}

func (f fakeEventReader) GetAuthorMedia(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.authorMedia == nil {
		return []json.RawMessage{}, nil
	}
	return f.authorMedia(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorLive(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.authorLive == nil {
		return []json.RawMessage{}, nil
	}
	return f.authorLive(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorPins(ctx context.Context, pubkey string) ([]store.PinItem, error) {
	if f.authorPins == nil {
		return []store.PinItem{}, nil
	}
	return f.authorPins(ctx, pubkey)
}

func (f fakeEventReader) GetLiveActivities(ctx context.Context, status, followedBy string, limit int) ([]json.RawMessage, error) {
	if f.liveActivities == nil {
		return []json.RawMessage{}, nil
	}
	return f.liveActivities(ctx, status, followedBy, limit)
}

func (f fakeEventReader) GetShortVideos(ctx context.Context, followedBy string, limit int) ([]json.RawMessage, error) {
	if f.shortVideos == nil {
		return []json.RawMessage{}, nil
	}
	return f.shortVideos(ctx, followedBy, limit)
}

func (f fakeEventReader) GetEventComments(ctx context.Context, eventID string, limit int) ([]json.RawMessage, error) {
	if f.eventComments == nil {
		return nil, nil
	}
	return f.eventComments(ctx, eventID, limit)
}

func (f fakeEventReader) GetEventReports(ctx context.Context, eventID string, limit int) ([]store.ReportTarget, error) {
	if f.eventReports == nil {
		return []store.ReportTarget{}, nil
	}
	return f.eventReports(ctx, eventID, limit)
}

func (f fakeEventReader) GetUserReports(ctx context.Context, pubkey string, limit int) ([]store.ReportTarget, error) {
	if f.userReports == nil {
		return []store.ReportTarget{}, nil
	}
	return f.userReports(ctx, pubkey, limit)
}
