package api_primal

import (
	"context"
	"strings"

	"github.com/xdzczk/nostrmash/internal/query"
)

func (g WSGateway) videoReelsPage(ctx context.Context, kwargs, spec map[string]any, limit int) ([]any, error) {
	viewer := g.feedViewer(kwargs, spec)
	followedBy := ""
	if validatePubkeyHex(viewer) == nil {
		followedBy = viewer
	}
	rows, err := g.query.GetShortVideos(ctx, followedBy, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			return g.emptyFeedPage(), nil
		}
		return nil, err
	}
	return g.enrichFeedEvents(ctx, viewer, rows, "created_at", nil, nil), nil
}

func (g WSGateway) primalV1FindLiveEvents(ctx context.Context, kwargs map[string]any) ([]any, error) {
	return g.liveEventsPage(ctx, kwargs, "")
}

func (g WSGateway) primalV1LiveEventsFromFollows(ctx context.Context, kwargs map[string]any) ([]any, error) {
	viewer := strings.ToLower(strings.TrimSpace(stringValue(kwargs["user_pubkey"])))
	if validatePubkeyHex(viewer) != nil {
		viewer = ""
	}
	return g.liveEventsPage(ctx, kwargs, viewer)
}

func (g WSGateway) liveEventsPage(ctx context.Context, kwargs map[string]any, followedBy string) ([]any, error) {
	limit := toBoundedPositiveInt(kwargs["limit"], 20, 100)
	rows, err := g.query.GetLiveActivities(ctx, "live", followedBy, limit)
	if err != nil {
		if query.IsUnsupportedCapability(err) {
			return g.emptyFeedPage(), nil
		}
		return nil, err
	}
	return g.enrichFeedEvents(ctx, followedBy, rows, "created_at", nil, nil), nil
}
