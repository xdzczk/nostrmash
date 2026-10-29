package api_primal

import (
	"context"
	"encoding/json"
	"testing"
)

func TestVideoReelsAndLiveCacheCalls(t *testing.T) {
	t.Parallel()
	reel := json.RawMessage(`{"id":"reel1","kind":22,"pubkey":"ab","created_at":1,"tags":[],"content":"clip"}`)
	live := json.RawMessage(`{"id":"live1","kind":30311,"pubkey":"ab","created_at":2,"tags":[["status","live"]],"content":""}`)
	gateway := mustNewWSGateway(t, fakeEventReader{
		kindExpansionHooks: kindExpansionHooks{
			shortVideos: func(_ context.Context, followedBy string, _ int) ([]json.RawMessage, error) {
				if followedBy != "" {
					t.Fatalf("video-reels followedBy = %q, want global", followedBy)
				}
				return []json.RawMessage{reel}, nil
			},
			liveActivities: func(_ context.Context, status, followedBy string, _ int) ([]json.RawMessage, error) {
				if status != "live" {
					t.Fatalf("status = %q", status)
				}
				if followedBy != "" && followedBy != "ab" {
					t.Fatalf("followedBy = %q", followedBy)
				}
				return []json.RawMessage{live}, nil
			},
		},
	}, WSGatewayOptions{})

	page, err := gateway.routeMegaFeed(context.Background(), map[string]any{}, map[string]any{"id": "video-reels"})
	if err != nil {
		t.Fatalf("video-reels: %v", err)
	}
	if !pageContainsID(t, page, "reel1") {
		t.Fatalf("video-reels page missing reel: %#v", page)
	}
	livePage, err := gateway.primalV1FindLiveEvents(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("find_live_events: %v", err)
	}
	if !pageContainsID(t, livePage, "live1") {
		t.Fatalf("find_live_events page missing live: %#v", livePage)
	}
	follows, err := gateway.primalV1LiveEventsFromFollows(context.Background(), map[string]any{})
	if err != nil {
		t.Fatalf("live_events_from_follows: %v", err)
	}
	if !pageContainsID(t, follows, "live1") {
		t.Fatalf("live_events_from_follows page missing live: %#v", follows)
	}
}

func pageContainsID(t *testing.T, page []any, id string) bool {
	t.Helper()
	raw, err := json.Marshal(page)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	for i := 0; i+len(id) <= len(raw); i++ {
		if string(raw[i:i+len(id)]) == id {
			return true
		}
	}
	return false
}
