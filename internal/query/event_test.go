package query

import (
	"context"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventActionCountsAliasesLegacyMethod(t *testing.T) {
	t.Parallel()
	svc := NewEventService(fakeEventReader{
		getEventCountsFn: func(_ context.Context, eventID string) (store.EventCounts, error) {
			if eventID != "event-1" {
				t.Fatalf("unexpected event id: %q", eventID)
			}
			return store.EventCounts{
				EventID:       "event-1",
				ReplyCount:    2,
				ReactionCount: 3,
				RepostCount:   4,
				Consistency:   "eventual",
			}, nil
		},
	})

	out, err := svc.GetEventActionCounts(context.Background(), "event-1")
	if err != nil {
		t.Fatalf("GetEventActionCounts returned error: %v", err)
	}
	if out.EventID != "event-1" || out.ReplyCount != 2 || out.ReactionCount != 3 || out.RepostCount != 4 {
		t.Fatalf("unexpected action counts: %#v", out)
	}
}
