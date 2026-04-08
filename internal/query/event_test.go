package query

import (
	"context"
	"testing"
)

func TestGetEventActionCountsAliasesLegacyMethod(t *testing.T) {
	t.Parallel()
	svc := NewEventService(fakeEventReader{
		getEventCountsFn: func(_ context.Context, eventID string) (EventCounts, error) {
			if eventID != "event-1" {
				t.Fatalf("unexpected event id: %q", eventID)
			}
			return EventCounts{
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

func TestServiceGetActionCountsUsesSharedEventOrchestration(t *testing.T) {
	t.Parallel()
	svc := NewService(fakeReader{})
	svc.reader = readerWithCounts{
		Reader: svc.reader,
		getEventCountsFn: func(_ context.Context, eventID string) (EventCounts, error) {
			if eventID != "event-2" {
				t.Fatalf("unexpected event id: %q", eventID)
			}
			return EventCounts{
				EventID:       "event-2",
				ReplyCount:    1,
				ReactionCount: 5,
				RepostCount:   0,
				Consistency:   "eventual",
			}, nil
		},
	}

	out, err := svc.GetActionCounts(context.Background(), "event-2")
	if err != nil {
		t.Fatalf("GetActionCounts returned error: %v", err)
	}
	if out.EventID != "event-2" || out.ReplyCount != 1 || out.ReactionCount != 5 || out.RepostCount != 0 {
		t.Fatalf("unexpected action counts: %#v", out)
	}
}

type readerWithCounts struct {
	Reader
	getEventCountsFn func(context.Context, string) (EventCounts, error)
}

func (r readerWithCounts) GetEventCounts(ctx context.Context, eventID string) (EventCounts, error) {
	if r.getEventCountsFn == nil {
		return EventCounts{}, nil
	}
	return r.getEventCountsFn(ctx, eventID)
}
