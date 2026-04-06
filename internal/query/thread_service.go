package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/store"
	"github.com/xdzczk/nostrmash/internal/store/traceutil"
)

type threadService struct {
	reader ThreadReader
}

// NewThreadService constructs a thread-only orchestration service from a narrow dependency.
func NewThreadService(reader ThreadReader) ThreadService {
	return threadService{reader: reader}
}

func (s Service) GetThread(ctx context.Context, req ThreadRequest) (out ThreadView, err error) {
	return threadService{reader: s.reader}.GetThread(ctx, req)
}

func (s threadService) GetThread(ctx context.Context, req ThreadRequest) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread")
	defer func() { span.End(err) }()
	out = ThreadView{Consistency: "eventual"}
	eventID := strings.TrimSpace(req.EventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return out, ErrThreadEventNotFound
		}
		return out, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, req.MaxDepth)
	if err != nil {
		return out, err
	}
	replies, next, err := s.reader.GetEventReplies(ctx, eventID, req.Limit, req.Cursor)
	if err != nil {
		return out, err
	}
	out.Event = raw
	out.Ancestors = ancestors
	out.MissingAncestorIDs = missing
	out.Replies = replies
	out.NextCursor = next
	return out, nil
}

func (s Service) GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (out ThreadView, err error) {
	return threadService{reader: s.reader}.GetThreadWindow(ctx, req)
}

func (s threadService) GetThreadWindow(ctx context.Context, req ThreadWindowRequest) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread_window")
	defer func() { span.End(err) }()
	const fetchPageSize = 100
	eventID := strings.TrimSpace(req.EventID)
	if eventID == "" {
		return ThreadView{}, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return ThreadView{}, ErrThreadEventNotFound
		}
		return ThreadView{}, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, req.MaxDepth)
	if err != nil {
		return ThreadView{}, err
	}
	var ascCursor *store.EventOrderCursor
	collected := make([]json.RawMessage, 0, fetchPageSize)
	type cursorKey struct {
		createdAt int64
		id        string
	}
	seenCursors := map[cursorKey]struct{}{}
	for {
		replies, nextCursor, err := s.reader.GetEventReplies(ctx, eventID, fetchPageSize, ascCursor)
		if err != nil {
			return ThreadView{}, err
		}
		collected = append(collected, replies...)
		if nextCursor == nil || len(replies) == 0 {
			break
		}
		key := cursorKey{
			createdAt: nextCursor.CreatedAt,
			id:        strings.TrimSpace(nextCursor.ID),
		}
		if _, seen := seenCursors[key]; seen {
			break
		}
		seenCursors[key] = struct{}{}
		ascCursor = nextCursor
	}

	out = ThreadView{
		Event:              raw,
		Ancestors:          ancestors,
		MissingAncestorIDs: missing,
		Consistency:        "eventual",
	}
	window, next := WindowDescendingReplies(collected, nil, req.Limit, req.Cursor, req.Offset)
	out.Replies = window
	out.NextCursor = next
	return out, nil
}

func (s Service) GetThreadView(
	ctx context.Context,
	eventID string,
	limit int,
	maxDepth int,
	cursor *store.EventOrderCursor,
) (out ThreadView, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread_view")
	defer func() { span.End(err) }()
	out = ThreadView{Consistency: "eventual"}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return out, fmt.Errorf("event id is required")
	}
	raw, err := s.reader.GetEventRawByID(ctx, eventID)
	if err != nil {
		return out, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, maxDepth)
	if err != nil {
		return out, err
	}
	replies, next, err := s.reader.GetEventReplies(ctx, eventID, limit, cursor)
	if err != nil {
		return out, err
	}
	out.Event = raw
	out.Ancestors = ancestors
	out.MissingAncestorIDs = missing
	out.Replies = replies
	out.NextCursor = next
	return out, nil
}
