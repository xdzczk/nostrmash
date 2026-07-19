package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/traceutil"
)

type threadService struct {
	reader ThreadReader
}

type descendingThreadWindowReader interface {
	GetEventRepliesDescending(ctx context.Context, eventID string, limit int, cursor *EventCursor, offset int) ([]json.RawMessage, *EventCursor, error)
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
		if errors.Is(err, readmodel.ErrNotFound) {
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

func (s Service) GetThreadSummary(ctx context.Context, rootEventID string) (out ThreadSummary, err error) {
	ctx, span := traceutil.StartSpan(ctx, "query.get_thread_summary")
	defer func() { span.End(err) }()
	rootEventID = strings.TrimSpace(rootEventID)
	if rootEventID == "" {
		return ThreadSummary{}, fmt.Errorf("root event id is required")
	}
	cap := s.capabilities.thread.summary
	if cap == nil {
		return ThreadSummary{}, unsupportedCapabilityError("thread summary")
	}
	row, err := cap.GetThreadSummary(ctx, rootEventID)
	if err != nil {
		return ThreadSummary{}, err
	}
	return threadSummaryFromStore(row), nil
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
		if errors.Is(err, readmodel.ErrNotFound) {
			return ThreadView{}, ErrThreadEventNotFound
		}
		return ThreadView{}, err
	}
	ancestors, missing, err := s.reader.GetEventAncestors(ctx, eventID, req.MaxDepth)
	if err != nil {
		return ThreadView{}, err
	}

	out = ThreadView{
		Event:              raw,
		Ancestors:          ancestors,
		MissingAncestorIDs: missing,
		Consistency:        "eventual",
	}

	if descendingReader, ok := s.reader.(descendingThreadWindowReader); ok {
		replies, next, err := descendingReader.GetEventRepliesDescending(ctx, eventID, req.Limit, req.Cursor, req.Offset)
		if err == nil {
			out.Replies = replies
			out.NextCursor = next
			return out, nil
		}
		if !IsUnsupportedCapability(err) {
			return ThreadView{}, err
		}
	}

	window, next, err := s.getThreadWindowFromAscendingScan(ctx, eventID, req, fetchPageSize)
	if err != nil {
		return ThreadView{}, err
	}
	out.Replies = window
	out.NextCursor = next
	return out, nil
}

func (s threadService) getThreadWindowFromAscendingScan(
	ctx context.Context,
	eventID string,
	req ThreadWindowRequest,
	fetchPageSize int,
) ([]json.RawMessage, *EventCursor, error) {
	var ascCursor *EventCursor
	seenCursors := map[string]struct{}{}
	capacity := descendingScanCapacity(req)
	collected := make([]json.RawMessage, 0, capacity)
	for {
		replies, nextCursor, err := s.reader.GetEventReplies(ctx, eventID, fetchPageSize, ascCursor)
		if err != nil {
			return nil, nil, err
		}
		if req.Cursor == nil {
			collected = appendTailReplies(collected, replies, capacity)
		} else {
			for _, reply := range replies {
				meta, ok := parseReplyMeta(reply)
				if !ok {
					continue
				}
				if meta.id == req.Cursor.ID && meta.createdAt == req.Cursor.CreatedAt {
					window, next := WindowDescendingReplies(collected, nil, req.Limit, nil, 0)
					return window, next, nil
				}
				collected = appendTailReply(collected, reply, capacity)
			}
		}
		if nextCursor == nil || len(replies) == 0 {
			break
		}
		key := cursorKey(*nextCursor)
		if _, seen := seenCursors[key]; seen {
			break
		}
		seenCursors[key] = struct{}{}
		ascCursor = nextCursor
	}
	if req.Cursor != nil {
		return []json.RawMessage{}, nil, nil
	}
	window, next := WindowDescendingReplies(collected, nil, req.Limit, nil, req.Offset)
	return window, next, nil
}

func descendingScanCapacity(req ThreadWindowRequest) int {
	if req.Limit <= 0 {
		return 1
	}
	if req.Cursor != nil {
		return req.Limit + 1
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}
	return req.Limit + offset + 1
}

func cursorKey(cur EventCursor) string {
	return fmt.Sprintf("%d/%s", cur.CreatedAt, strings.TrimSpace(cur.ID))
}

type replyMeta struct {
	createdAt int64
	id        string
}

func parseReplyMeta(raw json.RawMessage) (replyMeta, bool) {
	var payload struct {
		ID        string `json:"id"`
		CreatedAt int64  `json:"created_at"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return replyMeta{}, false
	}
	payload.ID = strings.TrimSpace(payload.ID)
	if payload.ID == "" {
		return replyMeta{}, false
	}
	return replyMeta{createdAt: payload.CreatedAt, id: payload.ID}, true
}

func appendTailReplies(dst []json.RawMessage, batch []json.RawMessage, limit int) []json.RawMessage {
	if limit <= 0 {
		return dst[:0]
	}
	if len(batch) >= limit {
		tail := batch[len(batch)-limit:]
		out := make([]json.RawMessage, 0, limit)
		out = append(out, tail...)
		return out
	}
	needDrop := len(dst) + len(batch) - limit
	if needDrop > 0 {
		dst = append(dst[:0], dst[needDrop:]...)
	}
	dst = append(dst, batch...)
	return dst
}

func appendTailReply(dst []json.RawMessage, value json.RawMessage, limit int) []json.RawMessage {
	if limit <= 0 {
		return dst[:0]
	}
	if len(dst) >= limit {
		copy(dst, dst[1:])
		dst = dst[:limit-1]
	}
	return append(dst, value)
}

func (s Service) GetThreadView(
	ctx context.Context,
	eventID string,
	limit int,
	maxDepth int,
	cursor *EventCursor,
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
