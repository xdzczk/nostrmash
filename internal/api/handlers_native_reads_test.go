package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xdzczk/nostrmash/internal/readmodel"
)

type nativePageReader struct {
	fakeEventReader
	listEventActions        func(context.Context, string, int, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
	moderationTags          func(context.Context, string, int) ([]readmodel.ModerationTag, json.RawMessage, error)
	followersPage           func(context.Context, string, int, *readmodel.FollowerCursor) ([]json.RawMessage, *readmodel.FollowerCursor, error)
	mutualFollowsPage       func(context.Context, string, string, int, string) ([]string, error)
	authorRepliesPage       func(context.Context, string, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
	recentEventsByKindPage  func(context.Context, int, string, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error)
	engagementNotifications func(context.Context, string, int, *readmodel.NotificationCursor) ([]readmodel.EngagementNotification, *readmodel.NotificationCursor, error)
	replaceableEvent        func(context.Context, string, int, string) (json.RawMessage, error)
}

func (r *nativePageReader) ListEventActions(ctx context.Context, eventID string, kind int, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
	return r.listEventActions(ctx, eventID, kind, limit, cursor)
}

func (r *nativePageReader) GetModerationListTags(ctx context.Context, pubkey string, kind int) ([]readmodel.ModerationTag, json.RawMessage, error) {
	return r.moderationTags(ctx, pubkey, kind)
}

func (r *nativePageReader) GetFollowersByPubkeyPage(ctx context.Context, targetPubkey string, limit int, cursor *readmodel.FollowerCursor) ([]json.RawMessage, *readmodel.FollowerCursor, error) {
	return r.followersPage(ctx, targetPubkey, limit, cursor)
}

func (r *nativePageReader) GetMutualFollowsPage(ctx context.Context, leftPubkey string, rightPubkey string, limit int, afterPubkey string) ([]string, error) {
	return r.mutualFollowsPage(ctx, leftPubkey, rightPubkey, limit, afterPubkey)
}

func (r *nativePageReader) GetAuthorRepliesPage(ctx context.Context, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
	return r.authorRepliesPage(ctx, pubkey, limit, cursor)
}

func (r *nativePageReader) GetRecentEventsByKindAndPubkeyPage(ctx context.Context, kind int, pubkey string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
	return r.recentEventsByKindPage(ctx, kind, pubkey, limit, cursor)
}

func (r *nativePageReader) ListEngagementNotifications(ctx context.Context, pubkey string, limit int, cursor *readmodel.NotificationCursor) ([]readmodel.EngagementNotification, *readmodel.NotificationCursor, error) {
	return r.engagementNotifications(ctx, pubkey, limit, cursor)
}

func (r *nativePageReader) GetParameterizedReplaceableEvent(ctx context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
	return r.replaceableEvent(ctx, pubkey, kind, dTag)
}

func callNative(t *testing.T, handler http.HandlerFunc, pattern, url string) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc(pattern, handler)
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) apiErrorBody {
	t.Helper()
	var envelope apiErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope: %v body=%s", err, rec.Body.String())
	}
	return envelope.Error
}

func TestGetEventActions_PaginatesReactionEvents(t *testing.T) {
	var cursors []*readmodel.EventOrderCursor
	reader := &nativePageReader{
		listEventActions: func(_ context.Context, eventID string, kind, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			if eventID != "note1" || kind != 7 || limit != 1 {
				t.Fatalf("unexpected args event=%s kind=%d limit=%d", eventID, kind, limit)
			}
			cursors = append(cursors, cursor)
			if cursor == nil {
				return []json.RawMessage{json.RawMessage(`{"id":"react-a","pubkey":"actor-a","kind":7,"created_at":20,"tags":[["e","note1"]],"content":"+"}`)},
					&readmodel.EventOrderCursor{CreatedAt: 20, ID: "react-a"}, nil
			}
			return []json.RawMessage{json.RawMessage(`{"id":"react-b","pubkey":"actor-b","kind":7,"created_at":10,"tags":[["e","note1"]],"content":"-"}`)}, nil, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	first := callNative(t, handlers.GetEventActions, "GET /api/v1/events/{id}/actions", "/api/v1/events/note1/actions?kind=7&limit=1")
	if first.Code != http.StatusOK {
		t.Fatalf("first status %d body %s", first.Code, first.Body.String())
	}
	var page struct {
		EventID    string            `json:"event_id"`
		Kind       int               `json:"kind"`
		Items      []json.RawMessage `json:"items"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode first page: %v", err)
	}
	if page.Kind != 7 || page.EventID != "note1" || page.NextCursor == "" || len(page.Items) != 1 {
		t.Fatalf("unexpected first page: %+v", page)
	}
	var item map[string]any
	if err := json.Unmarshal(page.Items[0], &item); err != nil {
		t.Fatalf("decode item: %v", err)
	}
	if item["kind"] != float64(7) || item["id"] != "react-a" {
		t.Fatalf("expected the reaction event, got %#v", item)
	}

	second := callNative(t, handlers.GetEventActions, "GET /api/v1/events/{id}/actions", "/api/v1/events/note1/actions?kind=7&limit=1&cursor="+page.NextCursor)
	if second.Code != http.StatusOK {
		t.Fatalf("second status %d body %s", second.Code, second.Body.String())
	}
	if len(cursors) != 2 || cursors[0] != nil || cursors[1] == nil || cursors[1].CreatedAt != 20 || cursors[1].ID != "react-a" {
		t.Fatalf("cursor was not applied: %#v", cursors)
	}
}

func TestGetEventActions_RejectsUnknownKind(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	rec := callNative(t, handlers.GetEventActions, "GET /api/v1/events/{id}/actions", "/api/v1/events/note1/actions?kind=1")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	body := decodeError(t, rec)
	if body.Code != "invalid_request" {
		t.Fatalf("code %q", body.Code)
	}
}

func TestGetEventActions_MissingEvent(t *testing.T) {
	reader := &nativePageReader{
		listEventActions: func(context.Context, string, int, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			return nil, nil, readmodel.ErrNotFound
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	rec := callNative(t, handlers.GetEventActions, "GET /api/v1/events/{id}/actions", "/api/v1/events/missing/actions?kind=9735")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	if decodeError(t, rec).Code != "not_found" {
		t.Fatalf("body %s", rec.Body.String())
	}
}

func TestGetEventZaps_ReturnsReceiptEvents(t *testing.T) {
	var gotKind int
	reader := &nativePageReader{
		listEventActions: func(_ context.Context, eventID string, kind, _ int, _ *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			gotKind = kind
			if eventID != "note1" {
				t.Fatalf("event %s", eventID)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"zap-1","pubkey":"sender","kind":9735,"created_at":30,"tags":[["e","note1"],["p","author"]],"content":""}`)}, nil, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	rec := callNative(t, handlers.GetEventZaps, "GET /api/v1/events/{id}/zaps", "/api/v1/events/note1/zaps")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var page struct {
		Kind  int               `json:"kind"`
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if gotKind != 9735 || page.Kind != 9735 || len(page.Items) != 1 {
		t.Fatalf("kind=%d page=%+v", gotKind, page)
	}
	var item map[string]any
	if err := json.Unmarshal(page.Items[0], &item); err != nil {
		t.Fatalf("decode item: %v", err)
	}
	if item["kind"] != float64(9735) || item["id"] != "zap-1" {
		t.Fatalf("expected zap receipt, got %#v", item)
	}

	missing := &nativePageReader{
		listEventActions: func(context.Context, string, int, int, *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			return nil, nil, readmodel.ErrNotFound
		},
	}
	handlers = mustNewHandlers(t, missing, 10)
	rec = callNative(t, handlers.GetEventZaps, "GET /api/v1/events/{id}/zaps", "/api/v1/events/gone/zaps")
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != "not_found" {
		t.Fatalf("missing zap target: %d %s", rec.Code, rec.Body.String())
	}
}

func TestGetMuteList_KeepsTagTypes(t *testing.T) {
	const (
		pubkeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		eventHex  = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	reader := &nativePageReader{
		moderationTags: func(_ context.Context, pubkey string, kind int) ([]readmodel.ModerationTag, json.RawMessage, error) {
			if pubkey != "alice" || kind != 10000 {
				t.Fatalf("pubkey=%s kind=%d", pubkey, kind)
			}
			return []readmodel.ModerationTag{
				{Name: "p", Value: pubkeyHex},
				{Name: "e", Value: eventHex},
				{Name: "t", Value: "nostr"},
				{Name: "word", Value: "spam"},
			}, json.RawMessage(`{"id":"mute1","pubkey":"alice","kind":10000,"tags":[["p","` + pubkeyHex + `"],["e","` + eventHex + `"],["t","nostr"],["word","spam"]]}`), nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	rec := callNative(t, handlers.GetMuteList, "GET /api/v1/users/{pubkey}/mute-list", "/api/v1/users/alice/mute-list")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d body %s", rec.Code, rec.Body.String())
	}
	var page struct {
		MuteList []string `json:"mute_list"`
		Tags     []struct {
			Name  string `json:"name"`
			Value string `json:"value"`
		} `json:"tags"`
		Event map[string]any `json:"event"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(page.Tags) != 4 || page.Tags[0].Name != "p" || page.Tags[0].Value != pubkeyHex || page.Tags[1].Name != "e" || page.Tags[1].Value != eventHex {
		t.Fatalf("tags collapsed: %#v", page.Tags)
	}
	if page.Tags[2].Name != "t" || page.Tags[2].Value != "nostr" || page.Tags[3].Name != "word" || page.Tags[3].Value != "spam" {
		t.Fatalf("hashtag or word lost: %#v", page.Tags)
	}
	if page.Event["kind"] != float64(10000) {
		t.Fatalf("expected raw kind 10000 event, got %#v", page.Event)
	}
	if len(page.MuteList) != 4 || page.MuteList[0] != pubkeyHex || page.MuteList[1] != eventHex {
		t.Fatalf("mute_list values: %#v", page.MuteList)
	}
}

func TestGetFollowers_KeysetCursor(t *testing.T) {
	var seen *readmodel.FollowerCursor
	reader := &nativePageReader{
		followersPage: func(_ context.Context, pubkey string, limit int, cursor *readmodel.FollowerCursor) ([]json.RawMessage, *readmodel.FollowerCursor, error) {
			if pubkey != "alice" || limit != 1 {
				t.Fatalf("pubkey=%s limit=%d", pubkey, limit)
			}
			seen = cursor
			if cursor == nil {
				return []json.RawMessage{json.RawMessage(`{"follower_pubkey":"bob","source_event_id":"contact-1","contact_list_created_at":50}`)},
					&readmodel.FollowerCursor{ContactListCreatedAt: 50, SourceEventID: "contact-1", FollowerPubkey: "bob"}, nil
			}
			return []json.RawMessage{json.RawMessage(`{"follower_pubkey":"cara","source_event_id":"contact-1","contact_list_created_at":50}`)}, nil, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	first := callNative(t, handlers.GetFollowers, "GET /api/v1/users/{pubkey}/followers", "/api/v1/users/alice/followers?limit=1")
	if first.Code != http.StatusOK {
		t.Fatalf("status %d body %s", first.Code, first.Body.String())
	}
	var page struct {
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.NextCursor == "" {
		t.Fatal("missing next_cursor")
	}
	second := callNative(t, handlers.GetFollowers, "GET /api/v1/users/{pubkey}/followers", "/api/v1/users/alice/followers?limit=1&cursor="+page.NextCursor)
	if second.Code != http.StatusOK {
		t.Fatalf("second status %d body %s", second.Code, second.Body.String())
	}
	if seen == nil || seen.ContactListCreatedAt != 50 || seen.SourceEventID != "contact-1" || seen.FollowerPubkey != "bob" {
		t.Fatalf("cursor fields: %#v", seen)
	}
}

func TestGetAuthorRepliesAndLongForm_KeysetCursor(t *testing.T) {
	var replyCursor *readmodel.EventOrderCursor
	var longFormCursor *readmodel.EventOrderCursor
	reader := &nativePageReader{
		authorRepliesPage: func(_ context.Context, _ string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			if limit != 1 {
				t.Fatalf("reply limit %d", limit)
			}
			replyCursor = cursor
			if cursor == nil {
				return []json.RawMessage{json.RawMessage(`{"id":"reply-a","kind":1,"created_at":9,"tags":[["e","parent","","reply"]]}`)},
					&readmodel.EventOrderCursor{CreatedAt: 9, ID: "reply-a"}, nil
			}
			return []json.RawMessage{json.RawMessage(`{"id":"reply-b","kind":1,"created_at":8,"tags":[["e","parent","","reply"]]}`)}, nil, nil
		},
		recentEventsByKindPage: func(_ context.Context, kind int, _ string, limit int, cursor *readmodel.EventOrderCursor) ([]json.RawMessage, *readmodel.EventOrderCursor, error) {
			if kind != 30023 || limit != 1 {
				t.Fatalf("kind=%d limit=%d", kind, limit)
			}
			longFormCursor = cursor
			if cursor == nil {
				return []json.RawMessage{json.RawMessage(`{"id":"article-a","kind":30023,"created_at":4}`)},
					&readmodel.EventOrderCursor{CreatedAt: 4, ID: "article-a"}, nil
			}
			return []json.RawMessage{json.RawMessage(`{"id":"article-b","kind":30023,"created_at":3}`)}, nil, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)

	replies := callNative(t, handlers.GetAuthorReplies, "GET /api/v1/authors/{pubkey}/replies", "/api/v1/authors/alice/replies?limit=1")
	if replies.Code != http.StatusOK {
		t.Fatalf("replies %d %s", replies.Code, replies.Body.String())
	}
	var replyPage struct {
		Items      []json.RawMessage `json:"items"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(replies.Body.Bytes(), &replyPage); err != nil {
		t.Fatalf("decode replies: %v", err)
	}
	if len(replyPage.Items) != 1 || replyPage.NextCursor == "" {
		t.Fatalf("reply page: %+v", replyPage)
	}
	again := callNative(t, handlers.GetAuthorReplies, "GET /api/v1/authors/{pubkey}/replies", "/api/v1/authors/alice/replies?limit=1&cursor="+replyPage.NextCursor)
	if again.Code != http.StatusOK {
		t.Fatalf("second replies %d %s", again.Code, again.Body.String())
	}
	if replyCursor == nil || replyCursor.CreatedAt != 9 || replyCursor.ID != "reply-a" {
		t.Fatalf("reply cursor %#v", replyCursor)
	}

	longForm := callNative(t, handlers.GetLongForm, "GET /api/v1/users/{pubkey}/long-form", "/api/v1/users/alice/long-form?limit=1")
	if longForm.Code != http.StatusOK {
		t.Fatalf("long-form %d %s", longForm.Code, longForm.Body.String())
	}
	var articles struct {
		LongForm   []json.RawMessage `json:"long_form"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(longForm.Body.Bytes(), &articles); err != nil {
		t.Fatalf("decode long-form: %v", err)
	}
	if len(articles.LongForm) != 1 || articles.NextCursor == "" {
		t.Fatalf("long-form page: %+v", articles)
	}
	again = callNative(t, handlers.GetLongForm, "GET /api/v1/users/{pubkey}/long-form", "/api/v1/users/alice/long-form?limit=1&cursor="+articles.NextCursor)
	if again.Code != http.StatusOK {
		t.Fatalf("second long-form %d %s", again.Code, again.Body.String())
	}
	if longFormCursor == nil || longFormCursor.CreatedAt != 4 || longFormCursor.ID != "article-a" {
		t.Fatalf("long-form cursor %#v", longFormCursor)
	}
}

func TestGetMutualFollows_SamePubkeyPagesPubkeys(t *testing.T) {
	var after string
	reader := &nativePageReader{
		mutualFollowsPage: func(_ context.Context, left, right string, limit int, afterPubkey string) ([]string, error) {
			if left != "alice" || right != "alice" || limit != 1 {
				t.Fatalf("left=%s right=%s limit=%d", left, right, limit)
			}
			after = afterPubkey
			if afterPubkey == "" {
				return []string{"bob", "cara"}, nil
			}
			return []string{"cara"}, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	first := callNative(t, handlers.GetMutualFollows, "GET /api/v1/users/{pubkey}/mutual-follows", "/api/v1/users/alice/mutual-follows?user_pubkey=alice&limit=1")
	if first.Code != http.StatusOK {
		t.Fatalf("status %d body %s", first.Code, first.Body.String())
	}
	var page struct {
		Pubkeys    []string `json:"pubkeys"`
		NextCursor string   `json:"next_cursor"`
		UserPubkey string   `json:"user_pubkey"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.UserPubkey != "alice" || len(page.Pubkeys) != 1 || page.Pubkeys[0] != "bob" || page.NextCursor == "" {
		t.Fatalf("page %+v", page)
	}
	second := callNative(t, handlers.GetMutualFollows, "GET /api/v1/users/{pubkey}/mutual-follows", "/api/v1/users/alice/mutual-follows?limit=1&cursor="+page.NextCursor)
	if second.Code != http.StatusOK {
		t.Fatalf("second %d %s", second.Code, second.Body.String())
	}
	if after != "bob" {
		t.Fatalf("after %q", after)
	}
}

func TestGetNotifications_IncludesQuotesAndMentions(t *testing.T) {
	var seen *readmodel.NotificationCursor
	reader := &nativePageReader{
		engagementNotifications: func(_ context.Context, pubkey string, limit int, cursor *readmodel.NotificationCursor) ([]readmodel.EngagementNotification, *readmodel.NotificationCursor, error) {
			if pubkey != "alice" || limit != 1 {
				t.Fatalf("pubkey=%s limit=%d", pubkey, limit)
			}
			seen = cursor
			if cursor == nil {
				return []readmodel.EngagementNotification{{
					Type: "quote", CreatedAt: 40, ActorPubkey: "quoter", EventID: "quote-event",
				}}, &readmodel.NotificationCursor{CreatedAt: 40, Type: "quote", ActorPubkey: "quoter", EventID: "quote-event"}, nil
			}
			return []readmodel.EngagementNotification{{
				Type: "mention", CreatedAt: 30, ActorPubkey: "mentioner", EventID: "mention-event",
			}}, nil, nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	first := callNative(t, handlers.GetNotifications, "GET /api/v1/users/{pubkey}/notifications", "/api/v1/users/alice/notifications?limit=1")
	if first.Code != http.StatusOK {
		t.Fatalf("status %d body %s", first.Code, first.Body.String())
	}
	var page struct {
		Notifications []struct {
			Type        string `json:"type"`
			CreatedAt   int64  `json:"created_at"`
			ActorPubkey string `json:"actor_pubkey"`
			EventID     string `json:"event_id"`
		} `json:"notifications"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.NextCursor == "" || len(page.Notifications) != 1 || page.Notifications[0].Type != "quote" || page.Notifications[0].ActorPubkey != "quoter" || page.Notifications[0].EventID != "quote-event" {
		t.Fatalf("first page %+v", page)
	}
	second := callNative(t, handlers.GetNotifications, "GET /api/v1/users/{pubkey}/notifications", "/api/v1/users/alice/notifications?limit=1&cursor="+page.NextCursor)
	if second.Code != http.StatusOK {
		t.Fatalf("second %d %s", second.Code, second.Body.String())
	}
	if err := json.Unmarshal(second.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode second: %v", err)
	}
	if len(page.Notifications) != 1 || page.Notifications[0].Type != "mention" || page.Notifications[0].ActorPubkey != "mentioner" || page.Notifications[0].EventID != "mention-event" {
		t.Fatalf("second page %+v", page)
	}
	if seen == nil || seen.Type != "quote" || seen.ActorPubkey != "quoter" || seen.EventID != "quote-event" || seen.CreatedAt != 40 {
		t.Fatalf("notification cursor %#v", seen)
	}
}

func TestGetReplaceable_ReturnsEventOrNotFound(t *testing.T) {
	reader := &nativePageReader{
		replaceableEvent: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			if pubkey != "alice" || kind != 30023 || dTag != "article" {
				t.Fatalf("pubkey=%s kind=%d d=%q", pubkey, kind, dTag)
			}
			return json.RawMessage(`{"id":"article-a","pubkey":"alice","kind":30023,"tags":[["d","article"]]}`), nil
		},
	}
	handlers := mustNewHandlers(t, reader, 10)
	found := callNative(t, handlers.GetReplaceable, "GET /api/v1/replaceable", "/api/v1/replaceable?pubkey=alice&kind=30023&identifier=article")
	if found.Code != http.StatusOK {
		t.Fatalf("status %d body %s", found.Code, found.Body.String())
	}
	var page struct {
		Event map[string]any `json:"event"`
	}
	if err := json.Unmarshal(found.Body.Bytes(), &page); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if page.Event["id"] != "article-a" || page.Event["kind"] != float64(30023) {
		t.Fatalf("event %#v", page.Event)
	}

	missing := &nativePageReader{
		replaceableEvent: func(context.Context, string, int, string) (json.RawMessage, error) {
			return nil, readmodel.ErrNotFound
		},
	}
	handlers = mustNewHandlers(t, missing, 10)
	rec := callNative(t, handlers.GetReplaceable, "GET /api/v1/replaceable", "/api/v1/replaceable?pubkey=alice&kind=30023&identifier=missing")
	if rec.Code != http.StatusNotFound || decodeError(t, rec).Code != "not_found" {
		t.Fatalf("missing: %d %s", rec.Code, rec.Body.String())
	}

	rec = callNative(t, handlers.GetReplaceable, "GET /api/v1/replaceable", "/api/v1/replaceable?pubkey=alice&kind=30023")
	if rec.Code != http.StatusBadRequest || decodeError(t, rec).Code != "invalid_request" {
		t.Fatalf("missing identifier: %d %s", rec.Code, rec.Body.String())
	}

	var gotDTag string
	emptyID := &nativePageReader{
		replaceableEvent: func(_ context.Context, _ string, _ int, dTag string) (json.RawMessage, error) {
			gotDTag = dTag
			return json.RawMessage(`{"id":"blank-d","kind":30023,"tags":[["d",""]]}`), nil
		},
	}
	handlers = mustNewHandlers(t, emptyID, 10)
	rec = callNative(t, handlers.GetReplaceable, "GET /api/v1/replaceable", "/api/v1/replaceable?pubkey=alice&kind=30023&identifier=")
	if rec.Code != http.StatusOK {
		t.Fatalf("empty identifier: %d %s", rec.Code, rec.Body.String())
	}
	if gotDTag != "" {
		t.Fatalf("empty d-tag was rewritten to %q", gotDTag)
	}
}
