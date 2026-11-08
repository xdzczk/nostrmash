package api_primal

import (
	"encoding/json"
	"testing"
	"time"
)

func TestFeedPageCacheReturnsTheFirstPageUntilItExpires(t *testing.T) {
	cache := newFeedPageCache(time.Minute)
	key := feedPageCacheKey("Ab", []int{1, 6}, 20)
	page := []any{json.RawMessage(`{"id":"1"}`)}
	cache.set(key, page, time.Now())
	got, ok := cache.get(feedPageCacheKey("ab", []int{1, 6}, 20), time.Now())
	if !ok || len(got) != 1 {
		t.Fatalf("cache miss: %#v %v", got, ok)
	}
	if _, ok := cache.get(key, time.Now().Add(2*time.Minute)); ok {
		t.Fatal("expired page was returned")
	}
}

func TestFeedPageCacheInvalidatesWhenTheLiveSetChanges(t *testing.T) {
	cache := newFeedPageCache(time.Minute)
	viewer := "ab"
	key := feedPageCacheKey(viewer, []int{1}, 20)
	cache.set(key, []any{json.RawMessage(`{"id":"note"}`)}, time.Now())
	cache.noteLive(viewer, "live-a,")
	if _, ok := cache.get(key, time.Now()); !ok {
		t.Fatal("the first live observation cleared the feed page")
	}
	cache.noteLive(viewer, "live-a,")
	if _, ok := cache.get(key, time.Now()); !ok {
		t.Fatal("an unchanged live set cleared the feed page")
	}
	cache.noteLive(viewer, "live-b,")
	if _, ok := cache.get(key, time.Now()); ok {
		t.Fatal("a changed live set left the feed page in place")
	}
}
