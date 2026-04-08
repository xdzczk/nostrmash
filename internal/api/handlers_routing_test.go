package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUserDMRouteAbsentWhenOnlyMentionsAndFollowersRegistered(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/users/{pubkey}/mentions", handlers.GetMentions)
	mux.HandleFunc("GET /api/v1/users/{pubkey}/followers", handlers.GetFollowers)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/pk1/dms", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
	}
}
