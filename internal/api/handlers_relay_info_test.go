package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNIP11ListFields(t *testing.T) {
	raw := []byte(`{
		"fetched_at":"2026-10-29T12:00:00Z",
		"ok":true,
		"info":{
			"name":"Demo",
			"icon":"https://cdn.example/a.png",
			"limitation":{"auth_required":false,"payment_required":true}
		}
	}`)
	name, icon, auth, payment := nip11ListFields(raw)
	if name != "Demo" || icon != "https://cdn.example/a.png" {
		t.Fatalf("name=%q icon=%q", name, icon)
	}
	if auth == nil || *auth || payment == nil || !*payment {
		t.Fatalf("auth=%v payment=%v", auth, payment)
	}

	failed := []byte(`{"fetched_at":"2026-10-29T12:00:00Z","ok":false,"error":"http_404"}`)
	name, icon, auth, payment = nip11ListFields(failed)
	if name != "" || icon != "" || auth != nil || payment != nil {
		t.Fatalf("failed fetch leaked fields: %q %q %v %v", name, icon, auth, payment)
	}
	if nip11Document(failed) == nil {
		t.Fatal("failed fetch should still be returned on the relay page")
	}
	if nip11Document(nil) != nil {
		t.Fatal("missing document should be null")
	}
}

func TestGetRelayInfo_Validation(t *testing.T) {
	h := mustNewHandlersWithOptions(t, fakeEventReader{}, HandlersOptions{})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/relays/info", nil)
	rec := httptest.NewRecorder()
	h.GetRelayInfo(rec, req)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "relay_url") {
		t.Fatalf("missing url: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/relays/info?relay_url=wss://127.0.0.1", nil)
	rec = httptest.NewRecorder()
	h.GetRelayInfo(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("private url: %d %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/relays/info?relay_url=wss://relay.example", nil)
	rec = httptest.NewRecorder()
	h.GetRelayInfo(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("no pool: %d %s", rec.Code, rec.Body.String())
	}
}
