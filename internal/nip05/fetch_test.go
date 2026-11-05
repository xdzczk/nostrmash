package nip05

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestCheckVerifiedMismatchAndMissing(t *testing.T) {
	t.Parallel()
	pubkey := strings.Repeat("ab", 32)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/nostr.json" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Query().Get("name") {
		case "missing":
			_, _ = w.Write([]byte(`{"names":{}}`))
		default:
			_, _ = w.Write([]byte(`{"names":{"bob":"` + pubkey + `"}}`))
		}
	}))
	defer srv.Close()

	f := NewFetcherForTest(2 * time.Second)
	f.client = srv.Client()
	f.endpointOverride = func(_, local string) string {
		return srv.URL + "/.well-known/nostr.json?name=" + url.QueryEscape(local)
	}

	if got := f.Check(context.Background(), "bob", "example.com", pubkey); got.Status != StatusVerified {
		t.Fatalf("verified: %+v", got)
	}
	if got := f.Check(context.Background(), "bob", "example.com", strings.Repeat("cd", 32)); got.Status != StatusMismatch {
		t.Fatalf("mismatch: %+v", got)
	}
	if got := f.Check(context.Background(), "missing", "example.com", pubkey); got.Status != StatusMismatch || got.ErrorCode != "name_missing" {
		t.Fatalf("missing: %+v", got)
	}
}

func TestCheckRejectsPrivateHost(t *testing.T) {
	t.Parallel()
	f := NewFetcher(time.Second)
	got := f.Check(context.Background(), "bob", "127.0.0.1", strings.Repeat("ab", 32))
	if got.Status != StatusBlocked {
		t.Fatalf("status=%s code=%s", got.Status, got.ErrorCode)
	}
	got = f.Check(context.Background(), "bob", "localhost", strings.Repeat("ab", 32))
	if got.Status != StatusBlocked {
		t.Fatalf("localhost status=%s", got.Status)
	}
}
