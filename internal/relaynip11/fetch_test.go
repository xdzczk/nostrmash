package relaynip11

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestDocumentURL(t *testing.T) {
	got, err := DocumentURL("wss://Relay.Example.com/nostr")
	if err != nil || got != "https://Relay.Example.com/nostr" {
		t.Fatalf("got %q err %v", got, err)
	}
	if _, err := DocumentURL("http://relay.example.com"); err == nil {
		t.Fatal("expected scheme rejection")
	}
}

func TestFetch_RejectsLoopbackBeforeDial(t *testing.T) {
	f := NewFetcher(time.Second)
	_, err := f.Fetch(context.Background(), "wss://127.0.0.1")
	var coded *FetchError
	if !errors.As(err, &coded) || coded.Code != "private_address" {
		t.Fatalf("err = %v", err)
	}
}

func TestFetchURL_ParsesDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/nostr+json" {
			t.Errorf("accept = %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"Demo","limitation":{"auth_required":true}}`))
	}))
	t.Cleanup(srv.Close)

	f := &Fetcher{client: srv.Client(), timeout: time.Second, maxBytes: 1024}
	info, err := f.fetchURL(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if info.Name != "Demo" || info.Limitation == nil || info.Limitation.AuthRequired == nil || !*info.Limitation.AuthRequired {
		t.Fatalf("%#v", info)
	}
}

func TestFetchURL_HTMLAndStatusAndSize(t *testing.T) {
	html := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html></html>"))
	}))
	t.Cleanup(html.Close)
	missing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(missing.Close)
	huge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 64)))
	}))
	t.Cleanup(huge.Close)

	f := &Fetcher{timeout: time.Second, maxBytes: 16}
	f.client = html.Client()
	if _, err := f.fetchURL(context.Background(), html.URL); err == nil || err.Error() != "not_json" {
		t.Fatalf("html: %v", err)
	}
	f.client = missing.Client()
	if _, err := f.fetchURL(context.Background(), missing.URL); err == nil || err.Error() != "http_404" {
		t.Fatalf("404: %v", err)
	}
	f.client = huge.Client()
	if _, err := f.fetchURL(context.Background(), huge.URL); err == nil || err.Error() != "too_large" {
		t.Fatalf("size: %v", err)
	}
}

func TestCheckRedirect(t *testing.T) {
	original, err := http.NewRequest(http.MethodGet, "https://relay.example/path", nil)
	if err != nil {
		t.Fatal(err)
	}
	private, err := http.NewRequest(http.MethodGet, "https://127.0.0.1/path", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRedirect(private, []*http.Request{original}); !errors.Is(err, errRedirectRejected) {
		t.Fatalf("private redirect: %v", err)
	}
	other, err := http.NewRequest(http.MethodGet, "https://other.example/path", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRedirect(other, []*http.Request{original}); !errors.Is(err, errRedirectRejected) {
		t.Fatalf("host change: %v", err)
	}
	down, err := http.NewRequest(http.MethodGet, "http://relay.example/path", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRedirect(down, []*http.Request{original}); !errors.Is(err, errRedirectRejected) {
		t.Fatalf("downgrade: %v", err)
	}
	same, err := http.NewRequest(http.MethodGet, "https://relay.example/nip11", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkRedirect(same, []*http.Request{original}); err != nil {
		t.Fatalf("same host: %v", err)
	}
	if err := checkRedirect(same, []*http.Request{original, original}); !errors.Is(err, errRedirectRejected) {
		t.Fatalf("too many: %v", err)
	}
}

func TestPublicDialRejectsLoopback(t *testing.T) {
	_, err := publicDialContext(context.Background(), time.Second, "tcp", "127.0.0.1:1")
	if !errors.Is(err, errPrivateAddress) {
		t.Fatalf("err = %v", err)
	}
}
