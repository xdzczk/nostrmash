package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type expectedRoute struct {
	Method string
	Path   string
}

func TestOpenAPIAndRouterStayInSync(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current test file path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	routerFile := filepath.Join(root, "cmd", "api", "main.go")
	openapiFile := filepath.Join(root, "docs", "openapi.yaml")

	routerSource, err := os.ReadFile(routerFile)
	if err != nil {
		t.Fatalf("read router source: %v", err)
	}
	openapiSource, err := os.ReadFile(openapiFile)
	if err != nil {
		t.Fatalf("read openapi source: %v", err)
	}

	expect := []expectedRoute{
		{Method: "GET", Path: "/health"},
		{Method: "GET", Path: "/ready"},
		{Method: "GET", Path: "/metrics"},
		{Method: "GET", Path: "/api/v1/events/{id}"},
		{Method: "POST", Path: "/api/v1/events/batch"},
		{Method: "GET", Path: "/api/v1/events/{id}/seen-on"},
		{Method: "GET", Path: "/api/v1/profiles/{pubkey}"},
		{Method: "POST", Path: "/api/v1/profiles/batch"},
		{Method: "GET", Path: "/api/v1/authors/{pubkey}/events"},
		{Method: "GET", Path: "/api/v1/authors/{pubkey}/replies"},
		{Method: "GET", Path: "/api/v1/events/{id}/counts"},
		{Method: "GET", Path: "/api/v1/events/{id}/replies"},
		{Method: "GET", Path: "/api/v1/events/{id}/ancestors"},
		{Method: "GET", Path: "/api/v1/threads/{eventId}"},
		{Method: "GET", Path: "/api/v1/relays/health"},
		{Method: "GET", Path: "/api/v1/contact-lists/{pubkey}"},
		{Method: "GET", Path: "/api/v1/relay-lists/{pubkey}"},
		{Method: "GET", Path: "/api/v1/search"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/bookmarks"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/highlights"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/long-form"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/zaps"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/mentions"},
		{Method: "GET", Path: "/api/v1/users/{pubkey}/followers"},
		{Method: "GET", Path: "/primal/v1/events/{id}"},
		{Method: "POST", Path: "/primal/v1/events/batch"},
		{Method: "GET", Path: "/primal/v1/profiles/{pubkey}"},
		{Method: "POST", Path: "/primal/v1/user_infos"},
		{Method: "GET", Path: "/primal/v1/threads/{eventId}"},
		{Method: "GET", Path: "/primal/v1/authors/{pubkey}/events"},
		{Method: "GET", Path: "/primal/v1/authors/{pubkey}/replies"},
		{Method: "GET", Path: "/primal/v1/events/{id}/actions"},
		{Method: "GET", Path: "/primal/v1/contact-lists/{pubkey}"},
		{Method: "GET", Path: "/primal/v1/relay-lists/{pubkey}"},
		{Method: "GET", Path: "/primal/ws"},
		{Method: "GET", Path: "/admin/v1/relays"},
		{Method: "GET", Path: "/admin/v1/jobs"},
		{Method: "GET", Path: "/admin/v1/invalid-events"},
		{Method: "GET", Path: "/admin/v1/rebuilds"},
		{Method: "POST", Path: "/admin/v1/rebuilds"},
		{Method: "GET", Path: "/admin/v1/storage"},
		{Method: "GET", Path: "/admin/v1/system"},
		{Method: "GET", Path: "/admin/v1/derivation-versions"},
	}

	routerText := string(routerSource)
	openapiText := string(openapiSource)
	for _, route := range expect {
		routerNeedle := `"` + route.Method + " " + route.Path + `"`
		if !strings.Contains(routerText, routerNeedle) {
			t.Fatalf("router missing route registration %s %s", route.Method, route.Path)
		}
		if !strings.Contains(openapiText, "\n  "+route.Path+":") {
			t.Fatalf("openapi missing path %s", route.Path)
		}
	}
}
