package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// fixtures carry the identifiers substituted into request shapes so the
// harness can run against any dataset (empty DBs still exercise the full
// read path and return an EOSE / 2xx / 404 quickly).
type fixtures struct {
	Pubkey  string
	EventID string
	Hashtag string
	Query   string
}

// wsRequest is a single Primal cache-protocol call. The wire frame sent is
// ["REQ", <subID>, {"cache": [verb, params]}].
type wsRequest struct {
	Name   string         `json:"name"`
	Verb   string         `json:"verb"`
	Params map[string]any `json:"params"`
}

// apiRequest is a single native HTTP call. Path may contain {pubkey}, {id},
// {eventId} or {hashtag} tokens that are substituted from fixtures.
type apiRequest struct {
	Name   string `json:"name"`
	Method string `json:"method"`
	Path   string `json:"path"`
}

type scenario struct {
	WS  []wsRequest  `json:"ws"`
	API []apiRequest `json:"api"`
}

// defaultScenario returns a representative, read-only mix of Primal cache verbs
// and native API reads. Verbs mirror the golden contract request shapes in
// internal/api_primal/testdata/primal_contracts and the WS dispatch registry.
func defaultScenario() scenario {
	return scenario{
		WS: []wsRequest{
			{Name: "net_stats", Verb: "net_stats", Params: map[string]any{}},
			{Name: "server_name", Verb: "server_name", Params: map[string]any{}},
			{Name: "user_profile", Verb: "user_profile", Params: map[string]any{"pubkey": "{pubkey}"}},
			{Name: "user_infos", Verb: "user_infos", Params: map[string]any{"pubkeys": []any{"{pubkey}"}}},
			{Name: "user_mentions", Verb: "user_mentions", Params: map[string]any{"pubkey": "{pubkey}", "limit": 20}},
			{Name: "user_followers", Verb: "user_followers", Params: map[string]any{"pubkey": "{pubkey}", "limit": 20}},
			{Name: "contact_list", Verb: "contact_list", Params: map[string]any{"pubkey": "{pubkey}"}},
			{Name: "feed", Verb: "feed", Params: map[string]any{"pubkey": "{pubkey}", "limit": 20}},
			{Name: "search", Verb: "search", Params: map[string]any{"query": "{query}", "limit": 20}},
			{Name: "events", Verb: "events", Params: map[string]any{"event_ids": []any{"{id}"}}},
			{Name: "thread_view", Verb: "thread_view", Params: map[string]any{"event_id": "{id}", "limit": 20}},
			{Name: "user_zaps", Verb: "user_zaps", Params: map[string]any{"pubkey": "{pubkey}", "limit": 20}},
			{Name: "get_bookmarks", Verb: "get_bookmarks", Params: map[string]any{"pubkey": "{pubkey}", "limit": 20}},
		},
		API: []apiRequest{
			{Name: "health", Method: "GET", Path: "/health"},
			{Name: "event_by_id", Method: "GET", Path: "/api/v1/events/{id}"},
			{Name: "profile_by_pubkey", Method: "GET", Path: "/api/v1/profiles/{pubkey}"},
			{Name: "thread", Method: "GET", Path: "/api/v1/threads/{id}"},
			{Name: "author_events", Method: "GET", Path: "/api/v1/authors/{pubkey}/events"},
			{Name: "followers", Method: "GET", Path: "/api/v1/users/{pubkey}/followers"},
			{Name: "trending_notes", Method: "GET", Path: "/api/v1/discovery/notes/trending"},
			{Name: "network_stats", Method: "GET", Path: "/api/v1/discovery/stats/network"},
			{Name: "search", Method: "GET", Path: "/api/v1/search?q={query}"},
			{Name: "trending_hashtags", Method: "GET", Path: "/api/v1/discovery/hashtags/trending"},
		},
	}
}

// loadScenario reads a scenario JSON file, falling back to the default for any
// channel the file leaves empty.
func loadScenario(path string) (scenario, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return scenario{}, fmt.Errorf("read scenario %s: %w", path, err)
	}
	var sc scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		return scenario{}, fmt.Errorf("parse scenario %s: %w", path, err)
	}
	def := defaultScenario()
	if len(sc.WS) == 0 {
		sc.WS = def.WS
	}
	if len(sc.API) == 0 {
		sc.API = def.API
	}
	return sc, nil
}

// substitute replaces fixture tokens in a string.
func substitute(s string, f fixtures) string {
	replacer := strings.NewReplacer(
		"{pubkey}", f.Pubkey,
		"{id}", f.EventID,
		"{eventId}", f.EventID,
		"{event_id}", f.EventID,
		"{hashtag}", f.Hashtag,
		"{query}", f.Query,
	)
	return replacer.Replace(s)
}

// resolvePath applies fixtures to an API path, URL-escaping the query value so
// arbitrary fixture text stays a valid request target.
func resolvePath(path string, f fixtures) string {
	escaped := f
	escaped.Query = url.QueryEscape(f.Query)
	return substitute(path, escaped)
}

// resolveParams returns a deep copy of a cache request's params with fixture
// tokens substituted in string and []any-of-string values.
func resolveParams(params map[string]any, f fixtures) map[string]any {
	out := make(map[string]any, len(params))
	for k, v := range params {
		switch typed := v.(type) {
		case string:
			out[k] = substitute(typed, f)
		case []any:
			list := make([]any, len(typed))
			for i, item := range typed {
				if s, ok := item.(string); ok {
					list[i] = substitute(s, f)
				} else {
					list[i] = item
				}
			}
			out[k] = list
		default:
			out[k] = v
		}
	}
	return out
}
