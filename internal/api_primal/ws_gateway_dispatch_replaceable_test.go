package api_primal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestWSGateway_ParameterizedReplaceableListRequiresIdentifier(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_missing_identifier", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey": "pk",
		"kind":   30023,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	_, noticeRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read notice frame: %v", err)
	}
	var notice []any
	if err := json.Unmarshal(noticeRaw, &notice); err != nil {
		t.Fatalf("decode notice frame: %v", err)
	}
	if len(notice) < 3 || notice[0] != "NOTICE" {
		t.Fatalf("expected NOTICE frame, got %s", string(noticeRaw))
	}
	if message, _ := notice[2].(string); message != "request failed" {
		t.Fatalf("unexpected notice message: %q", message)
	}
	_, eoseRaw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read eose frame: %v", err)
	}
	var eose []any
	if err := json.Unmarshal(eoseRaw, &eose); err != nil {
		t.Fatalf("decode eose frame: %v", err)
	}
	if len(eose) < 2 || eose[0] != "EOSE" {
		t.Fatalf("expected EOSE frame, got %s", string(eoseRaw))
	}
}

func TestWSGateway_ParameterizedReplaceableListAllowsEmptyIdentifier(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pk" || kind != 30000 || identifier != "" || limit != 1 {
				t.Fatalf("unexpected list lookup args pubkey=%s kind=%d identifier=%q limit=%d", pubkey, kind, identifier, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"empty_identifier_evt","kind":30000}`)}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_empty_identifier", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey":     "pk",
		"identifier": "",
		"limit":      1,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["empty_identifier_evt"] {
		t.Fatalf("expected empty-identifier list event in response, got ids=%#v", ids)
	}
}

func TestWSGateway_ParameterizedReplaceableListSupportsDTagAlias(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamListByIdentifierFn: func(_ context.Context, pubkey string, kind int, identifier string, limit int) ([]json.RawMessage, error) {
			if pubkey != "pk" || kind != 30000 || identifier != "from-d-tag" || limit != 1 {
				t.Fatalf("unexpected list lookup args pubkey=%s kind=%d identifier=%q limit=%d", pubkey, kind, identifier, limit)
			}
			return []json.RawMessage{json.RawMessage(`{"id":"d_tag_alias_evt","kind":30000}`)}, nil
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_param_list_d_tag", map[string]any{"cache": []any{"parameterized_replaceable_list", map[string]any{
		"pubkey": "pk",
		"d_tag":  "from-d-tag",
		"limit":  1,
	}}}}); err != nil {
		t.Fatalf("write parameterized_replaceable_list request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["d_tag_alias_evt"] {
		t.Fatalf("expected d_tag alias list event in response, got ids=%#v", ids)
	}
}

func TestWSGateway_ParametrizedReplaceableEventsSupportsEventsVector(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "pk_a" && kind == 30023 && dTag == "read-a":
				return json.RawMessage(`{"id":"pr_evt_a","kind":30023,"pubkey":"pk_a"}`), nil
			case pubkey == "pk_b" && kind == 30023 && dTag == "read-b":
				return json.RawMessage(`{"id":"pr_evt_b","kind":30023,"pubkey":"pk_b"}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_param_events_vector", map[string]any{"cache": []any{"parametrized_replaceable_events", map[string]any{
		"events": []any{
			map[string]any{"pubkey": "pk_a", "kind": 30023, "identifier": "read-a"},
			map[string]any{"pubkey": "pk_missing", "kind": 30023, "identifier": "read-missing"},
			map[string]any{"pubkey": "pk_b", "kind": 30023, "d_tag": "read-b"},
		},
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_events request: %v", err)
	}
	events := readThreadStreamUntilEOSE(t, conn)
	ids := extractEventIDsFromThreadStream(events)
	if !ids["pr_evt_a"] || !ids["pr_evt_b"] {
		t.Fatalf("missing expected parameterized events in payload: %#v", ids)
	}
	if ids["pr_evt_missing"] {
		t.Fatalf("unexpected missing parameterized event in payload: %#v", ids)
	}
}

func TestWSGateway_ParametrizedReplaceableEventSupportsIdentifierAndDTag(t *testing.T) {
	gateway := NewWSGateway(fakeEventReader{
		getParamEventFn: func(_ context.Context, pubkey string, kind int, dTag string) (json.RawMessage, error) {
			switch {
			case pubkey == "pk" && kind == 30023 && dTag == "from-identifier":
				return json.RawMessage(`{"id":"param_event_identifier","kind":30023}`), nil
			case pubkey == "pk" && kind == 30023 && dTag == "from-d-tag":
				return json.RawMessage(`{"id":"param_event_d_tag","kind":30023}`), nil
			default:
				return nil, store.ErrNotFound
			}
		},
	}, WSGatewayOptions{})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/ws", gateway.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/primal/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial ws: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON([]any{"REQ", "sub_param_event_identifier", map[string]any{"cache": []any{"parametrized_replaceable_event", map[string]any{
		"pubkey":     "pk",
		"kind":       30023,
		"identifier": "from-identifier",
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_event identifier request: %v", err)
	}
	identifierEvents := readThreadStreamUntilEOSE(t, conn)
	identifierIDs := extractEventIDsFromThreadStream(identifierEvents)
	if !identifierIDs["param_event_identifier"] {
		t.Fatalf("expected identifier event in response, got ids=%#v", identifierIDs)
	}

	if err := conn.WriteJSON([]any{"REQ", "sub_param_event_d_tag", map[string]any{"cache": []any{"parametrized_replaceable_event", map[string]any{
		"pubkey": "pk",
		"kind":   30023,
		"d_tag":  "from-d-tag",
	}}}}); err != nil {
		t.Fatalf("write parametrized_replaceable_event d_tag request: %v", err)
	}
	dTagEvents := readThreadStreamUntilEOSE(t, conn)
	dTagIDs := extractEventIDsFromThreadStream(dTagEvents)
	if !dTagIDs["param_event_d_tag"] {
		t.Fatalf("expected d_tag event in response, got ids=%#v", dTagIDs)
	}
}
