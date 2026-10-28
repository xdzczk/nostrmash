package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestBuildFilter_IncludesAuthorsWhenProvided(t *testing.T) {
	since := int64(100)
	until := int64(200)
	filter := buildFilter(PageRequest{
		Kinds:   []int{0, 3, 10002},
		Authors: []string{"pubkey-a"},
		Since:   &since,
		Until:   &until,
		Limit:   50,
	})

	authors, ok := filter["authors"].([]string)
	if !ok || len(authors) != 1 || authors[0] != "pubkey-a" {
		t.Fatalf("unexpected authors in filter: %#v", filter["authors"])
	}
	if _, ok := filter["since"].(int64); !ok {
		t.Fatalf("expected since in filter, got %#v", filter["since"])
	}
	if _, ok := filter["until"].(int64); !ok {
		t.Fatalf("expected until in filter, got %#v", filter["until"])
	}
}

func TestBuildFilter_OmitsAuthorsWhenEmpty(t *testing.T) {
	filter := buildFilter(PageRequest{
		Kinds: []int{1},
		Limit: 10,
	})
	if _, ok := filter["authors"]; ok {
		t.Fatalf("did not expect authors key: %#v", filter)
	}
}

func TestParseNostrEnvelope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		frame       string
		wantType    string
		wantSubID   string
		wantOK      bool
		wantPayload string
	}{
		{
			name:        "event envelope",
			frame:       `["EVENT","sub-1",{"id":"x","created_at":12}]`,
			wantType:    "EVENT",
			wantSubID:   "sub-1",
			wantOK:      true,
			wantPayload: `{"id":"x","created_at":12}`,
		},
		{
			name:      "eose envelope",
			frame:     `["EOSE","sub-1"]`,
			wantType:  "EOSE",
			wantSubID: "sub-1",
			wantOK:    true,
		},
		{
			name:   "notice ignored",
			frame:  `["NOTICE","oops"]`,
			wantOK: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotType, gotSubID, gotPayload, ok := parseNostrEnvelope([]byte(tc.frame))
			if ok != tc.wantOK {
				t.Fatalf("ok mismatch: got %v want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if gotType != tc.wantType {
				t.Fatalf("type mismatch: got %q want %q", gotType, tc.wantType)
			}
			if gotSubID != tc.wantSubID {
				t.Fatalf("sub id mismatch: got %q want %q", gotSubID, tc.wantSubID)
			}
			if string(gotPayload) != tc.wantPayload {
				t.Fatalf("payload mismatch: got %q want %q", string(gotPayload), tc.wantPayload)
			}
		})
	}
}

func TestFetchPageSilentRelayReturnsAfterIdle(t *testing.T) {
	url := startTestRelay(t, func(ctx context.Context, conn *websocket.Conn) {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		<-ctx.Done()
	})

	idle := 300 * time.Millisecond
	start := time.Now()
	result, err := (WebsocketFetcher{IdleTimeout: idle, ConnectTimeout: time.Second}).FetchPage(
		context.Background(),
		url,
		PageRequest{Kinds: []int{3, 10002}, Authors: []string{"aa"}, Limit: 2},
	)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if result.EOSESeen || len(result.Events) != 0 {
		t.Fatalf("expected an empty partial page, got %+v", result)
	}
	if elapsed < idle-50*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("idle wait took %s, want about %s", elapsed, idle)
	}
}

func TestFetchPageReturnsEventAndEOSE(t *testing.T) {
	url := startTestRelay(t, func(ctx context.Context, conn *websocket.Conn) {
		subID, err := readReqSubID(conn)
		if err != nil {
			return
		}
		event := `["EVENT",` + jsonString(subID) + `,{"id":"e1","kind":3}]`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
			return
		}
		eose := `["EOSE",` + jsonString(subID) + `]`
		_ = conn.WriteMessage(websocket.TextMessage, []byte(eose))
		_, _, _ = conn.ReadMessage()
		<-ctx.Done()
	})

	start := time.Now()
	result, err := (WebsocketFetcher{IdleTimeout: 2 * time.Second, ConnectTimeout: time.Second}).FetchPage(
		context.Background(),
		url,
		PageRequest{Kinds: []int{3}, Authors: []string{"aa"}, Limit: 2},
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !result.EOSESeen {
		t.Fatal("expected EOSE")
	}
	if len(result.Events) != 1 || !strings.Contains(string(result.Events[0]), `"e1"`) {
		t.Fatalf("events: %s", result.Events)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("EOSE took %s", time.Since(start))
	}
}

func TestFetchPageContextCancelUnblocksRead(t *testing.T) {
	url := startTestRelay(t, func(ctx context.Context, conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage()
		<-ctx.Done()
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err := (WebsocketFetcher{IdleTimeout: 5 * time.Second, ConnectTimeout: time.Second}).FetchPage(
		ctx,
		url,
		PageRequest{Kinds: []int{3}, Limit: 1},
	)
	elapsed := time.Since(start)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("cancel took %s", elapsed)
	}
}

func startTestRelay(t *testing.T, handle func(context.Context, *websocket.Conn)) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		handle(ctx, conn)
	}))
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func readReqSubID(conn *websocket.Conn) (string, error) {
	_, frame, err := conn.ReadMessage()
	if err != nil {
		return "", err
	}
	var env []json.RawMessage
	if err := json.Unmarshal(frame, &env); err != nil || len(env) < 2 {
		return "", errors.New("req envelope")
	}
	var subID string
	if err := json.Unmarshal(env[1], &subID); err != nil {
		return "", err
	}
	return subID, nil
}

func jsonString(s string) string {
	raw, _ := json.Marshal(s)
	return string(raw)
}
