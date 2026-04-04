package api_primal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/store"
)

type fakeEventReader struct {
	getEventRawByIDFn  func(context.Context, string) (json.RawMessage, error)
	getEventRawsByIDs  func(context.Context, []string) (map[string]json.RawMessage, error)
	getProfileByPubkey func(context.Context, string) (store.ProfileProjection, error)
}

func (f fakeEventReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawByIDFn(ctx, id)
}

func (f fakeEventReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDs == nil {
		return nil, errors.New("not implemented")
	}
	return f.getEventRawsByIDs(ctx, ids)
}

func (f fakeEventReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkey == nil {
		return store.ProfileProjection{}, errors.New("not implemented")
	}
	return f.getProfileByPubkey(ctx, pubkey)
}

func TestGetEventByID_GoldenResponse(t *testing.T) {
	storeRaw := mustReadJSON(t, "fixtures/event_store_raw.json")
	golden := mustReadJSON(t, "golden/get_event_success.json")

	handlers := NewHandlers(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			if id != "evt_123" {
				t.Fatalf("unexpected id: %s", id)
			}
			return storeRaw, nil
		},
	}, 10)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/v1/events/{id}", handlers.GetEventByID)

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/events/evt_123", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	assertJSONEqual(t, rec.Body.Bytes(), golden)
}

func TestBatchGetEvents_GoldenResponse(t *testing.T) {
	requestBody := mustReadText(t, "fixtures/batch_events_request_event_ids.json")
	golden := mustReadJSON(t, "golden/batch_events_success.json")

	handlers := NewHandlers(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			if len(ids) != 3 || ids[0] != "evt_1" || ids[1] != "evt_2" || ids[2] != "evt_3" {
				t.Fatalf("unexpected ids: %#v", ids)
			}
			return map[string]json.RawMessage{
				"evt_1": mustReadJSON(t, "fixtures/event_store_raw.json"),
				"evt_3": json.RawMessage(`{"id":"evt_3","kind":7,"content":"note-3"}`),
			}, nil
		},
	}, 10)

	req := httptest.NewRequest(http.MethodPost, "/primal/v1/events/batch", requestBody)
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	assertJSONEqual(t, rec.Body.Bytes(), golden)
}

func TestBatchGetEvents_AcceptsIDsAliasQuirk(t *testing.T) {
	requestBody := mustReadText(t, "fixtures/batch_events_request_ids_alias.json")
	golden := mustReadJSON(t, "golden/batch_events_success.json")

	handlers := NewHandlers(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			if len(ids) != 3 || ids[0] != "evt_1" || ids[1] != "evt_2" || ids[2] != "evt_3" {
				t.Fatalf("unexpected ids: %#v", ids)
			}
			return map[string]json.RawMessage{
				"evt_1": mustReadJSON(t, "fixtures/event_store_raw.json"),
				"evt_3": json.RawMessage(`{"id":"evt_3","kind":7,"content":"note-3"}`),
			}, nil
		},
	}, 10)

	req := httptest.NewRequest(http.MethodPost, "/primal/v1/events/batch", requestBody)
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	assertJSONEqual(t, rec.Body.Bytes(), golden)
}

func TestGetProfileByPubkey_GoldenResponse(t *testing.T) {
	profileJSON := mustReadJSON(t, "fixtures/profile_store_profile.json")
	golden := mustReadJSON(t, "golden/get_profile_success.json")

	handlers := NewHandlers(fakeEventReader{
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			if pubkey != "pubkey_abc" {
				t.Fatalf("unexpected pubkey: %s", pubkey)
			}
			return store.ProfileProjection{
				Pubkey:            "pubkey_abc",
				MetadataEventID:   "evt_meta_1",
				MetadataCreatedAt: 1700000001,
				ProfileJSON:       profileJSON,
			}, nil
		},
	}, 10)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/v1/profiles/{pubkey}", handlers.GetProfileByPubkey)

	req := httptest.NewRequest(http.MethodGet, "/primal/v1/profiles/pubkey_abc", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	assertJSONEqual(t, rec.Body.Bytes(), golden)
}

func TestBatchGetEvents_InvalidRequest_GoldenError(t *testing.T) {
	requestBody := mustReadText(t, "fixtures/batch_events_request_empty.json")
	golden := mustReadJSON(t, "golden/batch_events_invalid_request.json")

	handlers := NewHandlers(fakeEventReader{}, 10)
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/events/batch", requestBody)
	req = req.WithContext(logging.ContextWithRequestID(req.Context(), "req-fixed-1"))
	rec := httptest.NewRecorder()
	handlers.BatchGetEvents(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusBadRequest)
	}
	assertJSONEqual(t, rec.Body.Bytes(), golden)
}

func mustReadText(t *testing.T, relativePath string) io.Reader {
	t.Helper()
	path := filepath.Join("testdata", relativePath)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.NewReader(string(raw))
}

func mustReadJSON(t *testing.T, relativePath string) json.RawMessage {
	t.Helper()
	path := filepath.Join("testdata", relativePath)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return json.RawMessage(raw)
}

func assertJSONEqual(t *testing.T, got []byte, want []byte) {
	t.Helper()
	var gotValue any
	var wantValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got json: %v\nraw=%s", err, string(got))
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want json: %v\nraw=%s", err, string(want))
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("json mismatch\ngot=%s\nwant=%s", string(got), string(want))
	}
}
