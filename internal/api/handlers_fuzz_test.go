package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func FuzzDecodeEventCursor(f *testing.F) {
	f.Add("")
	f.Add("not-base64")
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":1,"id":"evt_1"}`)))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":1,"id":"   "}`)))
	f.Add(base64.RawURLEncoding.EncodeToString([]byte(`{"created_at":"x","id":7}`)))

	f.Fuzz(func(t *testing.T, raw string) {
		cursor, err := decodeEventCursor(raw)
		if err != nil {
			return
		}
		if cursor == nil {
			return
		}
		if strings.TrimSpace(cursor.ID) == "" {
			t.Fatalf("decoded cursor ID must be non-empty: %#v", cursor)
		}

		encoded, err := encodeEventCursor(cursor)
		if err != nil {
			t.Fatalf("encode decoded cursor: %v", err)
		}
		roundTrip, err := decodeEventCursor(encoded)
		if err != nil {
			t.Fatalf("round-trip decode failed: %v", err)
		}
		if roundTrip == nil || roundTrip.ID != cursor.ID || roundTrip.CreatedAt != cursor.CreatedAt {
			t.Fatalf("unexpected round-trip cursor: got=%#v want=%#v", roundTrip, cursor)
		}
	})
}

func FuzzBatchGetEventsRequestDecoder(f *testing.F) {
	f.Add([]byte(`{"ids":["evt_1","evt_2"]}`))
	f.Add([]byte(`{"ids":[]}`))
	f.Add([]byte(`{"ids":[1,true,{"x":"y"}]}`))
	f.Add([]byte(`{"ids":"not-array"}`))
	f.Add([]byte(`{"ids":["evt_1"]} trailing`))

	handlers := NewHandlers(fakeEventReader{
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			out := make(map[string]json.RawMessage, len(ids))
			for _, id := range ids {
				out[id] = json.RawMessage(`{"id":"evt"}`)
			}
			return out, nil
		},
	}, 200)

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/events/batch", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handlers.BatchGetEvents(rec, req)
		if rec.Code < 100 || rec.Code > 599 {
			t.Fatalf("invalid HTTP status: %d", rec.Code)
		}
	})
}

func FuzzBatchGetProfilesRequestDecoder(f *testing.F) {
	f.Add([]byte(`{"pubkeys":["pk1","pk2"]}`))
	f.Add([]byte(`{"pubkeys":[1,false,null]}`))
	f.Add([]byte(`{"pubkeys":"not-array"}`))
	f.Add([]byte(`{"pubkeys":["pk1"]} x`))

	handlers := NewHandlers(fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := make(map[string]store.ProfileProjection, len(pubkeys))
			for _, pubkey := range pubkeys {
				out[pubkey] = store.ProfileProjection{Pubkey: pubkey}
			}
			return out, nil
		},
	}, 200)

	f.Fuzz(func(t *testing.T, body []byte) {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/profiles/batch", bytes.NewReader(body))
		rec := httptest.NewRecorder()
		handlers.BatchGetProfiles(rec, req)
		if rec.Code < 100 || rec.Code > 599 {
			t.Fatalf("invalid HTTP status: %d", rec.Code)
		}
	})
}
