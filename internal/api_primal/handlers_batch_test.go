package api_primal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xdzczk/nostrmash/internal/store"
)

func TestPrimalBatchUserInfosEndpoint(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{
				pubkeys[0]: {
					Pubkey:            pubkeys[0],
					MetadataEventID:   "evt_meta",
					MetadataCreatedAt: 1700000001,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				},
			}, nil
		},
	}, 10)
	req := httptest.NewRequest(http.MethodPost, "/primal/v1/user_infos", strings.NewReader(`{"pubkeys":["pk1","pk2"]}`))
	rec := httptest.NewRecorder()
	handlers.BatchGetUserInfos(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got=%d want=%d", rec.Code, http.StatusOK)
	}
	var resp struct {
		Profiles       []any    `json:"profiles"`
		MissingPubkeys []string `json:"missing_pubkeys"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Profiles) != 1 || len(resp.MissingPubkeys) != 1 || resp.MissingPubkeys[0] != "pk2" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}
func TestPrimalBatchEndpoints_RejectOversizedPayloads(t *testing.T) {
	handlers := mustNewHandlers(t, fakeEventReader{}, 10)

	tooLargeIDs := bytes.Repeat([]byte("a"), int(publicBatchBodyLimitBytes+10))
	eventsBody := []byte(`{"event_ids":["` + string(tooLargeIDs) + `"]}`)
	eventsReq := httptest.NewRequest(http.MethodPost, "/primal/v1/events/batch", bytes.NewReader(eventsBody))
	eventsRec := httptest.NewRecorder()
	handlers.BatchGetEvents(eventsRec, eventsReq)
	if eventsRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected events status: got=%d want=%d", eventsRec.Code, http.StatusRequestEntityTooLarge)
	}

	tooLargePubkeys := bytes.Repeat([]byte("b"), int(publicBatchBodyLimitBytes+10))
	userInfosBody := []byte(`{"pubkeys":["` + string(tooLargePubkeys) + `"]}`)
	userInfosReq := httptest.NewRequest(http.MethodPost, "/primal/v1/user_infos", bytes.NewReader(userInfosBody))
	userInfosRec := httptest.NewRecorder()
	handlers.BatchGetUserInfos(userInfosRec, userInfosReq)
	if userInfosRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected user infos status: got=%d want=%d", userInfosRec.Code, http.StatusRequestEntityTooLarge)
	}
}
