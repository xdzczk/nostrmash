package api_primal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

var update = flag.Bool("update", false, "update golden contract fixtures")

type fakeEventReader struct {
	getEventRawByIDFn  func(context.Context, string) (json.RawMessage, error)
	getEventRawsByIDs  func(context.Context, []string) (map[string]json.RawMessage, error)
	getProfileByPubkey func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByBatch func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getAuthorEventsFn  func(context.Context, string, int) ([]json.RawMessage, error)
	getAuthorRepliesFn func(context.Context, string, int) ([]json.RawMessage, error)
	getEventCountsFn   func(context.Context, string) (store.EventCounts, error)
	getEventRepliesFn  func(context.Context, string, int, *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error)
	getEventAncestors  func(context.Context, string, int) ([]json.RawMessage, []string, error)
	getContactListFn   func(context.Context, string) (store.ContactListProjection, error)
	getRelayListFn     func(context.Context, string) (store.RelayListProjection, error)
	searchEventsFn     func(context.Context, string, int) ([]json.RawMessage, error)
	searchProfilesFn   func(context.Context, string, int) ([]store.ProfileProjection, error)
	getByKindPubkeyFn  func(context.Context, int, string, int) ([]json.RawMessage, error)
	getRefsPubkeyFn    func(context.Context, string, int) ([]json.RawMessage, error)
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

func (f fakeEventReader) GetEventWithProvenance(context.Context, string) (store.EventWithProvenance, error) {
	return store.EventWithProvenance{}, errors.New("not implemented")
}

func (f fakeEventReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) GetProfileByPubkey(ctx context.Context, pubkey string) (store.ProfileProjection, error) {
	if f.getProfileByPubkey == nil {
		return store.ProfileProjection{}, errors.New("not implemented")
	}
	return f.getProfileByPubkey(ctx, pubkey)
}

func (f fakeEventReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
	if f.getProfilesByBatch == nil {
		return nil, errors.New("not implemented")
	}
	return f.getProfilesByBatch(ctx, pubkeys)
}

func (f fakeEventReader) GetAuthorRecentEvents(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorEventsFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetAuthorReplies(ctx context.Context, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getAuthorRepliesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getAuthorRepliesFn(ctx, pubkey, limit)
}

func (f fakeEventReader) GetEventCounts(ctx context.Context, eventID string) (store.EventCounts, error) {
	if f.getEventCountsFn == nil {
		return store.EventCounts{}, errors.New("not implemented")
	}
	return f.getEventCountsFn(ctx, eventID)
}

func (f fakeEventReader) GetEventReplies(
	ctx context.Context,
	eventID string,
	limit int,
	cursor *store.EventOrderCursor,
) ([]json.RawMessage, *store.EventOrderCursor, error) {
	if f.getEventRepliesFn == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventRepliesFn(ctx, eventID, limit, cursor)
}

func (f fakeEventReader) GetEventAncestors(ctx context.Context, eventID string, maxDepth int) ([]json.RawMessage, []string, error) {
	if f.getEventAncestors == nil {
		return nil, nil, errors.New("not implemented")
	}
	return f.getEventAncestors(ctx, eventID, maxDepth)
}

func (f fakeEventReader) ListRelayHealth(context.Context) ([]model.IngestCheckpoint, error) {
	return nil, errors.New("not implemented")
}

func (f fakeEventReader) GetContactListByPubkey(ctx context.Context, pubkey string) (store.ContactListProjection, error) {
	if f.getContactListFn == nil {
		return store.ContactListProjection{}, errors.New("not implemented")
	}
	return f.getContactListFn(ctx, pubkey)
}

func (f fakeEventReader) GetRelayListByPubkey(ctx context.Context, pubkey string) (store.RelayListProjection, error) {
	if f.getRelayListFn == nil {
		return store.RelayListProjection{}, errors.New("not implemented")
	}
	return f.getRelayListFn(ctx, pubkey)
}

func (f fakeEventReader) SearchEventsByContent(ctx context.Context, query string, limit int) ([]json.RawMessage, error) {
	if f.searchEventsFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchEventsFn(ctx, query, limit)
}

func (f fakeEventReader) SearchProfiles(ctx context.Context, query string, limit int) ([]store.ProfileProjection, error) {
	if f.searchProfilesFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.searchProfilesFn(ctx, query, limit)
}

func (f fakeEventReader) GetRecentEventsByKindAndPubkey(ctx context.Context, kind int, pubkey string, limit int) ([]json.RawMessage, error) {
	if f.getByKindPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getByKindPubkeyFn(ctx, kind, pubkey, limit)
}

func (f fakeEventReader) GetEventsReferencingPubkey(ctx context.Context, targetPubkey string, limit int) ([]json.RawMessage, error) {
	if f.getRefsPubkeyFn == nil {
		return nil, errors.New("not implemented")
	}
	return f.getRefsPubkeyFn(ctx, targetPubkey, limit)
}

func TestPrimalContracts(t *testing.T) {
	root := filepath.Join("testdata", "primal_contracts")
	endpoints, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read contracts root: %v", err)
	}
	for _, endpoint := range endpoints {
		if !endpoint.IsDir() {
			continue
		}
		endpointName := endpoint.Name()
		endpointPath := filepath.Join(root, endpointName)
		cases, err := os.ReadDir(endpointPath)
		if err != nil {
			t.Fatalf("read endpoint dir %s: %v", endpointName, err)
		}
		for _, tc := range cases {
			if !tc.IsDir() {
				continue
			}
			caseName := tc.Name()
			casePath := filepath.Join(endpointPath, caseName)
			t.Run(endpointName+"_"+caseName, func(t *testing.T) {
				runPrimalContractCase(t, casePath)
			})
		}
	}
}

type contractRequest struct {
	Method       string          `json:"method"`
	Path         string          `json:"path"`
	Body         json.RawMessage `json:"body,omitempty"`
	BodyText     string          `json:"body_text,omitempty"`
	MaxBatchSize int             `json:"max_batch_size,omitempty"`
	RequestID    string          `json:"request_id,omitempty"`
}

type contractResponse struct {
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

func runPrimalContractCase(t *testing.T, casePath string) {
	t.Helper()
	reqFixture := readJSONFile[contractRequest](t, filepath.Join(casePath, "request.json"))
	if reqFixture.MaxBatchSize <= 0 {
		reqFixture.MaxBatchSize = 10
	}

	eventRaw := mustReadJSON(t, "fixtures/event_store_raw.json")
	profileRaw := mustReadJSON(t, "fixtures/profile_store_profile.json")
	handlers := NewHandlers(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			switch id {
			case "evt_123":
				return eventRaw, nil
			case "evt_missing":
				return nil, store.ErrNotFound
			case "evt_store_err":
				return nil, errors.New("storage down")
			default:
				return nil, store.ErrNotFound
			}
		},
		getEventRawsByIDs: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
			for _, id := range ids {
				if id == "evt_store_err" {
					return nil, errors.New("storage down")
				}
			}
			out := make(map[string]json.RawMessage)
			for _, id := range ids {
				switch id {
				case "evt_1":
					out[id] = eventRaw
				case "evt_3":
					out[id] = json.RawMessage(`{"id":"evt_3","kind":7,"content":"note-3"}`)
				}
			}
			return out, nil
		},
		getProfileByPubkey: func(_ context.Context, pubkey string) (store.ProfileProjection, error) {
			switch pubkey {
			case "pubkey_abc":
				return store.ProfileProjection{
					Pubkey:            "pubkey_abc",
					MetadataEventID:   "evt_meta_1",
					MetadataCreatedAt: 1700000001,
					ProfileJSON:       profileRaw,
				}, nil
			case "pubkey_missing":
				return store.ProfileProjection{}, store.ErrNotFound
			case "pubkey_store_err":
				return store.ProfileProjection{}, errors.New("storage down")
			default:
				return store.ProfileProjection{}, store.ErrNotFound
			}
		},
		getProfilesByBatch: func(_ context.Context, pubkeys []string) (map[string]store.ProfileProjection, error) {
			out := make(map[string]store.ProfileProjection)
			for _, pubkey := range pubkeys {
				if pubkey == "pubkey_store_err" {
					return nil, errors.New("storage down")
				}
				switch pubkey {
				case "pubkey_abc":
					out[pubkey] = store.ProfileProjection{
						Pubkey:            "pubkey_abc",
						MetadataEventID:   "evt_meta_1",
						MetadataCreatedAt: 1700000001,
						ProfileJSON:       profileRaw,
					}
				case "pubkey_xyz":
					out[pubkey] = store.ProfileProjection{
						Pubkey:            "pubkey_xyz",
						MetadataEventID:   "evt_meta_2",
						MetadataCreatedAt: 1700000500,
						ProfileJSON:       json.RawMessage(`{"name":"XYZ"}`),
					}
				}
			}
			return out, nil
		},
		getAuthorEventsFn: func(_ context.Context, pubkey string, _ int) ([]json.RawMessage, error) {
			if pubkey == "pubkey_store_err" {
				return nil, errors.New("storage down")
			}
			if pubkey == "pubkey_abc" {
				return []json.RawMessage{
					json.RawMessage(`{"id":"auth_evt_2","kind":1,"pubkey":"pubkey_abc","created_at":1700000002}`),
					json.RawMessage(`{"id":"auth_evt_1","kind":1,"pubkey":"pubkey_abc","created_at":1700000001}`),
				}, nil
			}
			return []json.RawMessage{}, nil
		},
		getAuthorRepliesFn: func(_ context.Context, pubkey string, _ int) ([]json.RawMessage, error) {
			if pubkey == "pubkey_store_err" {
				return nil, errors.New("storage down")
			}
			if pubkey == "pubkey_abc" {
				return []json.RawMessage{
					json.RawMessage(`{"id":"reply_evt_1","kind":1,"pubkey":"pubkey_abc","created_at":1700000010}`),
				}, nil
			}
			return []json.RawMessage{}, nil
		},
		getEventCountsFn: func(_ context.Context, eventID string) (store.EventCounts, error) {
			if eventID == "evt_store_err" {
				return store.EventCounts{}, errors.New("storage down")
			}
			return store.EventCounts{
				EventID:       eventID,
				ReplyCount:    2,
				ReactionCount: 3,
				RepostCount:   1,
				Consistency:   "eventual",
			}, nil
		},
		getEventRepliesFn: func(_ context.Context, eventID string, _ int, _ *store.EventOrderCursor) ([]json.RawMessage, *store.EventOrderCursor, error) {
			if eventID == "evt_store_err" {
				return nil, nil, errors.New("storage down")
			}
			if eventID == "evt_missing" {
				return nil, nil, store.ErrNotFound
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"thread_reply_1","kind":1,"pubkey":"pubkey_b","created_at":1700000020}`),
			}, nil, nil
		},
		getEventAncestors: func(_ context.Context, eventID string, _ int) ([]json.RawMessage, []string, error) {
			if eventID == "evt_store_err" {
				return nil, nil, errors.New("storage down")
			}
			if eventID == "evt_missing" {
				return nil, nil, store.ErrNotFound
			}
			return []json.RawMessage{
				json.RawMessage(`{"id":"root_evt","kind":1,"pubkey":"pubkey_root","created_at":1699999990}`),
			}, []string{"missing_ancestor_1"}, nil
		},
		getContactListFn: func(_ context.Context, pubkey string) (store.ContactListProjection, error) {
			switch pubkey {
			case "pubkey_abc":
				return store.ContactListProjection{
					Pubkey:          "pubkey_abc",
					EventID:         "contact_evt_1",
					CreatedAt:       1700000100,
					DerivationVer:   1,
					ContactsJSONRaw: json.RawMessage(`["pubkey_x","pubkey_y"]`),
				}, nil
			case "pubkey_store_err":
				return store.ContactListProjection{}, errors.New("storage down")
			default:
				return store.ContactListProjection{}, store.ErrNotFound
			}
		},
		getRelayListFn: func(_ context.Context, pubkey string) (store.RelayListProjection, error) {
			switch pubkey {
			case "pubkey_abc":
				return store.RelayListProjection{
					Pubkey:        "pubkey_abc",
					EventID:       "relay_evt_1",
					CreatedAt:     1700000200,
					DerivationVer: 1,
					RelaysJSONRaw: json.RawMessage(`["wss://relay.primal.net","wss://nos.lol"]`),
				}, nil
			case "pubkey_store_err":
				return store.RelayListProjection{}, errors.New("storage down")
			default:
				return store.RelayListProjection{}, store.ErrNotFound
			}
		},
	}, reqFixture.MaxBatchSize)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /primal/v1/events/{id}", handlers.GetEventByID)
	mux.HandleFunc("POST /primal/v1/events/batch", handlers.BatchGetEvents)
	mux.HandleFunc("GET /primal/v1/profiles/{pubkey}", handlers.GetProfileByPubkey)
	mux.HandleFunc("POST /primal/v1/user_infos", handlers.BatchGetUserInfos)
	mux.HandleFunc("GET /primal/v1/threads/{eventId}", handlers.GetThreadView)
	mux.HandleFunc("GET /primal/v1/authors/{pubkey}/events", handlers.GetAuthorEvents)
	mux.HandleFunc("GET /primal/v1/authors/{pubkey}/replies", handlers.GetAuthorReplies)
	mux.HandleFunc("GET /primal/v1/events/{id}/actions", handlers.GetEventActions)
	mux.HandleFunc("GET /primal/v1/contact-lists/{pubkey}", handlers.GetContactList)
	mux.HandleFunc("GET /primal/v1/relay-lists/{pubkey}", handlers.GetRelayList)

	reqBody := bytes.NewReader(nil)
	if reqFixture.BodyText != "" {
		reqBody = bytes.NewReader([]byte(reqFixture.BodyText))
	} else if len(reqFixture.Body) > 0 {
		reqBody = bytes.NewReader(reqFixture.Body)
	}
	req := httptest.NewRequest(reqFixture.Method, reqFixture.Path, reqBody)
	if reqFixture.RequestID != "" {
		req = req.WithContext(logging.ContextWithRequestID(req.Context(), reqFixture.RequestID))
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	actualBody, err := json.Marshal(contractResponse{
		Status: rec.Code,
		Body:   json.RawMessage(bytes.TrimSpace(rec.Body.Bytes())),
	})
	if err != nil {
		t.Fatalf("marshal actual response: %v", err)
	}
	goldenPath := filepath.Join(casePath, "response.golden.json")
	if *update {
		writeFormattedJSON(t, goldenPath, actualBody)
	}
	want := mustReadJSONPath(t, goldenPath)
	assertJSONEqual(t, actualBody, want)
}

func readJSONFile[T any](t *testing.T, path string) T {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return out
}

func mustReadJSON(t *testing.T, relativePath string) json.RawMessage {
	t.Helper()
	path := filepath.Join("testdata", relativePath)
	return mustReadJSONPath(t, path)
}

func mustReadJSONPath(t *testing.T, path string) json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return json.RawMessage(raw)
}

func writeFormattedJSON(t *testing.T, path string, value []byte) {
	t.Helper()
	var decoded any
	if err := json.Unmarshal(value, &decoded); err != nil {
		t.Fatalf("unmarshal generated json for %s: %v", path, err)
	}
	pretty, err := json.MarshalIndent(decoded, "", "  ")
	if err != nil {
		t.Fatalf("marshal pretty json for %s: %v", path, err)
	}
	pretty = append(pretty, '\n')
	if err := os.WriteFile(path, pretty, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
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
		t.Fatalf("json mismatch\n%s", formatJSONDiff(gotValue, wantValue))
	}
}

func formatJSONDiff(got any, want any) string {
	gotPretty, _ := json.MarshalIndent(got, "", "  ")
	wantPretty, _ := json.MarshalIndent(want, "", "  ")
	return fmt.Sprintf("got=%s\nwant=%s", gotPretty, wantPretty)
}
