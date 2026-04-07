package api_primal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/xdzczk/nostrmash/internal/logging"
	"github.com/xdzczk/nostrmash/internal/store"
)

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
	const dmReceiver = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const dmPeer = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const dmPeerAlt = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	eventRaw := mustReadJSON(t, "fixtures/event_store_raw.json")
	profileRaw := mustReadJSON(t, "fixtures/profile_store_profile.json")
	handlers := NewHandlers(fakeEventReader{
		getEventRawByIDFn: func(_ context.Context, id string) (json.RawMessage, error) {
			switch id {
			case "evt_123":
				return eventRaw, nil
			case "evt_cursor":
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
				case "md_receiver":
					out[id] = json.RawMessage(`{"id":"md_receiver","kind":0,"pubkey":"` + dmReceiver + `","content":"{\"name\":\"Receiver\"}"}`)
				case "md_peer":
					out[id] = json.RawMessage(`{"id":"md_peer","kind":0,"pubkey":"` + dmPeer + `","content":"{\"name\":\"Peer\"}"}`)
				case "md_peer_alt":
					out[id] = json.RawMessage(`{"id":"md_peer_alt","kind":0,"pubkey":"` + dmPeerAlt + `","content":"{\"name\":\"Peer Alt\"}"}`)
				case "dm_latest_1":
					out[id] = json.RawMessage(`{"id":"dm_latest_1","kind":4,"pubkey":"` + dmPeer + `","created_at":1700000030}`)
				case "dm_latest_2":
					out[id] = json.RawMessage(`{"id":"dm_latest_2","kind":4,"pubkey":"` + dmPeerAlt + `","created_at":1700000020}`)
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
				case dmReceiver:
					out[pubkey] = store.ProfileProjection{
						Pubkey:            dmReceiver,
						MetadataEventID:   "md_receiver",
						MetadataCreatedAt: 1700000005,
						ProfileJSON:       json.RawMessage(`{"name":"Receiver"}`),
					}
				case dmPeer:
					out[pubkey] = store.ProfileProjection{
						Pubkey:            dmPeer,
						MetadataEventID:   "md_peer",
						MetadataCreatedAt: 1700000006,
						ProfileJSON:       json.RawMessage(`{"name":"Peer"}`),
					}
				case dmPeerAlt:
					out[pubkey] = store.ProfileProjection{
						Pubkey:            dmPeerAlt,
						MetadataEventID:   "md_peer_alt",
						MetadataCreatedAt: 1700000007,
						ProfileJSON:       json.RawMessage(`{"name":"Peer Alt"}`),
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
			if eventID == "evt_cursor" {
				return []json.RawMessage{
						json.RawMessage(`{"id":"thread_reply_1","kind":1,"pubkey":"pubkey_b","created_at":1700000020}`),
					}, &store.EventOrderCursor{
						CreatedAt: 1700000020,
						ID:        "thread_reply_1",
					}, nil
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
		getDirectMsgsRangeFn: func(_ context.Context, pubkey string, peer string, since int64, until int64, limit int, offset int) ([]json.RawMessage, error) {
			if pubkey == "pubkey_store_err" || peer == "pubkey_store_err" {
				return nil, errors.New("storage down")
			}
			if pubkey == dmReceiver && peer == dmPeer && since == 0 && until == 1700000100 && limit == 2 && offset == 0 {
				return []json.RawMessage{
					json.RawMessage(`{"id":"dm_2","kind":4,"pubkey":"` + dmPeer + `","created_at":20}`),
					json.RawMessage(`{"id":"dm_1","kind":4,"pubkey":"` + dmReceiver + `","created_at":10}`),
				}, nil
			}
			return []json.RawMessage{}, nil
		},
		getDMContactsDetailedFn: func(_ context.Context, receiver string, limit int, offset int, since int64, until int64) ([]json.RawMessage, error) {
			if receiver == "pubkey_store_err" {
				return nil, errors.New("storage down")
			}
			if receiver == dmReceiver && limit == 2 && offset == 0 && since == 0 && until == 1700000100 {
				return []json.RawMessage{
					json.RawMessage(`{"peer_pubkey":"` + dmPeer + `","cnt":3,"latest_at":30,"latest_event_id":"dm_latest_1"}`),
					json.RawMessage(`{"peer_pubkey":"` + dmPeerAlt + `","cnt":1,"latest_at":20,"latest_event_id":"dm_latest_2"}`),
				}, nil
			}
			return []json.RawMessage{}, nil
		},
		getDMCountFn: func(_ context.Context, receiver string, sender string) (int64, error) {
			if receiver == "pubkey_store_err" || sender == "pubkey_store_err" {
				return 0, errors.New("storage down")
			}
			if receiver == dmReceiver && sender == dmPeer {
				return 7, nil
			}
			if receiver == dmReceiver && sender == "" {
				return 8, nil
			}
			return 0, nil
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
	mux.HandleFunc("POST /primal/v1/dms/messages", handlers.PostDirectMessages)
	mux.HandleFunc("POST /primal/v1/dms/contacts", handlers.PostDirectMessageContacts)
	mux.HandleFunc("POST /primal/v1/dms/count", handlers.PostDirectMessageCount)
	mux.HandleFunc("POST /primal/v1/dms/count2", handlers.PostDirectMessageCount2)
	mux.HandleFunc("POST /primal/v1/dms/reset-count", handlers.PostResetDirectMessageCount)
	mux.HandleFunc("POST /primal/v1/dms/reset-counts", handlers.PostResetDirectMessageCounts)

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
