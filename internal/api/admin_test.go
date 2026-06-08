package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

type fakeAdminService struct {
	getRelaysFn              func(context.Context) ([]adminRelayState, error)
	getRelaySuggestionsFn    func(context.Context, int, bool) ([]adminRelaySuggestion, error)
	getJobsFn                func(context.Context, int) (adminJobsResponse, error)
	getInvalidEventsFn       func(context.Context, int) (adminInvalidEventsResponse, error)
	getProjectionStatusFn    func(context.Context) (adminProjectionStatusResponse, error)
	getDiscoveryStatusFn     func(context.Context) (adminDiscoveryStatusResponse, error)
	getSearchStatusFn        func(context.Context) (adminSearchStatusResponse, error)
	triggerMeilisearchSyncFn func(context.Context, int) (adminMeilisearchSyncResponse, error)
	getRebuildsFn            func(context.Context, int) ([]adminRebuildRunResponse, error)
	triggerRebuildFn         func(context.Context, derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error)
	getStorageFn             func(context.Context) (adminStorageResponse, error)
	getSystemFn              func(context.Context) (adminSystemResponse, error)
	getDerivationVersionsFn  func(context.Context) ([]adminDerivationVersionResponse, error)
	getTrustRunsFn           func(context.Context, int) ([]adminTrustRunResponse, error)
	getTrustRunFn            func(context.Context, int64) (adminTrustRunResponse, error)
	triggerTrustRunFn        func(context.Context) (adminTrustRunResponse, error)
	getTopTrustScoresFn      func(context.Context, int) ([]adminTrustScoreResponse, error)
}

func (f fakeAdminService) GetRelays(ctx context.Context) ([]adminRelayState, error) {
	return f.getRelaysFn(ctx)
}
func (f fakeAdminService) GetRelaySuggestions(ctx context.Context, limit int, recommendedOnly bool) ([]adminRelaySuggestion, error) {
	if f.getRelaySuggestionsFn == nil {
		return []adminRelaySuggestion{}, nil
	}
	return f.getRelaySuggestionsFn(ctx, limit, recommendedOnly)
}
func (f fakeAdminService) GetJobs(ctx context.Context, limit int) (adminJobsResponse, error) {
	return f.getJobsFn(ctx, limit)
}
func (f fakeAdminService) GetInvalidEvents(ctx context.Context, limit int) (adminInvalidEventsResponse, error) {
	return f.getInvalidEventsFn(ctx, limit)
}
func (f fakeAdminService) GetProjectionStatus(ctx context.Context) (adminProjectionStatusResponse, error) {
	if f.getProjectionStatusFn == nil {
		return adminProjectionStatusResponse{}, nil
	}
	return f.getProjectionStatusFn(ctx)
}
func (f fakeAdminService) GetDiscoveryStatus(ctx context.Context) (adminDiscoveryStatusResponse, error) {
	if f.getDiscoveryStatusFn == nil {
		return adminDiscoveryStatusResponse{}, nil
	}
	return f.getDiscoveryStatusFn(ctx)
}
func (f fakeAdminService) GetSearchStatus(ctx context.Context) (adminSearchStatusResponse, error) {
	if f.getSearchStatusFn == nil {
		return adminSearchStatusResponse{}, nil
	}
	return f.getSearchStatusFn(ctx)
}
func (f fakeAdminService) TriggerMeilisearchSync(ctx context.Context, batchSize int) (adminMeilisearchSyncResponse, error) {
	if f.triggerMeilisearchSyncFn == nil {
		return adminMeilisearchSyncResponse{}, nil
	}
	return f.triggerMeilisearchSyncFn(ctx, batchSize)
}
func (f fakeAdminService) GetRebuilds(ctx context.Context, limit int) ([]adminRebuildRunResponse, error) {
	return f.getRebuildsFn(ctx, limit)
}
func (f fakeAdminService) TriggerRebuild(ctx context.Context, p derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error) {
	return f.triggerRebuildFn(ctx, p)
}
func (f fakeAdminService) GetStorage(ctx context.Context) (adminStorageResponse, error) {
	return f.getStorageFn(ctx)
}
func (f fakeAdminService) GetSystem(ctx context.Context) (adminSystemResponse, error) {
	return f.getSystemFn(ctx)
}
func (f fakeAdminService) GetDerivationVersions(ctx context.Context) ([]adminDerivationVersionResponse, error) {
	return f.getDerivationVersionsFn(ctx)
}
func (f fakeAdminService) GetTrustRuns(ctx context.Context, limit int) ([]adminTrustRunResponse, error) {
	if f.getTrustRunsFn == nil {
		return []adminTrustRunResponse{}, nil
	}
	return f.getTrustRunsFn(ctx, limit)
}
func (f fakeAdminService) GetTrustRun(ctx context.Context, runID int64) (adminTrustRunResponse, error) {
	if f.getTrustRunFn == nil {
		return adminTrustRunResponse{}, nil
	}
	return f.getTrustRunFn(ctx, runID)
}
func (f fakeAdminService) TriggerTrustRun(ctx context.Context) (adminTrustRunResponse, error) {
	if f.triggerTrustRunFn == nil {
		return adminTrustRunResponse{}, nil
	}
	return f.triggerTrustRunFn(ctx)
}
func (f fakeAdminService) GetTopTrustScores(ctx context.Context, limit int) ([]adminTrustScoreResponse, error) {
	if f.getTopTrustScoresFn == nil {
		return []adminTrustScoreResponse{}, nil
	}
	return f.getTopTrustScoresFn(ctx, limit)
}
func (f fakeAdminService) GetRelayRegistry(ctx context.Context, limit int) (adminRelayRegistryResponse, error) {
	return adminRelayRegistryResponse{Relays: []adminRelayRegistryEntry{}}, nil
}
func (f fakeAdminService) GetRelayRegistryDesired(ctx context.Context) (adminRelayRegistryDesiredResponse, error) {
	return adminRelayRegistryDesiredResponse{RelayURLs: []string{}}, nil
}
func (f fakeAdminService) SetRelayRegistryPolicy(ctx context.Context, req adminSetPolicyRequest) error {
	return nil
}
func (f fakeAdminService) GetRelayDiagnostics(ctx context.Context, relayURL string) (adminRelayDiagnosticsResponse, error) {
	return adminRelayDiagnosticsResponse{RecentObservations: []adminProbeObservationEntry{}}, nil
}
func (f fakeAdminService) GetRelayAdmissionDryRun(ctx context.Context) (adminAdmissionDryRunResponse, error) {
	return adminAdmissionDryRunResponse{Proposals: []adminAdmissionProposal{}}, nil
}
func (f fakeAdminService) SetAccountState(ctx context.Context, pubkey, state, reason string) (adminAccountStateResponse, error) {
	return adminAccountStateResponse{Pubkey: pubkey, State: state}, nil
}
func (f fakeAdminService) EnqueueAccountHydration(ctx context.Context, pubkey, reason string) (accountHydrateResponse, error) {
	return accountHydrateResponse{Pubkey: pubkey, Status: "queued"}, nil
}

func TestAdminRoutes_RequireBearerToken(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getJobsFn: func(_ context.Context, _ int) (adminJobsResponse, error) {
			return adminJobsResponse{}, nil
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/jobs", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAdminJobs_AuthorizedAndReturnsQueueData(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getJobsFn: func(_ context.Context, limit int) (adminJobsResponse, error) {
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			return adminJobsResponse{
				Counts: map[string]int64{"pending": 3},
				DueNow: 1,
				Recent: []adminJobItem{
					{ID: 101, JobType: "project_reply_counts", Status: "pending"},
				},
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/jobs?limit=2", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body struct {
		DueNow int64 `json:"due_now"`
		Recent []struct {
			ID int64 `json:"id"`
		} `json:"recent"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode jobs body: %v", err)
	}
	if body.DueNow != 1 || len(body.Recent) != 1 || body.Recent[0].ID != 101 {
		t.Fatalf("unexpected body: %+v", body)
	}
}

func TestAdminRelaySuggestions_AuthorizedAndReturnsSuggestions(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getRelaySuggestionsFn: func(_ context.Context, limit int, recommendedOnly bool) ([]adminRelaySuggestion, error) {
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			if !recommendedOnly {
				t.Fatal("expected recommended_only=true")
			}
			return []adminRelaySuggestion{
				{
					RelayURL:               "wss://relay.example",
					WeightedScore:          12.5,
					SupportingPubkeysCount: 3,
					Recommended:            true,
				},
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/relays/suggestions?limit=2&recommended_only=true", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body struct {
		Suggestions []adminRelaySuggestion `json:"suggestions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode suggestions body: %v", err)
	}
	if len(body.Suggestions) != 1 || body.Suggestions[0].RelayURL != "wss://relay.example" {
		t.Fatalf("unexpected suggestions body: %#v", body.Suggestions)
	}
}

func TestAdminRebuilds_PostTriggersRebuildRun(t *testing.T) {
	now := time.Now().UTC()
	mux := newAdminTestMux("token", fakeAdminService{
		triggerRebuildFn: func(_ context.Context, p derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error) {
			if p.DerivationName != derivation.DerivationReplyCounts {
				t.Fatalf("unexpected derivation: %s", p.DerivationName)
			}
			if p.Scope.Type != derivation.RebuildScopeFull {
				t.Fatalf("unexpected scope: %s", p.Scope.Type)
			}
			return adminRebuildRunResponse{
				ID:             77,
				DerivationName: p.DerivationName,
				TargetVersion:  2,
				Scope:          p.Scope,
				Status:         derivation.RebuildStatusPending,
				StartedAt:      &now,
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/rebuilds", strings.NewReader(`{
		"derivation_name":"reply_counts",
		"target_version":2,
		"scope":{"type":"full"}
	}`))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusAccepted)
	}
	var body struct {
		ID int64 `json:"id"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode rebuild body: %v", err)
	}
	if body.ID != 77 {
		t.Fatalf("unexpected id: %d", body.ID)
	}
}

func TestAdminStatusRoutes_ReturnFreshnessSignals(t *testing.T) {
	now := time.Date(2026, 4, 9, 10, 0, 0, 0, time.UTC)
	mux := newAdminTestMux("token", fakeAdminService{
		getProjectionStatusFn: func(_ context.Context) (adminProjectionStatusResponse, error) {
			return adminProjectionStatusResponse{
				NowUTC: now,
				Ingest: adminFreshnessSignal{Name: "ingest", Status: "fresh", Stale: false},
				Subsystems: []adminFreshnessSignal{
					{Name: "profiles_latest", Status: "fresh", Stale: false},
				},
				Healthy: true,
			}, nil
		},
		getDiscoveryStatusFn: func(_ context.Context) (adminDiscoveryStatusResponse, error) {
			return adminDiscoveryStatusResponse{
				NowUTC: now,
				Signals: []adminFreshnessSignal{
					{Name: "note_discovery_stats", Status: "fresh", Stale: false},
				},
				Ready: true,
			}, nil
		},
		getSearchStatusFn: func(_ context.Context) (adminSearchStatusResponse, error) {
			return adminSearchStatusResponse{
				NowUTC: now,
				Signals: []adminFreshnessSignal{
					{Name: "events_note_index", Status: "stale", Stale: true},
				},
				StaleSubsystems: []string{"events_note_index"},
				Ready:           false,
			}, nil
		},
	})

	for _, tc := range []struct {
		path      string
		wantCode  int
		wantToken string
	}{
		{path: "/admin/v1/status/projections", wantCode: http.StatusOK, wantToken: `"healthy":true`},
		{path: "/admin/v1/status/discovery", wantCode: http.StatusOK, wantToken: `"ready":true`},
		{path: "/admin/v1/status/search", wantCode: http.StatusOK, wantToken: `"events_note_index"`},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != tc.wantCode {
			t.Fatalf("%s unexpected status: got %d want %d", tc.path, rec.Code, tc.wantCode)
		}
		if !strings.Contains(rec.Body.String(), tc.wantToken) {
			t.Fatalf("%s expected body token %s, got %s", tc.path, tc.wantToken, rec.Body.String())
		}
	}
}

func TestAdminMeilisearchSync_DefaultsToAsync(t *testing.T) {
	called := make(chan int, 1)
	mux := newAdminTestMux("token", fakeAdminService{
		triggerMeilisearchSyncFn: func(_ context.Context, batchSize int) (adminMeilisearchSyncResponse, error) {
			called <- batchSize
			return adminMeilisearchSyncResponse{}, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/search/meilisearch/sync?batch_size=123", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusAccepted)
	}
	var body struct {
		BatchSize int    `json:"batch_size"`
		Async     bool   `json:"async"`
		Status    string `json:"status"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode meilisearch sync body: %v", err)
	}
	if body.BatchSize != 123 || !body.Async || body.Status != "started" {
		t.Fatalf("unexpected meilisearch sync body: %+v", body)
	}

	select {
	case got := <-called:
		if got != 123 {
			t.Fatalf("unexpected batch size in async call: got %d want %d", got, 123)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for async meilisearch sync call")
	}
}

func TestAdminMeilisearchSync_WaitTrue(t *testing.T) {
	startedAt := time.Date(2026, 4, 12, 6, 0, 0, 0, time.UTC)
	finishedAt := startedAt.Add(2 * time.Second)
	mux := newAdminTestMux("token", fakeAdminService{
		triggerMeilisearchSyncFn: func(_ context.Context, batchSize int) (adminMeilisearchSyncResponse, error) {
			if batchSize != 50 {
				t.Fatalf("unexpected batch size: got %d want %d", batchSize, 50)
			}
			resp := adminMeilisearchSyncResponse{
				StartedAt:  startedAt,
				FinishedAt: finishedAt,
				BatchSize:  batchSize,
				Async:      false,
				Status:     "completed",
			}
			resp.Stats.Notes = 10
			resp.Stats.Profiles = 20
			resp.Stats.Documents = 30
			return resp, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/search/meilisearch/sync?batch_size=50&wait=true", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusAccepted)
	}
	var body struct {
		Async  bool   `json:"async"`
		Status string `json:"status"`
		Stats  struct {
			Documents int64 `json:"documents"`
		} `json:"stats"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode meilisearch sync wait body: %v", err)
	}
	if body.Async || body.Status != "completed" || body.Stats.Documents != 30 {
		t.Fatalf("unexpected meilisearch wait body: %+v", body)
	}
}

func TestAdminRebuilds_RejectsOversizedPayload(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		triggerRebuildFn: func(_ context.Context, p derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error) {
			return adminRebuildRunResponse{}, nil
		},
	})
	tooLarge := `{"derivation_name":"` + strings.Repeat("x", adminBodyLimitBytes+10) + `","target_version":2,"scope":{"type":"full"}}`
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/rebuilds", strings.NewReader(tooLarge))
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestAdminDerivationVersions_ReturnsProjectionVersionStatus(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getDerivationVersionsFn: func(_ context.Context) ([]adminDerivationVersionResponse, error) {
			return []adminDerivationVersionResponse{
				{
					DerivationName:  "reply_counts",
					ActiveVersion:   1,
					TargetVersion:   2,
					CompiledVersion: 2,
					Status:          "rebuild_pending",
				},
			}, nil
		},
	})
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/derivation-versions", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	var body struct {
		Versions []struct {
			Status string `json:"status"`
		} `json:"versions"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode versions body: %v", err)
	}
	if len(body.Versions) != 1 || body.Versions[0].Status != "rebuild_pending" {
		t.Fatalf("unexpected versions: %+v", body.Versions)
	}
}

func TestAdminRelays_IncludesDurableLifecycleFields(t *testing.T) {
	now := time.Date(2026, 4, 5, 12, 0, 0, 0, time.UTC)
	connected := now.Add(-10 * time.Minute)
	disconnected := now.Add(-5 * time.Minute)
	progress := now.Add(-2 * time.Minute)
	lastErr := "connection_lost"
	errAt := now.Add(-4 * time.Minute)

	mux := newAdminTestMux("token", fakeAdminService{
		getRelaysFn: func(_ context.Context) ([]adminRelayState, error) {
			return []adminRelayState{
				{
					RelayURL: "wss://relay.one",
					Checkpoints: []adminRelayCheckpoint{
						{
							Mode:               "live",
							FilterGroup:        "default_v1",
							State:              "errored",
							Status:             "errored",
							Since:              ptrInt64(1700000100),
							Cursor:             ptrString("1700000100"),
							LastConnectedAt:    &connected,
							LastDisconnectedAt: &disconnected,
							LastProgressAt:     &progress,
							LastError:          &lastErr,
							LastErrorAt:        &errAt,
							UpdatedAt:          now,
						},
					},
				},
			}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/admin/v1/relays", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, key := range []string{
		`"state":"errored"`,
		`"last_connected_at"`,
		`"last_disconnected_at"`,
		`"last_progress_at"`,
		`"last_error":"connection_lost"`,
		`"last_error_at"`,
	} {
		if !strings.Contains(body, key) {
			t.Fatalf("expected response to include %s; body=%s", key, body)
		}
	}
}

func TestAdminTrustRoutes_BasicFlow(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getTrustRunsFn: func(_ context.Context, limit int) ([]adminTrustRunResponse, error) {
			if limit != 2 {
				t.Fatalf("unexpected limit: %d", limit)
			}
			return []adminTrustRunResponse{{ID: 101, Status: "pending"}}, nil
		},
		getTrustRunFn: func(_ context.Context, runID int64) (adminTrustRunResponse, error) {
			return adminTrustRunResponse{ID: runID, Status: "succeeded"}, nil
		},
		triggerTrustRunFn: func(_ context.Context) (adminTrustRunResponse, error) {
			return adminTrustRunResponse{ID: 202, Status: "pending"}, nil
		},
		getTopTrustScoresFn: func(_ context.Context, limit int) ([]adminTrustScoreResponse, error) {
			if limit != 3 {
				t.Fatalf("unexpected score limit: %d", limit)
			}
			return []adminTrustScoreResponse{{Pubkey: "a", Rank: 1, Score: 10}}, nil
		},
	})

	reqRuns := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs?limit=2", nil)
	reqRuns.Header.Set("Authorization", "Bearer token")
	recRuns := httptest.NewRecorder()
	mux.ServeHTTP(recRuns, reqRuns)
	if recRuns.Code != http.StatusOK {
		t.Fatalf("unexpected runs status: got %d want %d", recRuns.Code, http.StatusOK)
	}

	reqRun := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs/17", nil)
	reqRun.Header.Set("Authorization", "Bearer token")
	recRun := httptest.NewRecorder()
	mux.ServeHTTP(recRun, reqRun)
	if recRun.Code != http.StatusOK {
		t.Fatalf("unexpected run status: got %d want %d", recRun.Code, http.StatusOK)
	}

	reqTrigger := httptest.NewRequest(http.MethodPost, "/admin/v1/trust/runs", nil)
	reqTrigger.Header.Set("Authorization", "Bearer token")
	recTrigger := httptest.NewRecorder()
	mux.ServeHTTP(recTrigger, reqTrigger)
	if recTrigger.Code != http.StatusAccepted {
		t.Fatalf("unexpected trigger status: got %d want %d", recTrigger.Code, http.StatusAccepted)
	}

	reqScores := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/scores?limit=3", nil)
	reqScores.Header.Set("Authorization", "Bearer token")
	recScores := httptest.NewRecorder()
	mux.ServeHTTP(recScores, reqScores)
	if recScores.Code != http.StatusOK {
		t.Fatalf("unexpected scores status: got %d want %d", recScores.Code, http.StatusOK)
	}
}

func TestAdminTrustRoutes_ErrorMappingAndValidation(t *testing.T) {
	mux := newAdminTestMux("token", fakeAdminService{
		getTrustRunsFn: func(_ context.Context, limit int) ([]adminTrustRunResponse, error) {
			if limit != 50 {
				t.Fatalf("expected default runs limit 50, got %d", limit)
			}
			return nil, errors.New("store unavailable")
		},
		getTrustRunFn: func(_ context.Context, runID int64) (adminTrustRunResponse, error) {
			if runID != 404 {
				t.Fatalf("unexpected run id: %d", runID)
			}
			return adminTrustRunResponse{}, errors.New("missing")
		},
		triggerTrustRunFn: func(_ context.Context) (adminTrustRunResponse, error) {
			return adminTrustRunResponse{}, errors.New("trust runtime is not configured")
		},
		getTopTrustScoresFn: func(_ context.Context, limit int) ([]adminTrustScoreResponse, error) {
			if limit != 50 {
				t.Fatalf("expected default score limit 50, got %d", limit)
			}
			return nil, errors.New("store unavailable")
		},
	})

	t.Run("trust runs default limit internal error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
		}
	})

	t.Run("trust runs rejects invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs?limit=9999", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("trust run rejects invalid id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs/not-a-number", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("trust run maps missing to not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/runs/404", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusNotFound)
		}
	})

	t.Run("trigger trust run returns bad request on runtime error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/admin/v1/trust/runs", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
		}
	})

	t.Run("trust scores default limit internal error", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/scores", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusInternalServerError)
		}
	})

	t.Run("trust scores reject invalid limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/admin/v1/trust/scores?limit=0", nil)
		req.Header.Set("Authorization", "Bearer token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("unexpected status: got %d want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

func newAdminTestMux(token string, service AdminService) http.Handler {
	handlers := NewAdminHandlers(service)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /admin/v1/relays", handlers.GetRelays)
	adminMux.HandleFunc("GET /admin/v1/relays/suggestions", handlers.GetRelaySuggestions)
	adminMux.HandleFunc("GET /admin/v1/jobs", handlers.GetJobs)
	adminMux.HandleFunc("GET /admin/v1/invalid-events", handlers.GetInvalidEvents)
	adminMux.HandleFunc("GET /admin/v1/status/projections", handlers.GetProjectionStatus)
	adminMux.HandleFunc("GET /admin/v1/status/discovery", handlers.GetDiscoveryStatus)
	adminMux.HandleFunc("GET /admin/v1/status/search", handlers.GetSearchStatus)
	adminMux.HandleFunc("POST /admin/v1/search/meilisearch/sync", handlers.TriggerMeilisearchSync)
	adminMux.HandleFunc("GET /admin/v1/rebuilds", handlers.GetRebuilds)
	adminMux.HandleFunc("POST /admin/v1/rebuilds", handlers.TriggerRebuild)
	adminMux.HandleFunc("GET /admin/v1/storage", handlers.GetStorage)
	adminMux.HandleFunc("GET /admin/v1/system", handlers.GetSystem)
	adminMux.HandleFunc("GET /admin/v1/derivation-versions", handlers.GetDerivationVersions)
	adminMux.HandleFunc("GET /admin/v1/trust/runs", handlers.GetTrustRuns)
	adminMux.HandleFunc("GET /admin/v1/trust/runs/{runID}", handlers.GetTrustRun)
	adminMux.HandleFunc("POST /admin/v1/trust/runs", handlers.TriggerTrustRun)
	adminMux.HandleFunc("GET /admin/v1/trust/scores", handlers.GetTopTrustScores)
	return WithRequestID(RequireBearerToken(token, adminMux))
}

func ptrString(v string) *string {
	c := v
	return &c
}

func ptrInt64(v int64) *int64 {
	c := v
	return &c
}
