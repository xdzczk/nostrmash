package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/derivation"
)

type fakeAdminService struct {
	getRelaysFn             func(context.Context) ([]adminRelayState, error)
	getJobsFn               func(context.Context, int) (adminJobsResponse, error)
	getInvalidEventsFn      func(context.Context, int) (adminInvalidEventsResponse, error)
	getRebuildsFn           func(context.Context, int) ([]adminRebuildRunResponse, error)
	triggerRebuildFn        func(context.Context, derivation.TriggerProjectionRebuildParams) (adminRebuildRunResponse, error)
	getStorageFn            func(context.Context) (adminStorageResponse, error)
	getSystemFn             func(context.Context) (adminSystemResponse, error)
	getDerivationVersionsFn func(context.Context) ([]adminDerivationVersionResponse, error)
	getTrustRunsFn          func(context.Context, int) ([]adminTrustRunResponse, error)
	getTrustRunFn           func(context.Context, int64) (adminTrustRunResponse, error)
	triggerTrustRunFn       func(context.Context) (adminTrustRunResponse, error)
	getTopTrustScoresFn     func(context.Context, int) ([]adminTrustScoreResponse, error)
}

func (f fakeAdminService) GetRelays(ctx context.Context) ([]adminRelayState, error) {
	return f.getRelaysFn(ctx)
}
func (f fakeAdminService) GetJobs(ctx context.Context, limit int) (adminJobsResponse, error) {
	return f.getJobsFn(ctx, limit)
}
func (f fakeAdminService) GetInvalidEvents(ctx context.Context, limit int) (adminInvalidEventsResponse, error) {
	return f.getInvalidEventsFn(ctx, limit)
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

func newAdminTestMux(token string, service AdminService) http.Handler {
	handlers := NewAdminHandlers(service)
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("GET /admin/v1/relays", handlers.GetRelays)
	adminMux.HandleFunc("GET /admin/v1/jobs", handlers.GetJobs)
	adminMux.HandleFunc("GET /admin/v1/invalid-events", handlers.GetInvalidEvents)
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
