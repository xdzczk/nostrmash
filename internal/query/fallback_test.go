package query

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/metrics"
	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetEventByID_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 1 || ids[0] != "evt-1" {
					t.Fatalf("unexpected ids for relay fallback: %#v", ids)
				}
				return map[string]json.RawMessage{"evt-1": json.RawMessage(`{"id":"evt-1"}`)}, nil
			},
		},
	})

	raw, err := svc.GetEventByID(context.Background(), "evt-1")
	if err != nil {
		t.Fatalf("GetEventByID returned error: %v", err)
	}
	if string(raw) != `{"id":"evt-1"}` {
		t.Fatalf("unexpected raw event: %s", string(raw))
	}
}

func TestNewServiceWithOptions_AcceptsNilFallbackReader(t *testing.T) {
	t.Parallel()
	// The fallback reader is now a typed optional; an unsupported value can no
	// longer be supplied. A nil fallback must construct cleanly.
	if _, err := NewServiceWithOptions(fakeReader{}, ServiceOptions{}); err != nil {
		t.Fatalf("expected construction to succeed without a fallback reader: %v", err)
	}
}

func TestGetEventByID_LocalHitSkipsRelayFallback(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return json.RawMessage(`{"id":"evt-local"}`), nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				t.Fatalf("relay fallback should not run on local hit")
				return nil, nil
			},
		},
	})
	raw, err := svc.GetEventByID(context.Background(), "evt-local")
	if err != nil {
		t.Fatalf("GetEventByID returned error: %v", err)
	}
	if string(raw) != `{"id":"evt-local"}` {
		t.Fatalf("unexpected local event payload: %s", string(raw))
	}
}

func TestGetEventByID_LocalMissRelayMiss(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{}, nil
			},
		},
	})

	_, err := svc.GetEventByID(context.Background(), "evt-missing")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound, got %v", err)
	}
}

func TestGetEventBatch_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"evt-1": json.RawMessage(`{"id":"evt-1"}`),
			}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 1 || ids[0] != "evt-2" {
					t.Fatalf("unexpected fallback ids: %#v", ids)
				}
				return map[string]json.RawMessage{
					"evt-2": json.RawMessage(`{"id":"evt-2"}`),
				}, nil
			},
		},
	})

	out, err := svc.GetEventBatch(context.Background(), []string{"evt-1", "evt-2"})
	if err != nil {
		t.Fatalf("GetEventBatch returned error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("unexpected batch size: %d", len(out))
	}
}

func TestGetEventBatch_LocalMissRelayPartialSuccess(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
			return map[string]json.RawMessage{
				"evt-1": json.RawMessage(`{"id":"evt-1"}`),
			}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(_ context.Context, ids []string) (map[string]json.RawMessage, error) {
				if len(ids) != 2 {
					t.Fatalf("expected 2 missing ids for fallback, got %#v", ids)
				}
				return map[string]json.RawMessage{
					"evt-2": json.RawMessage(`{"id":"evt-2"}`),
				}, nil
			},
		},
	})

	out, err := svc.GetEventBatch(context.Background(), []string{"evt-1", "evt-2", "evt-3"})
	if err != nil {
		t.Fatalf("GetEventBatch returned error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected partial batch size 2, got %d", len(out))
	}
	if _, ok := out["evt-3"]; ok {
		t.Fatalf("did not expect unresolved fallback id evt-3 in output")
	}
}

func TestGetProfile_LocalMissRelaySuccess(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(_ context.Context, pubkeys []string) (map[string]Profile, error) {
				if len(pubkeys) != 1 || pubkeys[0] != "pk-1" {
					t.Fatalf("unexpected pubkeys for fallback: %#v", pubkeys)
				}
				return map[string]Profile{
					"pk-1": {
						Pubkey:            "pk-1",
						MetadataEventID:   "meta-1",
						MetadataCreatedAt: 100,
						ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
					},
				}, nil
			},
		},
	})

	profile, err := svc.GetProfile(context.Background(), "pk-1")
	if err != nil {
		t.Fatalf("GetProfile returned error: %v", err)
	}
	if profile.Pubkey != "pk-1" {
		t.Fatalf("unexpected profile pubkey: %s", profile.Pubkey)
	}
}

func TestGetProfiles_LocalMissRelayMissPreservesMissing(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]Profile, error) {
				return map[string]Profile{}, nil
			},
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{"pk-1", "pk-2"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 0 {
		t.Fatalf("expected no profiles, got %#v", out.Profiles)
	}
	if len(out.MissingPubkeys) != 2 {
		t.Fatalf("expected 2 missing pubkeys, got %#v", out.MissingPubkeys)
	}
}

func TestGetProfiles_LocalMissRelayPartialSuccessPreservesMissing(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{
		getProfilesByPubkeysFn: func(context.Context, []string) (map[string]store.ProfileProjection, error) {
			return map[string]store.ProfileProjection{}, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]Profile, error) {
				return map[string]Profile{
					"pk-1": {
						Pubkey:            "pk-1",
						MetadataEventID:   "meta-1",
						MetadataCreatedAt: 123,
						ProfileJSON:       json.RawMessage(`{"name":"one"}`),
					},
				}, nil
			},
		},
	})

	out, err := svc.GetProfiles(context.Background(), []string{"pk-1", "pk-2"})
	if err != nil {
		t.Fatalf("GetProfiles returned error: %v", err)
	}
	if len(out.Profiles) != 1 {
		t.Fatalf("expected 1 recovered profile, got %d", len(out.Profiles))
	}
	if len(out.MissingPubkeys) != 1 || out.MissingPubkeys[0] != "pk-2" {
		t.Fatalf("expected pk-2 to stay missing, got %#v", out.MissingPubkeys)
	}
}

func TestFallbackPolicy_TrustedOnlyBlocksDiscoveryMisses(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, fakeReader{}, ServiceOptions{
		FallbackFetchTrustMode: trustModeTrustedOnly,
	})
	allowedDiscovery, _ := svc.fallbackPolicy().eventAdmission(fallbackLookupDiscoveryMiss)
	if allowedDiscovery {
		t.Fatalf("expected trusted_only policy to block discovery-driven fallback")
	}
	allowedSearch, _ := svc.fallbackPolicy().eventAdmission(fallbackLookupSearchMiss)
	if allowedSearch {
		t.Fatalf("expected trusted_only policy to block search-driven fallback")
	}
	allowedDirect, _ := svc.fallbackPolicy().eventAdmission(fallbackLookupDirect)
	if !allowedDirect {
		t.Fatalf("expected trusted_only policy to preserve direct fallback by default")
	}
}

func TestGetProfile_TrustedOnlyUntrustedDirectBlockedWhenDisabled(t *testing.T) {
	t.Parallel()
	allowDirect := false
	svc := mustNewServiceWithOptions(t, readerWithAdvancedSearch{
		Reader: fakeReader{
			getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
				return store.ProfileProjection{}, store.ErrNotFound
			},
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			out := make(map[string]readmodel.TrustQualification, len(pubkeys))
			for _, pubkey := range pubkeys {
				out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: false}
			}
			return out, nil
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]Profile, error) {
				t.Fatalf("fallback should be denied for untrusted direct lookup when direct exception disabled")
				return nil, nil
			},
		},
		FallbackFetchTrustMode:         trustModeTrustedOnly,
		FallbackFetchAllowDirectLookup: &allowDirect,
	})

	_, err := svc.GetProfile(context.Background(), "pk-denied")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound, got %v", err)
	}
}

func TestGetEventByID_PreferTrustedUntrustedFallbackUsesSingleStrictAttempt(t *testing.T) {
	t.Parallel()
	var attempts int
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				attempts++
				return map[string]json.RawMessage{}, nil
			},
		},
		FallbackFetchTrustMode:     trustModePreferTrusted,
		FallbackFetchMaxAttempts:   5,
		FallbackFetchMaxTimeBudget: 5 * time.Second,
	})

	_, _ = svc.GetEventByID(context.Background(), "evt-untrusted")
	if attempts != 1 {
		t.Fatalf("expected strict untrusted fallback to use one attempt, got %d", attempts)
	}
}

func TestGetEventByID_OpenModeHonorsMaxFallbackAttempts(t *testing.T) {
	t.Parallel()
	var attempts int
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				attempts++
				return map[string]json.RawMessage{}, nil
			},
		},
		FallbackFetchTrustMode:     trustModeOpen,
		FallbackFetchMaxAttempts:   2,
		FallbackFetchMaxTimeBudget: time.Second,
	})

	_, _ = svc.GetEventByID(context.Background(), "evt-open-attempts")
	if attempts != 2 {
		t.Fatalf("expected open mode to honor configured max attempts, got %d", attempts)
	}
}

func TestGetEventByID_FallbackMetricsEmitHit(t *testing.T) {
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{
					"evt-hit": json.RawMessage(`{"id":"evt-hit"}`),
				}, nil
			},
		},
	})

	attemptBefore := fallbackMetricValue(t, "nostrmash_lookup_fallback_total", `entity="event"`, `result="attempt"`)
	resultBefore := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="hit"`,
	)
	if _, err := svc.GetEventByID(context.Background(), "evt-hit"); err != nil {
		t.Fatalf("GetEventByID returned error: %v", err)
	}
	attemptAfter := fallbackMetricValue(t, "nostrmash_lookup_fallback_total", `entity="event"`, `result="attempt"`)
	resultAfter := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="hit"`,
	)
	if attemptAfter <= attemptBefore {
		t.Fatalf("expected fallback attempt metric increment for event")
	}
	if resultAfter <= resultBefore {
		t.Fatalf("expected fallback hit result metric increment for event")
	}
}

func TestGetEventByID_FallbackMetricsEmitMiss(t *testing.T) {
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{}, nil
			},
		},
	})

	resultBefore := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="miss"`,
	)
	_, _ = svc.GetEventByID(context.Background(), "evt-miss")
	resultAfter := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="miss"`,
	)
	if resultAfter <= resultBefore {
		t.Fatalf("expected fallback miss result metric increment for event")
	}
}

func TestGetProfile_FallbackMetricsEmitError(t *testing.T) {
	svc := mustNewServiceWithOptions(t, fakeReader{
		getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchProfilesByPubkeysFn: func(context.Context, []string) (map[string]Profile, error) {
				return nil, errors.New("dial relay wss://relay.example: i/o timeout")
			},
		},
	})

	resultBefore := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="profile"`,
		`result="error"`,
	)
	_, _ = svc.GetProfile(context.Background(), "pk-error")
	resultAfter := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="profile"`,
		`result="error"`,
	)
	if resultAfter <= resultBefore {
		t.Fatalf("expected fallback error result metric increment for profile")
	}
}

func TestGetEventByID_FallbackErrorDegradedToNotFoundStillEmitsErrorMetric(t *testing.T) {
	logOutput, restore := captureDefaultSlogJSON()
	defer restore()

	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return nil, errors.New("dial relay wss://relay.example: i/o timeout")
			},
		},
	})

	resultBefore := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="error"`,
	)
	_, err := svc.GetEventByID(context.Background(), "evt-error")
	if !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("expected store.ErrNotFound, got %v", err)
	}
	resultAfter := fallbackMetricValue(
		t,
		"nostrmash_lookup_fallback_total",
		`entity="event"`,
		`result="error"`,
	)
	if resultAfter <= resultBefore {
		t.Fatalf("expected fallback error metric increment when degraded to not-found")
	}

	body := logOutput.String()
	if !strings.Contains(body, `"query_fallback_lookup_failed"`) {
		t.Fatalf("expected fallback infra failure log, got %q", body)
	}
	if !strings.Contains(body, `"entity_type":"event"`) {
		t.Fatalf("expected event entity_type in fallback log, got %q", body)
	}
	if !strings.Contains(body, `"entity_key":"evt-error"`) {
		t.Fatalf("expected entity_key in fallback log, got %q", body)
	}
	if !strings.Contains(body, `"error_class":"transport"`) {
		t.Fatalf("expected error_class in fallback log, got %q", body)
	}
	if !strings.Contains(body, `"degraded_to_not_found":true`) {
		t.Fatalf("expected degraded_to_not_found=true in fallback log, got %q", body)
	}
}

func TestFallbackMetricsUseBoundedEntityAndResultLabels(t *testing.T) {
	svc := mustNewServiceWithOptions(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	}, ServiceOptions{
		FallbackReader: fakeFallbackReader{
			fetchEventsByIDsFn: func(context.Context, []string) (map[string]json.RawMessage, error) {
				return map[string]json.RawMessage{}, nil
			},
		},
	})
	_, _ = svc.GetEventByID(context.Background(), "evt-bounded-labels")

	body := fallbackMetricsBody(t)
	lines := strings.Split(body, "\n")
	foundFallbackLine := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, "nostrmash_lookup_fallback_total") {
			continue
		}
		foundFallbackLine = true
		if !strings.Contains(line, `entity="`) || !strings.Contains(line, `result="`) {
			t.Fatalf("expected entity/result labels in fallback metric line: %q", line)
		}
		if strings.Contains(line, `surface="`) || strings.Contains(line, `outcome="`) {
			t.Fatalf("unexpected unbounded/legacy labels in fallback metric line: %q", line)
		}
	}
	if !foundFallbackLine {
		t.Fatalf("expected fallback metric line in /metrics output")
	}
}

func fallbackMetricValue(t *testing.T, metricName string, labelFragments ...string) float64 {
	t.Helper()

	body := fallbackMetricsBody(t)
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metricName) {
			continue
		}
		matches := true
		for _, fragment := range labelFragments {
			if !strings.Contains(line, fragment) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			t.Fatalf("parse metric value from line %q: %v", line, err)
		}
		return value
	}
	return 0
}

func fallbackMetricsBody(t *testing.T) string {
	t.Helper()
	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)
	return rec.Body.String()
}

func captureDefaultSlogJSON() (*bytes.Buffer, func()) {
	buf := &bytes.Buffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(buf, nil)))
	return buf, func() {
		slog.SetDefault(prev)
	}
}

type fakeReader struct {
	getEventRawByIDFn       func(context.Context, string) (json.RawMessage, error)
	getEventRawsByIDsFn     func(context.Context, []string) (map[string]json.RawMessage, error)
	getProfileByPubkeyFn    func(context.Context, string) (store.ProfileProjection, error)
	getProfilesByPubkeysFn  func(context.Context, []string) (map[string]store.ProfileProjection, error)
	getProfilePublicStatsFn func(context.Context, string) (store.ProfilePublicStatsProjection, error)
}

func (f fakeReader) GetEventRawByID(ctx context.Context, id string) (json.RawMessage, error) {
	if f.getEventRawByIDFn != nil {
		return f.getEventRawByIDFn(ctx, id)
	}
	return nil, store.ErrNotFound
}

func (f fakeReader) GetEventWithProvenance(context.Context, string) (EventWithProvenance, error) {
	return EventWithProvenance{}, store.ErrNotFound
}

func (f fakeReader) GetEventRawsByIDs(ctx context.Context, ids []string) (map[string]json.RawMessage, error) {
	if f.getEventRawsByIDsFn != nil {
		return f.getEventRawsByIDsFn(ctx, ids)
	}
	return map[string]json.RawMessage{}, nil
}

func (f fakeReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (f fakeReader) GetProfileByPubkey(ctx context.Context, pubkey string) (Profile, error) {
	if f.getProfileByPubkeyFn != nil {
		row, err := f.getProfileByPubkeyFn(ctx, pubkey)
		if err != nil {
			return Profile{}, err
		}
		return profileFromStore(row), nil
	}
	return Profile{}, store.ErrNotFound
}

func (f fakeReader) GetProfilesByPubkeys(ctx context.Context, pubkeys []string) (map[string]Profile, error) {
	if f.getProfilesByPubkeysFn != nil {
		rows, err := f.getProfilesByPubkeysFn(ctx, pubkeys)
		if err != nil {
			return nil, err
		}
		out := make(map[string]Profile, len(rows))
		for pubkey, row := range rows {
			out[pubkey] = profileFromStore(row)
		}
		return out, nil
	}
	return map[string]Profile{}, nil
}

func (f fakeReader) GetProfilePublicStatsByPubkey(ctx context.Context, pubkey string) (ProfilePublicStats, error) {
	if f.getProfilePublicStatsFn != nil {
		row, err := f.getProfilePublicStatsFn(ctx, pubkey)
		if err != nil {
			return ProfilePublicStats{}, err
		}
		return profilePublicStatsFromStore(row), nil
	}
	return ProfilePublicStats{Pubkey: pubkey}, nil
}

func (f fakeReader) GetAuthorAnalyticsSummary(context.Context, string) (AuthorAnalyticsSummary, error) {
	return AuthorAnalyticsSummary{Windows: []AuthorAnalyticsWindowSummary{}}, nil
}

func (f fakeReader) GetAuthorQuoteRepostRecentActivity(context.Context, string, int) ([]QuoteRepostActivity, error) {
	return []QuoteRepostActivity{}, nil
}

func (f fakeReader) GetAuthorTopicStats(context.Context, string, int, int) ([]AuthorTopicStat, error) {
	return []AuthorTopicStat{}, nil
}

func (f fakeReader) GetAuthorTopLanguages(context.Context, string, int, int) ([]LanguageSummary, error) {
	return []LanguageSummary{}, nil
}

func (f fakeReader) GetAuthorMediaMixStats(context.Context, string, int) (AuthorAnalyticsMediaMix, error) {
	return AuthorAnalyticsMediaMix{}, nil
}

func (f fakeReader) GetAuthorActivityWindows(context.Context, string, int) (AuthorActivityWindows, error) {
	return AuthorActivityWindows{Timezone: "UTC"}, nil
}

func (f fakeReader) GetAuthorPostingPatterns(context.Context, string, int) (AuthorPostingPatterns, error) {
	return AuthorPostingPatterns{Timezone: "UTC"}, nil
}

func (f fakeReader) GetAuthorTopNotes(context.Context, string, int, int) ([]AuthorTopNote, error) {
	return []AuthorTopNote{}, nil
}

func (f fakeReader) GetAuthorRecycleCandidates(context.Context, string, int, int, float64, bool, bool, int, int) ([]AuthorRecycleCandidate, error) {
	return []AuthorRecycleCandidate{}, nil
}

func (f fakeReader) GetAuthorPerformanceSummary(context.Context, string, int) (AuthorPerformanceSummary, error) {
	return AuthorPerformanceSummary{}, nil
}

func (f fakeReader) GetAuthorRecentEvents(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetAuthorReplies(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetEventCounts(context.Context, string) (EventCounts, error) {
	return EventCounts{}, nil
}

func (f fakeReader) GetEventReplies(context.Context, string, int, *EventCursor) ([]json.RawMessage, *EventCursor, error) {
	return []json.RawMessage{}, nil, nil
}

func (f fakeReader) GetEventAncestors(context.Context, string, int) ([]json.RawMessage, []string, error) {
	return []json.RawMessage{}, []string{}, nil
}

func (f fakeReader) ListRelayHealth(context.Context) ([]model.IngestCheckpoint, error) {
	return []model.IngestCheckpoint{}, nil
}

func (f fakeReader) GetContactListByPubkey(context.Context, string) (ContactList, error) {
	return ContactList{}, store.ErrNotFound
}

func (f fakeReader) GetRelayListByPubkey(context.Context, string) (RelayList, error) {
	return RelayList{}, store.ErrNotFound
}

func (f fakeReader) SearchEventsByContent(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) SearchProfiles(context.Context, string, int) ([]Profile, error) {
	return []Profile{}, nil
}

func (f fakeReader) GetRecentEventsByKindAndPubkey(context.Context, int, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetEventsReferencingPubkey(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}

func (f fakeReader) GetFollowersByPubkey(context.Context, string, int) ([]json.RawMessage, error) {
	return []json.RawMessage{}, nil
}
