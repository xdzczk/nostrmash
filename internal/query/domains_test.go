package query

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
)

type curatedReaderWithHomeTrendingDomains struct {
	fakeReader
	getHomeTrendingDomainsFn func(context.Context, time.Duration, int) ([]storeread.DomainSummaryProjection, error)
}

func (r curatedReaderWithHomeTrendingDomains) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (r curatedReaderWithHomeTrendingDomains) GetHomeTrendingDomains(
	ctx context.Context,
	window time.Duration,
	limit int,
) ([]storeread.DomainSummaryProjection, error) {
	if r.getHomeTrendingDomainsFn == nil {
		return nil, nil
	}
	return r.getHomeTrendingDomainsFn(ctx, window, limit)
}

func TestGetHomeTrendingDomains_MapsSnapshotRows(t *testing.T) {
	t.Parallel()
	var gotWindow time.Duration
	var gotLimit int
	svc := mustNewService(t, curatedReaderWithHomeTrendingDomains{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getHomeTrendingDomainsFn: func(_ context.Context, window time.Duration, limit int) ([]storeread.DomainSummaryProjection, error) {
			gotWindow = window
			gotLimit = limit
			latest := int64(1700000000)
			return []storeread.DomainSummaryProjection{
				{
					Domain:        "example.com",
					LatestEventAt: &latest,
					Activity: storeread.DomainActivityStatsProjection{
						Last24h: storeread.DomainActivityProjection{LinkCount: 3, NoteCount: 2, UniqueAuthors: 2},
						Last7d:  storeread.DomainActivityProjection{LinkCount: 9, NoteCount: 6, UniqueAuthors: 4},
					},
				},
			}, nil
		},
	})

	out, err := svc.GetHomeTrendingDomains(context.Background(), 24*time.Hour, 10)
	if err != nil {
		t.Fatalf("GetHomeTrendingDomains returned error: %v", err)
	}
	if gotWindow != 24*time.Hour {
		t.Fatalf("unexpected window passed to reader: %s", gotWindow)
	}
	if gotLimit != 10 {
		t.Fatalf("unexpected limit passed to reader: %d", gotLimit)
	}
	if len(out) != 1 {
		t.Fatalf("unexpected result length: got=%d want=1", len(out))
	}
	if out[0].Domain != "example.com" || out[0].Activity.Last24h.LinkCount != 3 || out[0].Activity.Last7d.UniqueAuthors != 4 {
		t.Fatalf("unexpected mapped domain summary: %#v", out[0])
	}
}

func TestGetHomeTrendingDomains_BoundsLimitAndDefaultsWindow(t *testing.T) {
	t.Parallel()
	var gotLimit int
	svc := mustNewService(t, curatedReaderWithHomeTrendingDomains{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getHomeTrendingDomainsFn: func(_ context.Context, _ time.Duration, limit int) ([]storeread.DomainSummaryProjection, error) {
			gotLimit = limit
			return nil, nil
		},
	})

	if _, err := svc.GetHomeTrendingDomains(context.Background(), 24*time.Hour, 500); err != nil {
		t.Fatalf("GetHomeTrendingDomains returned error: %v", err)
	}
	if gotLimit != 50 {
		t.Fatalf("expected limit to be capped at 50, got %d", gotLimit)
	}

	if _, err := svc.GetHomeTrendingDomains(context.Background(), 0, 10); err == nil {
		t.Fatalf("expected error for non-positive window")
	}
}

func TestGetHomeTrendingDomains_UnsupportedWithoutCapability(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, fakeReader{
		getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
			return nil, store.ErrNotFound
		},
	})
	_, err := svc.GetHomeTrendingDomains(context.Background(), 24*time.Hour, 10)
	if !IsUnsupportedCapability(err) {
		t.Fatalf("expected unsupported capability error, got %v", err)
	}
}
