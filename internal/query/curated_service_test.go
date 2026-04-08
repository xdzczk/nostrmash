package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetNetworkStatsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getNetworkStatsFn: func(context.Context) (store.NetworkStats, error) {
			return store.NetworkStats{Events: 11, Profiles: 22, Relays: 33}, nil
		},
	})
	stats, err := svc.GetNetworkStats(context.Background())
	if err != nil {
		t.Fatalf("GetNetworkStats returned error: %v", err)
	}
	if stats.Events != 11 || stats.Profiles != 22 || stats.Relays != 33 {
		t.Fatalf("unexpected network stats: %#v", stats)
	}
}

func TestGetCuratedRecommendedReadsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getCuratedRecommendedReadsFn: func(context.Context, int) ([]store.CuratedRecommendedRead, error) {
			return []store.CuratedRecommendedRead{{
				EventID: "evt-1",
				Title:   "Read one",
				URL:     "https://example.com",
				Rank:    1,
			}}, nil
		},
	})
	out, err := svc.GetCuratedRecommendedReads(context.Background(), 5)
	if err != nil {
		t.Fatalf("GetCuratedRecommendedReads returned error: %v", err)
	}
	if len(out) != 1 || out[0].EventID != "evt-1" || out[0].Rank != 1 {
		t.Fatalf("unexpected curated reads: %#v", out)
	}
}

func TestGetCuratedReadsTopicsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getCuratedReadsTopicsFn: func(context.Context, int) ([]store.CuratedReadsTopic, error) {
			return []store.CuratedReadsTopic{{Topic: "nostr", Rank: 2}}, nil
		},
	})
	out, err := svc.GetCuratedReadsTopics(context.Background(), 5)
	if err != nil {
		t.Fatalf("GetCuratedReadsTopics returned error: %v", err)
	}
	if len(out) != 1 || out[0].Topic != "nostr" || out[0].Rank != 2 {
		t.Fatalf("unexpected curated topics: %#v", out)
	}
}

func TestGetCuratedFeaturedAuthorsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getCuratedFeaturedAuthorsFn: func(context.Context, int) ([]store.CuratedFeaturedAuthor, error) {
			return []store.CuratedFeaturedAuthor{{Pubkey: "pk-1", Rank: 3}}, nil
		},
	})
	out, err := svc.GetCuratedFeaturedAuthors(context.Background(), 5)
	if err != nil {
		t.Fatalf("GetCuratedFeaturedAuthors returned error: %v", err)
	}
	if len(out) != 1 || out[0].Pubkey != "pk-1" || out[0].Rank != 3 {
		t.Fatalf("unexpected curated featured authors: %#v", out)
	}
}

type curatedLegacyReader struct {
	fakeReader
	getNetworkStatsFn            func(context.Context) (store.NetworkStats, error)
	getCuratedRecommendedReadsFn func(context.Context, int) ([]store.CuratedRecommendedRead, error)
	getCuratedReadsTopicsFn      func(context.Context, int) ([]store.CuratedReadsTopic, error)
	getCuratedFeaturedAuthorsFn  func(context.Context, int) ([]store.CuratedFeaturedAuthor, error)
}

func (r curatedLegacyReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (r curatedLegacyReader) GetNetworkStats(ctx context.Context) (store.NetworkStats, error) {
	if r.getNetworkStatsFn == nil {
		return store.NetworkStats{}, nil
	}
	return r.getNetworkStatsFn(ctx)
}

func (r curatedLegacyReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]store.CuratedRecommendedRead, error) {
	if r.getCuratedRecommendedReadsFn == nil {
		return []store.CuratedRecommendedRead{}, nil
	}
	return r.getCuratedRecommendedReadsFn(ctx, limit)
}

func (r curatedLegacyReader) GetCuratedReadsTopics(ctx context.Context, limit int) ([]store.CuratedReadsTopic, error) {
	if r.getCuratedReadsTopicsFn == nil {
		return []store.CuratedReadsTopic{}, nil
	}
	return r.getCuratedReadsTopicsFn(ctx, limit)
}

func (r curatedLegacyReader) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]store.CuratedFeaturedAuthor, error) {
	if r.getCuratedFeaturedAuthorsFn == nil {
		return []store.CuratedFeaturedAuthor{}, nil
	}
	return r.getCuratedFeaturedAuthorsFn(ctx, limit)
}
