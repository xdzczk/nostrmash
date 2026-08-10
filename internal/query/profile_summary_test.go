package query

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/store"
)

func TestGetProfilePublicSummary_ComposesProfileAndStats(t *testing.T) {
	t.Parallel()
	recent := int64(900)
	hops := 1
	rank := int64(4)
	svc := mustNewService(t, trustQualificationReader{
		fakeReader: fakeReader{
			getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
				return store.ProfileProjection{
					Pubkey:            "pk_1",
					MetadataEventID:   "meta_1",
					MetadataCreatedAt: 321,
					ProfileJSON:       json.RawMessage(`{"name":"alice"}`),
				}, nil
			},
			getProfilePublicStatsFn: func(context.Context, string) (store.ProfilePublicStatsProjection, error) {
				return store.ProfilePublicStatsProjection{
					Pubkey:           "pk_1",
					FollowerCount:    6,
					FollowingCount:   3,
					NoteCount:        11,
					ReplyCount:       2,
					RecentActivityAt: &recent,
				}, nil
			},
		},
		getTrustStateFn: func(_ context.Context, pubkey string) (readmodel.TrustState, error) {
			return readmodel.TrustState{
				Pubkey:      pubkey,
				Qualified:   true,
				IsSeed:      true,
				HopDistance: &hops,
				Rank:        &rank,
			}, nil
		},
		countRankedPubkeysFn: func(context.Context) (int64, error) {
			return 80, nil
		},
	})

	out, err := svc.GetProfilePublicSummary(context.Background(), "pk_1")
	if err != nil {
		t.Fatalf("GetProfilePublicSummary returned error: %v", err)
	}
	if out.Profile.Pubkey != "pk_1" || out.Profile.MetadataEventID != "meta_1" || out.Profile.MetadataCreatedAt != 321 {
		t.Fatalf("unexpected profile summary profile data: %#v", out.Profile)
	}
	if out.Stats.FollowerCount != 6 || out.Stats.FollowingCount != 3 || out.Stats.NoteCount != 11 || out.Stats.ReplyCount != 2 {
		t.Fatalf("unexpected profile summary stats: %#v", out.Stats)
	}
	if out.Stats.RecentActivityAt == nil || *out.Stats.RecentActivityAt != 900 {
		t.Fatalf("unexpected recent activity: %#v", out.Stats.RecentActivityAt)
	}
	if out.TrustSummary == nil || out.TrustSummary.Tier != "seed" {
		t.Fatalf("expected seed trust summary, got %#v", out.TrustSummary)
	}
	if out.TrustSummary.Percentile == nil || *out.TrustSummary.Percentile != 5.0 {
		t.Fatalf("expected percentile 5.0, got %#v", out.TrustSummary.Percentile)
	}
}

func TestGetProfilePublicSummary_MissingProfile(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, fakeReader{
		getProfileByPubkeyFn: func(context.Context, string) (store.ProfileProjection, error) {
			return store.ProfileProjection{}, store.ErrNotFound
		},
	})
	_, err := svc.GetProfilePublicSummary(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if !IsNotFound(err) {
		t.Fatalf("expected not found error, got %v", err)
	}
}
