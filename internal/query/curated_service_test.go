package query

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xdzczk/nostrmash/internal/model"
	"github.com/xdzczk/nostrmash/internal/readmodel"
	"github.com/xdzczk/nostrmash/internal/store"
	storeread "github.com/xdzczk/nostrmash/internal/store/read"
	storetrust "github.com/xdzczk/nostrmash/internal/store/trust"
)

func TestGetNetworkStatsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getNetworkStatsFn: func(context.Context) (storeread.NetworkStats, error) {
			return storeread.NetworkStats{Events: 11, Profiles: 22, Relays: 33}, nil
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

func TestGetPublicDiscoveryNetworkStats(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedReaderWithPublicStats{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getPublicNetworkStatsFn: func(context.Context, int) (storeread.PublicDiscoveryNetworkStats, error) {
			return storeread.PublicDiscoveryNetworkStats{
				EventsIngested:    44,
				ProjectedProfiles: 12,
				Relays:            8,
				ActiveAuthors:     storeread.WindowedCount{Last24h: 4, Last7d: 10},
				NoteVolume:        storeread.WindowedCount{Last24h: 17, Last7d: 61},
			}, nil
		},
	})
	out, err := svc.GetPublicDiscoveryNetworkStats(context.Background(), 10)
	if err != nil {
		t.Fatalf("GetPublicDiscoveryNetworkStats returned error: %v", err)
	}
	if out.EventsIngested != 44 || out.ProjectedProfiles != 12 || out.NoteVolume.Last7d != 61 {
		t.Fatalf("unexpected public network stats: %#v", out)
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
		getCuratedRecommendedReadsFn: func(context.Context, int) ([]storeread.CuratedRecommendedRead, error) {
			return []storeread.CuratedRecommendedRead{{
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
		getCuratedReadsTopicsFn: func(context.Context, int) ([]storeread.CuratedReadsTopic, error) {
			return []storeread.CuratedReadsTopic{{Topic: "nostr", Rank: 2}}, nil
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
		getCuratedFeaturedAuthorsFn: func(context.Context, int) ([]storeread.CuratedFeaturedAuthor, error) {
			return []storeread.CuratedFeaturedAuthor{{Pubkey: "pk-1", Rank: 3}}, nil
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

func TestGetTrendingHashtagsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingHashtagsFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingHashtag, error) {
			return []storeread.TrendingHashtag{{Hashtag: "nostr", EventCount: 12, UniqueAuthors: 9}}, nil
		},
	})
	out, err := svc.GetTrendingHashtags(context.Background(), 24*time.Hour, 5, 0)
	if err != nil {
		t.Fatalf("GetTrendingHashtags returned error: %v", err)
	}
	if len(out) != 1 || out[0].Hashtag != "nostr" || out[0].EventCount != 12 || out[0].UniqueAuthors != 9 {
		t.Fatalf("unexpected trending hashtags: %#v", out)
	}
}

func TestGetTrendingNotesMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingNotesFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{{
				EventID:       "note-1",
				AuthorPubkey:  "pk-1",
				CreatedAt:     1700000000,
				Content:       "hello",
				Language:      "en",
				ReplyCount:    3,
				RepostCount:   2,
				ReactionCount: 5,
				ZapCount:      1,
				ZapMSats:      21000,
				Score:         12.75,
			}}, nil
		},
	})
	out, err := svc.GetTrendingNotes(context.Background(), 24*time.Hour, 5, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes returned error: %v", err)
	}
	if len(out) != 1 || out[0].EventID != "note-1" || out[0].Score != 12.75 || out[0].ZapMSats != 21000 || out[0].Language != "en" {
		t.Fatalf("unexpected trending notes: %#v", out)
	}
}

func TestGetHotConversationsMapsLegacyStoreModel(t *testing.T) {
	t.Parallel()
	svc := mustNewService(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getHotConversationsFn: func(context.Context, time.Duration, int, int) ([]store.HotConversation, error) {
			return []store.HotConversation{{
				RootEventID:      "root-1",
				AuthorPubkey:     "pk-1",
				CreatedAt:        1700000000,
				Content:          "hot thread",
				ReplyCount:       8,
				ParticipantCount: 5,
				LastActivityAt:   1700000300,
				Replies24h:       4,
				Replies7d:        8,
				VelocityScore:    4.75,
				Consistency:      "eventual",
			}}, nil
		},
	})
	out, err := svc.GetHotConversations(context.Background(), 24*time.Hour, 5, 0)
	if err != nil {
		t.Fatalf("GetHotConversations returned error: %v", err)
	}
	if len(out) != 1 || out[0].RootEventID != "root-1" || out[0].VelocityScore != 4.75 || out[0].Replies24h != 4 {
		t.Fatalf("unexpected hot conversations: %#v", out)
	}
}

func TestDiscoveryTrustPolicy_TrendingNotesPreferTrusted(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingNotesFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{
				{EventID: "n1", AuthorPubkey: "u1", Score: 100},
				{EventID: "n2", AuthorPubkey: "u2", Score: 99},
				{EventID: "n3", AuthorPubkey: "u3", Score: 98},
			}, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			out := map[string]readmodel.TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "u3"}
			}
			return out, nil
		},
	}, ServiceOptions{
		DiscoveryCandidateTrustMode: trustModePreferTrusted,
	})
	out, err := svc.GetTrendingNotes(context.Background(), 24*time.Hour, 3, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes returned error: %v", err)
	}
	if len(out) != 3 || out[0].EventID != "n3" || out[1].EventID != "n1" || out[2].EventID != "n2" {
		t.Fatalf("unexpected trust-ranked notes: %#v", out)
	}
}

func TestDiscoveryTrustPolicy_TrendingNotesTrustedOnly(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingNotesFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{
				{EventID: "n1", AuthorPubkey: "u1"},
				{EventID: "n2", AuthorPubkey: "u2"},
				{EventID: "n3", AuthorPubkey: "u3"},
			}, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			out := map[string]readmodel.TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "u2" || pubkey == "u3"}
			}
			return out, nil
		},
	}, ServiceOptions{
		DiscoveryCandidateTrustMode: trustModeTrustedOnly,
	})
	out, err := svc.GetTrendingNotes(context.Background(), 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes returned error: %v", err)
	}
	if len(out) != 2 || out[0].EventID != "n2" || out[1].EventID != "n3" {
		t.Fatalf("unexpected trusted-only notes: %#v", out)
	}
}

func TestDiscoveryTrustPolicy_UsesProjectedTrustQualifiedNotesWhenReady(t *testing.T) {
	t.Parallel()
	fallbackQualificationCalls := 0
	svc := mustNewServiceWithOptions(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrustQualifiedTrendingNotesFn: func(context.Context, time.Duration, int, int, string, storetrust.TrustQualificationPolicy, time.Duration) ([]storeread.TrustQualifiedTrendingNote, bool, error) {
			return []storeread.TrustQualifiedTrendingNote{
				{
					Note:    storeread.TrendingNote{EventID: "projected_1", AuthorPubkey: "trusted_pk", Score: 10},
					Trusted: true,
				},
				{
					Note:    storeread.TrendingNote{EventID: "projected_2", AuthorPubkey: "open_pk", Score: 9},
					Trusted: false,
				},
			}, true, nil
		},
		getTrustQualificationsFn: func(_ context.Context, _ []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			fallbackQualificationCalls++
			return map[string]readmodel.TrustQualification{}, nil
		},
	}, ServiceOptions{
		DiscoveryCandidateTrustMode: trustModePreferTrusted,
	})
	out, err := svc.GetTrendingNotes(context.Background(), 24*time.Hour, 5, 0)
	if err != nil {
		t.Fatalf("GetTrendingNotes returned error: %v", err)
	}
	if len(out) != 2 || out[0].EventID != "projected_1" || out[1].EventID != "projected_2" {
		t.Fatalf("unexpected projected trust-ranked notes: %#v", out)
	}
	if fallbackQualificationCalls != 0 {
		t.Fatalf("expected no fallback qualification calls when projection is ready, got %d", fallbackQualificationCalls)
	}
}

func TestDiscoveryTrustPolicy_ProfilesApplyConsistentQualification(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingProfilesFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingProfile, error) {
			return []storeread.TrendingProfile{
				{Pubkey: "p1", Score: 100},
				{Pubkey: "p2", Score: 99},
				{Pubkey: "p3", Score: 98},
			}, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			out := map[string]readmodel.TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "p3"}
			}
			return out, nil
		},
	}, ServiceOptions{
		DiscoveryCandidateTrustMode: trustModePreferTrusted,
	})
	out, err := svc.GetTrendingProfiles(context.Background(), 24*time.Hour, 3, 0)
	if err != nil {
		t.Fatalf("GetTrendingProfiles returned error: %v", err)
	}
	if len(out) != 3 || out[0].Pubkey != "p3" {
		t.Fatalf("unexpected trust-ranked profiles: %#v", out)
	}
}

func TestDiscoveryTrustPolicy_HashtagsDerivedFromTrustedNotes(t *testing.T) {
	t.Parallel()
	svc := mustNewServiceWithOptions(t, curatedLegacyReader{
		fakeReader: fakeReader{
			getEventRawByIDFn: func(context.Context, string) (json.RawMessage, error) {
				return nil, store.ErrNotFound
			},
		},
		getTrendingNotesFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error) {
			return []storeread.TrendingNote{
				{EventID: "n1", AuthorPubkey: "u1", Content: "#nostr #dev"},
				{EventID: "n2", AuthorPubkey: "u2", Content: "#spam"},
				{EventID: "n3", AuthorPubkey: "u3", Content: "#nostr #bitcoin"},
			}, nil
		},
		getTrendingHashtagsFn: func(context.Context, time.Duration, int, int) ([]storeread.TrendingHashtag, error) {
			t.Fatal("open hashtag query path should be bypassed under trust policy")
			return nil, nil
		},
		getTrustQualificationsFn: func(_ context.Context, pubkeys []string, _ readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error) {
			out := map[string]readmodel.TrustQualification{}
			for _, pubkey := range pubkeys {
				out[pubkey] = readmodel.TrustQualification{Pubkey: pubkey, Trusted: pubkey == "u1" || pubkey == "u3"}
			}
			return out, nil
		},
	}, ServiceOptions{
		DiscoveryCandidateTrustMode: trustModeTrustedOnly,
	})
	out, err := svc.GetTrendingHashtags(context.Background(), 24*time.Hour, 10, 0)
	if err != nil {
		t.Fatalf("GetTrendingHashtags returned error: %v", err)
	}
	if len(out) == 0 {
		t.Fatalf("expected trust-derived hashtags")
	}
	if out[0].Hashtag != "nostr" || out[0].EventCount != 2 {
		t.Fatalf("unexpected top hashtag ranking: %#v", out)
	}
	for _, row := range out {
		if row.Hashtag == "spam" {
			t.Fatalf("unexpected untrusted hashtag in trusted_only mode: %#v", out)
		}
	}
}

type curatedLegacyReader struct {
	fakeReader
	getNetworkStatsFn                   func(context.Context) (storeread.NetworkStats, error)
	getCuratedRecommendedReadsFn        func(context.Context, int) ([]storeread.CuratedRecommendedRead, error)
	getCuratedReadsTopicsFn             func(context.Context, int) ([]storeread.CuratedReadsTopic, error)
	getTrendingNotesFn                  func(context.Context, time.Duration, int, int) ([]storeread.TrendingNote, error)
	getHotConversationsFn               func(context.Context, time.Duration, int, int) ([]store.HotConversation, error)
	getTrustQualifiedTrendingNotesFn    func(context.Context, time.Duration, int, int, string, storetrust.TrustQualificationPolicy, time.Duration) ([]storeread.TrustQualifiedTrendingNote, bool, error)
	getTrendingHashtagsFn               func(context.Context, time.Duration, int, int) ([]storeread.TrendingHashtag, error)
	getTrendingProfilesFn               func(context.Context, time.Duration, int, int) ([]storeread.TrendingProfile, error)
	getTrustQualifiedTrendingProfilesFn func(context.Context, time.Duration, int, int, bool, string, storetrust.TrustQualificationPolicy, time.Duration) ([]storeread.TrustQualifiedTrendingProfile, bool, error)
	getCuratedFeaturedAuthorsFn         func(context.Context, int) ([]storeread.CuratedFeaturedAuthor, error)
	getTrustQualificationsFn            func(context.Context, []string, readmodel.TrustQualificationPolicy) (map[string]readmodel.TrustQualification, error)
	isTrustedAuthorFn                   func(context.Context, string, readmodel.TrustQualificationPolicy) (bool, error)
}

type curatedReaderWithPublicStats struct {
	fakeReader
	getPublicNetworkStatsFn func(context.Context, int) (storeread.PublicDiscoveryNetworkStats, error)
}

func (r curatedReaderWithPublicStats) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (r curatedReaderWithPublicStats) GetPublicDiscoveryNetworkStats(ctx context.Context, hashtagLimit int) (storeread.PublicDiscoveryNetworkStats, error) {
	if r.getPublicNetworkStatsFn == nil {
		return storeread.PublicDiscoveryNetworkStats{}, nil
	}
	return r.getPublicNetworkStatsFn(ctx, hashtagLimit)
}

func (r curatedLegacyReader) GetEventSeenOn(context.Context, string) ([]model.EventRelay, error) {
	return []model.EventRelay{}, nil
}

func (r curatedLegacyReader) GetNetworkStats(ctx context.Context) (storeread.NetworkStats, error) {
	if r.getNetworkStatsFn == nil {
		return storeread.NetworkStats{}, nil
	}
	return r.getNetworkStatsFn(ctx)
}

func (r curatedLegacyReader) GetCuratedRecommendedReads(ctx context.Context, limit int) ([]storeread.CuratedRecommendedRead, error) {
	if r.getCuratedRecommendedReadsFn == nil {
		return []storeread.CuratedRecommendedRead{}, nil
	}
	return r.getCuratedRecommendedReadsFn(ctx, limit)
}

func (r curatedLegacyReader) GetCuratedReadsTopics(ctx context.Context, limit int) ([]storeread.CuratedReadsTopic, error) {
	if r.getCuratedReadsTopicsFn == nil {
		return []storeread.CuratedReadsTopic{}, nil
	}
	return r.getCuratedReadsTopicsFn(ctx, limit)
}

func (r curatedLegacyReader) GetCuratedFeaturedAuthors(ctx context.Context, limit int) ([]storeread.CuratedFeaturedAuthor, error) {
	if r.getCuratedFeaturedAuthorsFn == nil {
		return []storeread.CuratedFeaturedAuthor{}, nil
	}
	return r.getCuratedFeaturedAuthorsFn(ctx, limit)
}

func (r curatedLegacyReader) GetTrendingHashtags(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]storeread.TrendingHashtag, error) {
	if r.getTrendingHashtagsFn == nil {
		return []storeread.TrendingHashtag{}, nil
	}
	return r.getTrendingHashtagsFn(ctx, window, limit, offset)
}

func (r curatedLegacyReader) GetTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]storeread.TrendingNote, error) {
	if r.getTrendingNotesFn == nil {
		return []storeread.TrendingNote{}, nil
	}
	return r.getTrendingNotesFn(ctx, window, limit, offset)
}

func (r curatedLegacyReader) GetHotConversations(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]store.HotConversation, error) {
	if r.getHotConversationsFn == nil {
		return []store.HotConversation{}, nil
	}
	return r.getHotConversationsFn(ctx, window, limit, offset)
}

func (r curatedLegacyReader) GetTrustQualifiedTrendingNotes(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	mode string,
	policy storetrust.TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]storeread.TrustQualifiedTrendingNote, bool, error) {
	if r.getTrustQualifiedTrendingNotesFn == nil {
		return nil, false, nil
	}
	return r.getTrustQualifiedTrendingNotesFn(ctx, window, limit, offset, mode, policy, maxStaleness)
}

func (r curatedLegacyReader) GetTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
) ([]storeread.TrendingProfile, error) {
	if r.getTrendingProfilesFn == nil {
		return []storeread.TrendingProfile{}, nil
	}
	return r.getTrendingProfilesFn(ctx, window, limit, offset)
}

func (r curatedLegacyReader) GetTrustQualifiedTrendingProfiles(
	ctx context.Context,
	window time.Duration,
	limit int,
	offset int,
	rising bool,
	mode string,
	policy storetrust.TrustQualificationPolicy,
	maxStaleness time.Duration,
) ([]storeread.TrustQualifiedTrendingProfile, bool, error) {
	if r.getTrustQualifiedTrendingProfilesFn == nil {
		return nil, false, nil
	}
	return r.getTrustQualifiedTrendingProfilesFn(ctx, window, limit, offset, rising, mode, policy, maxStaleness)
}

func (r curatedLegacyReader) GetTrustQualifications(
	ctx context.Context,
	pubkeys []string,
	policy readmodel.TrustQualificationPolicy,
) (map[string]readmodel.TrustQualification, error) {
	if r.getTrustQualificationsFn == nil {
		return map[string]readmodel.TrustQualification{}, nil
	}
	return r.getTrustQualificationsFn(ctx, pubkeys, policy)
}

func (r curatedLegacyReader) IsTrustedAuthor(
	ctx context.Context,
	pubkey string,
	policy readmodel.TrustQualificationPolicy,
) (bool, error) {
	if r.isTrustedAuthorFn == nil {
		return false, nil
	}
	return r.isTrustedAuthorFn(ctx, pubkey, policy)
}
