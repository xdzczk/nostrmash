package api

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/query"
)

func reasonCodes(reasons []query.DiscoveryReason) []string {
	codes := make([]string, 0, len(reasons))
	for _, reason := range reasons {
		codes = append(codes, reason.Code)
	}
	return codes
}

func TestBuildProfileRanking_TrendingSurface(t *testing.T) {
	profile := query.TrendingProfile{
		Pubkey:                   "pk_trending",
		Score:                    42.5,
		RecentPostCount:          5,
		RecentReplyCount:         0,
		RecentEngagementReceived: 100,
		RecentNewFollowers:       30,
		FollowerCount:            10,
	}

	ranking := buildProfileRanking(profile, 1, discoverySurfaceTrending)

	codes := reasonCodes(ranking.Reasons)
	if len(codes) == 0 || codes[0] != "engagement_quality" {
		t.Fatalf("expected engagement_quality to lead trending reasons, got %v", codes)
	}
	for _, code := range codes {
		if code == "follower_growth" {
			t.Fatalf("trending surface should not surface follower_growth even when new followers are present, got %v", codes)
		}
	}
	wantCodes := []string{"engagement_quality", "publishing_momentum", "engagement_received"}
	if len(codes) != len(wantCodes) {
		t.Fatalf("unexpected reason set for trending surface: got %v want %v", codes, wantCodes)
	}
	for i, want := range wantCodes {
		if codes[i] != want {
			t.Fatalf("reason[%d] = %q want %q (got %v)", i, codes[i], want, codes)
		}
	}

	// Trending confidence sample excludes follower growth entirely.
	wantSample := profile.RecentPostCount + profile.RecentReplyCount + profile.RecentEngagementReceived
	if got, want := discoveryConfidence(wantSample), ranking.Confidence; got != want {
		t.Fatalf("confidence = %q want %q (sample=%d)", want, got, wantSample)
	}
}

func TestBuildProfileRanking_RisingSurface(t *testing.T) {
	t.Run("follower growth present leads and relative engagement still surfaces", func(t *testing.T) {
		profile := query.TrendingProfile{
			Pubkey:                   "pk_rising",
			Score:                    12.0,
			RecentPostCount:          3,
			RecentEngagementReceived: 40,
			RecentNewFollowers:       15,
			FollowerCount:            50,
		}

		ranking := buildProfileRanking(profile, 1, discoverySurfaceRising)
		codes := reasonCodes(ranking.Reasons)
		wantCodes := []string{"follower_growth", "relative_engagement_growth", "engagement_received", "publishing_momentum"}
		if len(codes) != len(wantCodes) {
			t.Fatalf("unexpected reason set for rising surface: got %v want %v", codes, wantCodes)
		}
		for i, want := range wantCodes {
			if codes[i] != want {
				t.Fatalf("reason[%d] = %q want %q (got %v)", i, codes[i], want, codes)
			}
		}

		wantSample := profile.RecentNewFollowers + profile.RecentEngagementReceived
		if got, want := discoveryConfidence(wantSample), ranking.Confidence; got != want {
			t.Fatalf("confidence = %q want %q (sample=%d)", want, got, wantSample)
		}
	})

	t.Run("no new followers still surfaces relative engagement growth, not follower_growth", func(t *testing.T) {
		profile := query.TrendingProfile{
			Pubkey:                   "pk_no_followers",
			Score:                    8.0,
			RecentPostCount:          2,
			RecentEngagementReceived: 25,
			RecentNewFollowers:       0,
			FollowerCount:            5,
		}

		ranking := buildProfileRanking(profile, 2, discoverySurfaceRising)
		codes := reasonCodes(ranking.Reasons)
		if len(codes) == 0 || codes[0] != "relative_engagement_growth" {
			t.Fatalf("expected relative_engagement_growth to lead when there's no follower growth, got %v", codes)
		}
		for _, code := range codes {
			if code == "follower_growth" {
				t.Fatalf("did not expect follower_growth with zero new followers, got %v", codes)
			}
		}
	})
}

func TestBuildProfileRanking_TrendingSurface_SinglePostIsNotPublishingMomentum(t *testing.T) {
	// A single post with no engagement isn't meaningful "momentum" on its
	// own; leading with it read as a weak/uninformative "why now" reason.
	profile := query.TrendingProfile{
		Pubkey:          "pk_one_post",
		Score:           1.2,
		RecentPostCount: 1,
	}

	ranking := buildProfileRanking(profile, 1, discoverySurfaceTrending)
	codes := reasonCodes(ranking.Reasons)
	for _, code := range codes {
		if code == "publishing_momentum" {
			t.Fatalf("did not expect publishing_momentum for a single post, got %v", codes)
		}
	}

	multiPost := profile
	multiPost.RecentPostCount = 2
	multiRanking := buildProfileRanking(multiPost, 1, discoverySurfaceTrending)
	multiCodes := reasonCodes(multiRanking.Reasons)
	found := false
	for _, code := range multiCodes {
		if code == "publishing_momentum" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected publishing_momentum once post count is above the noise floor, got %v", multiCodes)
	}
}

func TestBuildProfileRanking_RisingSurface_TrivialNewFollowersAreNotFollowerGrowth(t *testing.T) {
	// A couple of new followers on a brand-new account is common noise, not
	// a meaningful "follower growth" signal -- surfacing it made the rising
	// list look like it was just listing every newly-created account.
	profile := query.TrendingProfile{
		Pubkey:             "pk_barely_new",
		Score:              1.0,
		RecentNewFollowers: 2,
		FollowerCount:      2,
	}

	ranking := buildProfileRanking(profile, 1, discoverySurfaceRising)
	codes := reasonCodes(ranking.Reasons)
	for _, code := range codes {
		if code == "follower_growth" {
			t.Fatalf("did not expect follower_growth at/below the noise floor, got %v", codes)
		}
	}

	substantial := profile
	substantial.RecentNewFollowers = 60
	substantial.FollowerCount = 400
	substantialRanking := buildProfileRanking(substantial, 1, discoverySurfaceRising)
	substantialCodes := reasonCodes(substantialRanking.Reasons)
	if len(substantialCodes) == 0 || substantialCodes[0] != "follower_growth" {
		t.Fatalf("expected follower_growth to lead once new followers are above the noise floor, got %v", substantialCodes)
	}
}

func TestBuildProfileRanking_UsesScoredInputsWhenPresent(t *testing.T) {
	// Raw counters look impressive (bot farm), but the score credited
	// nothing. Reasons and confidence must follow the scored values so the
	// card cannot advertise engagement the ranking did not use.
	zero := 0.0
	profile := query.TrendingProfile{
		Pubkey:                   "pk_farmed",
		Score:                    0.8,
		RecentPostCount:          2,
		RecentEngagementReceived: 50,
		RecentNewFollowers:       40,
		FollowerCount:            40,
		ScoredEngagementReceived: &zero,
		ScoredNewFollowers:       &zero,
	}

	trending := buildProfileRanking(profile, 1, discoverySurfaceTrending)
	for _, code := range reasonCodes(trending.Reasons) {
		if code == "engagement_quality" || code == "engagement_received" {
			t.Fatalf("did not expect engagement reasons when scored engagement is 0, got %v", reasonCodes(trending.Reasons))
		}
	}

	rising := buildProfileRanking(profile, 1, discoverySurfaceRising)
	for _, code := range reasonCodes(rising.Reasons) {
		if code == "follower_growth" || code == "relative_engagement_growth" || code == "engagement_received" {
			t.Fatalf("did not expect growth/engagement reasons when scored inputs are 0, got %v", reasonCodes(rising.Reasons))
		}
	}
}

func TestBuildProfileRanking_EmptyReasonsAreNonNilAndDefaultToRising(t *testing.T) {
	profile := query.TrendingProfile{Pubkey: "pk_quiet"}

	trending := buildProfileRanking(profile, 1, discoverySurfaceTrending)
	if trending.Reasons == nil {
		t.Fatal("expected non-nil (possibly empty) reasons slice for trending surface")
	}
	if len(trending.Reasons) != 0 {
		t.Fatalf("expected no reasons for a profile with no activity, got %v", reasonCodes(trending.Reasons))
	}

	// Any surface value other than "trending" (including unset/legacy
	// callers) falls back to the rising reason set.
	rising := buildProfileRanking(profile, 1, "")
	if rising.Reasons == nil {
		t.Fatal("expected non-nil (possibly empty) reasons slice for default surface")
	}
}
