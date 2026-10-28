package api

import (
	"testing"

	"github.com/xdzczk/nostrmash/internal/derivation"
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
	if len(codes) != 1 || codes[0] != "engagement_quality" {
		t.Fatalf("expected a single monotonic engagement_quality reason, got %v", codes)
	}
	for _, code := range codes {
		if code == "follower_growth" {
			t.Fatalf("trending surface should not surface follower_growth even when new followers are present, got %v", codes)
		}
	}

	// Trending confidence sample is root posts plus the engagement the
	// score used. Replies and follower growth are not part of it.
	wantSample := profile.RecentPostCount + profile.RecentEngagementReceived
	if got, want := discoveryConfidence(wantSample), ranking.Confidence; got != want {
		t.Fatalf("confidence = %q want %q (sample=%d)", want, got, wantSample)
	}
}

func TestBuildProfileRanking_RisingSurface(t *testing.T) {
	t.Run("follower growth leads when that term dominates the score", func(t *testing.T) {
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
		if len(codes) != 1 || codes[0] != "follower_growth" {
			t.Fatalf("expected a single follower_growth reason, got %v", codes)
		}
		if got := ranking.Reasons[0].Evidence[0].Metric; got != "follower_momentum" {
			t.Fatalf("follower evidence metric = %q, want audience-adjusted momentum", got)
		}

		wantSample := profile.RecentNewFollowers + profile.RecentEngagementReceived
		if got, want := discoveryConfidence(wantSample), ranking.Confidence; got != want {
			t.Fatalf("confidence = %q want %q (sample=%d)", want, got, wantSample)
		}
	})

	t.Run("a single interaction is below the engagement reason floor", func(t *testing.T) {
		// Mirrors the score's risingEngagementNoiseFloor: one (weighted)
		// interaction contributes nothing to the rising score, so it must
		// not be advertised as an engagement-based reason either.
		profile := query.TrendingProfile{
			Pubkey:                   "pk_lone_boost",
			Score:                    0.5,
			RecentPostCount:          2,
			RecentEngagementReceived: 1,
			FollowerCount:            3,
		}

		ranking := buildProfileRanking(profile, 3, discoverySurfaceRising)
		for _, code := range reasonCodes(ranking.Reasons) {
			if code == "relative_engagement_growth" || code == "engagement_received" {
				t.Fatalf("did not expect engagement reasons for a single interaction, got %v", reasonCodes(ranking.Reasons))
			}
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
	multiCodes := reasonCodes(buildProfileRanking(multiPost, 1, discoverySurfaceTrending).Reasons)
	for _, code := range append(codes, multiCodes...) {
		if code == "publishing_momentum" || code == "engagement_quality" {
			t.Fatalf("posts without engagement are not a per-post reason, got single %v multi %v", codes, multiCodes)
		}
	}
	if len(codes) != 1 || codes[0] != "combined_engagement" || len(multiCodes) != 1 || multiCodes[0] != "combined_engagement" {
		t.Fatalf("expected combined engagement when no single metric applies, got single %v multi %v", codes, multiCodes)
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

	stillSmall := profile
	stillSmall.RecentNewFollowers = 11
	stillSmallCodes := reasonCodes(buildProfileRanking(stillSmall, 1, discoverySurfaceRising).Reasons)
	for _, code := range stillSmallCodes {
		if code == "follower_growth" {
			t.Fatalf("did not expect follower_growth below 12 raw new followers, got %v", stillSmallCodes)
		}
	}

	clearsFloor := profile
	clearsFloor.RecentNewFollowers = 12
	clearsFloorCodes := reasonCodes(buildProfileRanking(clearsFloor, 1, discoverySurfaceRising).Reasons)
	if len(clearsFloorCodes) == 0 || clearsFloorCodes[0] != "follower_growth" {
		t.Fatalf("expected follower_growth once new followers clear the credit minimum, got %v", clearsFloorCodes)
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

func TestBuildNoteRankings_DoesNotCiteACountThatRisesDownThePage(t *testing.T) {
	notes := []query.TrendingNote{
		{EventID: "lead", ReplyCount: 69, ZapCount: 21, CreatedAt: 300, Score: 10},
		{EventID: "second", ReplyCount: 92, ZapCount: 28, CreatedAt: 100, Score: 9},
		{EventID: "third", ReplyCount: 10, ZapCount: 1, CreatedAt: 400, Score: 8},
	}
	rankings := buildNoteRankings(notes, "24h")
	if got := reasonCodes(rankings[0].Reasons); len(got) != 1 || got[0] != "combined_engagement" {
		t.Fatalf("lead must not claim replies, zaps, or recency when each rises below it, got %v", got)
	}
	if len(rankings[0].Reasons[0].Evidence) != 0 {
		t.Fatalf("combined engagement has no number, got %#v", rankings[0].Reasons[0].Evidence)
	}
	if got := reasonCodes(rankings[1].Reasons); len(got) != 1 || got[0] != "reply_velocity" {
		t.Fatalf("second note's reply count does not rise further down the page, got %v", got)
	}
	if rankings[1].Reasons[0].Evidence[0].Value != 92 {
		t.Fatalf("second note evidence = %v, want 92 replies", rankings[1].Reasons[0].Evidence)
	}
}

func TestAssignProfileRankings_TrendingCitesPerPostOnlyWhenItDoesNotRise(t *testing.T) {
	rows := []query.TrendingProfile{
		{Pubkey: "ben", Score: 20, RecentPostCount: 10, RecentEngagementReceived: 240},
		{Pubkey: "calle", Score: 10, RecentPostCount: 2, RecentEngagementReceived: 200},
	}
	day := assignProfileRankings(rows, discoverySurfaceTrending, "24h")
	if got := reasonCodes(day[0].Reasons); len(got) != 1 || got[0] != "engagement_received" {
		t.Fatalf("24h lead per-post rate rises on the next row, got %v", got)
	}
	if day[0].Reasons[0].Evidence[0].Value != 240 {
		t.Fatalf("lead evidence = %v, want 240 interactions", day[0].Reasons[0].Evidence)
	}
	if got := reasonCodes(day[1].Reasons); len(got) != 1 || got[0] != "engagement_quality" {
		t.Fatalf("lower row can cite its per-post rate, got %v", got)
	}

	week := assignProfileRankings(rows, discoverySurfaceTrending, "7d")
	for i, ranking := range week {
		if got := reasonCodes(ranking.Reasons); len(got) != 1 || got[0] != "engagement_received" {
			t.Fatalf("7d row %d must not cite a per-post rate, got %v", i, got)
		}
	}
}

func TestRankings_30dDoesNotCiteRecency(t *testing.T) {
	notes := []query.TrendingNote{
		{EventID: "newer", ReplyCount: 2, CreatedAt: 300, Score: 10},
		{EventID: "older", ReplyCount: 9, CreatedAt: 100, Score: 9},
	}
	day := buildNoteRankings(notes, "24h")
	if got := reasonCodes(day[0].Reasons); len(got) != 1 || got[0] != "recency" {
		t.Fatalf("24h lead is newer with fewer replies, got %v", got)
	}
	month := buildNoteRankings(notes, "30d")
	if got := reasonCodes(month[0].Reasons); len(got) != 1 || got[0] != "combined_engagement" {
		t.Fatalf("30d list must not cite recency, got %v", got)
	}

	recent := int64(300)
	quiet := int64(100)
	profiles := []query.TrendingProfile{
		{Pubkey: "newer", Score: 10, RecentEngagementReceived: 4, RecentActivityAt: &recent},
		{Pubkey: "older", Score: 9, RecentEngagementReceived: 40, RecentActivityAt: &quiet},
	}
	week := assignProfileRankings(profiles, discoverySurfaceTrending, "7d")
	if got := reasonCodes(week[0].Reasons); len(got) != 1 || got[0] != "recency" {
		t.Fatalf("7d profile lead is the more recent account, got %v", got)
	}
	monthProfiles := assignProfileRankings(profiles, discoverySurfaceTrending, "30d")
	if got := reasonCodes(monthProfiles[0].Reasons); len(got) != 1 || got[0] != "combined_engagement" {
		t.Fatalf("30d profile list must not cite recency, got %v", got)
	}
}

func TestAssignProfileRankings_RisingCitesAudienceAdjustedMomentum(t *testing.T) {
	rows := []query.TrendingProfile{
		{Pubkey: "sovereign", Score: 40, RecentNewFollowers: 37, FollowerCount: 100},
		{Pubkey: "nostrich", Score: 5, RecentNewFollowers: 44, FollowerCount: 5000},
	}
	rankings := assignProfileRankings(rows, discoverySurfaceRising, "7d")
	lead := rankings[0].Reasons[0]
	next := rankings[1].Reasons[0]
	if lead.Code != "follower_growth" || next.Code != "follower_growth" {
		t.Fatalf("both rows are follower-momentum leads, got %s then %s", lead.Code, next.Code)
	}
	_, _, wantLead := derivation.FollowerMomentum(100, 37)
	_, _, wantNext := derivation.FollowerMomentum(5000, 44)
	if lead.Evidence[0].Value != wantLead || next.Evidence[0].Value != wantNext {
		t.Fatalf("momentum = %v then %v, want %v then %v", lead.Evidence[0].Value, next.Evidence[0].Value, wantLead, wantNext)
	}
	if lead.Evidence[0].Value <= next.Evidence[0].Value {
		t.Fatalf("printed momentum rose down the page: %v then %v", lead.Evidence[0].Value, next.Evidence[0].Value)
	}
	if lead.Evidence[0].Metric != "follower_momentum" || next.Evidence[0].Value == 44 {
		t.Fatalf("evidence must be adjusted momentum, not the raw +44, got %#v %#v", lead.Evidence, next.Evidence)
	}
}

func TestBuildHashtagAndDomainRankings_LeadWithAuthorBreadth(t *testing.T) {
	topic := buildHashtagRanking(query.TrendingHashtag{Hashtag: "news", EventCount: 1576, UniqueAuthors: 455}, 1)
	if topic.Score != 455 || reasonCodes(topic.Reasons)[0] != "author_breadth" {
		t.Fatalf("hashtag ranking = score %v reasons %v", topic.Score, reasonCodes(topic.Reasons))
	}
	domain := buildDomainRanking(query.DomainSummary{
		Domain: "github.com",
		Activity: query.DomainActivityStats{
			Last24h: query.DomainActivity{LinkCount: 80, UniqueAuthors: 12},
			Last7d:  query.DomainActivity{LinkCount: 400, UniqueAuthors: 40},
		},
	}, 1, "7d")
	if domain.Score != 40 || reasonCodes(domain.Reasons)[0] != "author_breadth" {
		t.Fatalf("domain ranking = score %v reasons %v", domain.Score, reasonCodes(domain.Reasons))
	}
	if domain.Reasons[1].Code != "link_circulation" || domain.Reasons[1].Evidence[0].Value != 400 {
		t.Fatalf("domain volume reason = %#v", domain.Reasons)
	}
}
