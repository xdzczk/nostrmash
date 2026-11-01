package derivation

import "testing"

func TestProfileTrendingEngagement_ReactionFarmUsesCheapFloor(t *testing.T) {
	reactions := weightedPeople(20)
	got := profileTrendingEngagement(nil, reactions, nil, nil)
	want := roundProfileEngagement(cheapEngagementSignal(20, 0, 0))
	if got != want {
		t.Fatalf("reaction farm = %v, want the cheap floor %v", got, want)
	}
	if got >= 20 {
		t.Fatalf("reaction farm counted in full: %v", got)
	}
}

func TestProfileTrendingEngagement_RepliesUnlockLikes(t *testing.T) {
	replies := []weightedReply{
		{Pubkey: "a", Content: "archival relays still matter for this conversation", Weight: 1},
		{Pubkey: "b", Content: "paid filters changed who actually showed up", Weight: 1},
	}
	reactions := weightedPeople(3)
	got := profileTrendingEngagement(replies, reactions, nil, nil)
	if got != 5 {
		t.Fatalf("replies plus likes inside the budget = %v, want 5", got)
	}
}

func TestProfileTrendingEngagement_GreetingPileDoesNotUnlockLikes(t *testing.T) {
	replies := make([]weightedReply, 8)
	for i := range replies {
		replies[i] = weightedReply{Pubkey: string(rune('a' + i)), Content: "GM", Weight: 1}
	}
	reactions := weightedPeople(10)
	got := profileTrendingEngagement(replies, reactions, nil, nil)
	want := roundProfileEngagement(1 + cheapEngagementSignal(10, 0, 0))
	if got != want {
		t.Fatalf("greeting pile = %v, want one pile vote plus the cheap floor %v", got, want)
	}
	if got >= 8 {
		t.Fatalf("greeting pile counted per person: %v", got)
	}
}

func TestProfileTrendingEngagement_ZapDoesNotUnlockReactionFarm(t *testing.T) {
	zaps := []weightedReply{{Pubkey: "z", Weight: 1}}
	reactions := weightedPeople(100)
	got := profileTrendingEngagement(nil, reactions, nil, zaps)
	want := roundProfileEngagement(1 + cheapEngagementSignal(100, 0, 1))
	if got != want {
		t.Fatalf("zap plus reaction farm = %v, want %v", got, want)
	}
	if got > 30 {
		t.Fatalf("one zap unlocked too much of the reaction pile: %v", got)
	}
}

func TestProfileTrendingEngagement_RepostFromPileIsNotASecondVote(t *testing.T) {
	replies := []weightedReply{
		{Pubkey: "p1", Content: "GM", Weight: 1},
		{Pubkey: "p2", Content: "good morning", Weight: 1},
	}
	reposts := []weightedReply{
		{Pubkey: "p1", Weight: 1},
		{Pubkey: "other", Weight: 1},
	}
	got := profileTrendingEngagement(replies, nil, reposts, nil)
	// Pile vote 1. The pile member's repost is omitted. The other repost is
	// 2 cheap points with nothing to unlock it, so it counts at 0.12.
	want := roundProfileEngagement(1 + cheapEngagementSignal(0, 1, 0))
	if got != want {
		t.Fatalf("pile repost = %v, want %v", got, want)
	}
}

func TestProfileTrendingEngagement_RepostsCostTwoBeforeTheFloor(t *testing.T) {
	replies := []weightedReply{{Pubkey: "a", Content: "archival relays still matter for this conversation", Weight: 1}}
	reposts := weightedPeople(3)
	got := profileTrendingEngagement(replies, nil, reposts, nil)
	want := roundProfileEngagement(1 + cheapEngagementSignal(0, 3, 1))
	if got != want {
		t.Fatalf("reposts = %v, want %v", got, want)
	}
	if got == 4 {
		t.Fatalf("reposts were counted as one point each: %v", got)
	}
}

func TestProfileRisingEngagement_OnePersonNotTwoSignals(t *testing.T) {
	replies := []weightedReply{{Pubkey: "a", Content: "archival relays still matter for this conversation", Weight: 1}}
	reactions := []weightedReply{{Pubkey: "a", Weight: 1}}
	if got := profileTrendingEngagement(replies, reactions, nil, nil); got != 2 {
		t.Fatalf("trending = %v, want both signals", got)
	}
	if got := profileRisingEngagement(replies, reactions, nil, nil); got != 1 {
		t.Fatalf("rising = %v, want one person", got)
	}
}

func TestProfileRisingEngagement_ReactionFarmUsesCheapFloor(t *testing.T) {
	reactions := weightedPeople(12)
	got := profileRisingEngagement(nil, reactions, nil, nil)
	want := roundProfileEngagement(cheapEngagementSignal(12, 0, 0))
	if got != want {
		t.Fatalf("rising reaction farm = %v, want %v", got, want)
	}
}

func TestProfileRisingEngagement_ZapperLeavesTheGreetingPile(t *testing.T) {
	replies := []weightedReply{
		{Pubkey: "z", Content: "GM", Weight: 1},
		{Pubkey: "g", Content: "good morning", Weight: 1},
	}
	zaps := []weightedReply{{Pubkey: "z", Weight: 1}}
	reactions := weightedPeople(5)
	got := profileRisingEngagement(replies, reactions, nil, zaps)
	// The zapper is one substantive person. The other greeting stays the pile
	// and does not open likes, so the five reactions stay on the floor.
	// Original share is 0 because both replies were the pile.
	want := roundProfileEngagement(1 + 1 + cheapEngagementSignal(5, 0, 0))
	if got != want {
		t.Fatalf("zapper in a greeting pile = %v, want %v", got, want)
	}
}

func TestProfileRisingEngagement_RepostAndLikeAreOnePerson(t *testing.T) {
	person := []weightedReply{{Pubkey: "a", Weight: 1}}
	got := profileRisingEngagement(nil, person, person, nil)
	want := roundProfileEngagement(cheapEngagementSignal(0, 1, 0))
	if got != want {
		t.Fatalf("repost plus like = %v, want one repost at the floor %v", got, want)
	}
}

func TestProfileRisingEngagement_ZapDoesNotTurnTheRestOfThePileIntoReplies(t *testing.T) {
	sentenceA := "archival relays still matter when the conversation moves"
	sentenceB := "paid inbox filters changed who was willing to post"
	replies := []weightedReply{
		{Pubkey: "p1", Content: sentenceA, Weight: 1},
		{Pubkey: "p2", Content: sentenceA, Weight: 1},
		{Pubkey: "p3", Content: sentenceB, Weight: 1},
		{Pubkey: "p4", Content: sentenceB, Weight: 1},
	}
	zaps := []weightedReply{{Pubkey: "p1", Weight: 1}}
	reactions := []weightedReply{
		{Pubkey: "r1", Weight: 1},
		{Pubkey: "r2", Weight: 1},
		{Pubkey: "r3", Weight: 1},
		{Pubkey: "r4", Weight: 1},
	}
	got := profileRisingEngagement(replies, reactions, nil, zaps)
	// Two copied sentences are two pile votes. The zapper steps out of the
	// first pile and counts once. Nothing in that crowd opens likes.
	want := roundProfileEngagement(2 + 1 + cheapEngagementSignal(4, 0, 0))
	if got != want {
		t.Fatalf("two piles plus a zap = %v, want %v", got, want)
	}
}

func TestDiscountProfileVotes_WindowsStaySeparate(t *testing.T) {
	replies := []profileVote{{
		Pubkey:     "a",
		Content:    "archival relays still matter for this conversation",
		Weight:     1,
		Trending7d: true,
		Rising7d:   true,
	}}
	got := discountProfileVotes(replies, nil, nil, nil)
	if got.trending24h != 0 || got.rising24h != 0 {
		t.Fatalf("7d reply leaked into 24h: %+v", got)
	}
	if got.trending7d != 1 || got.rising7d != 1 || got.trending30d != 0 || got.rising30d != 1 {
		t.Fatalf("window split = %+v", got)
	}
}

func weightedPeople(n int) []weightedReply {
	out := make([]weightedReply, n)
	for i := range out {
		out[i] = weightedReply{Pubkey: "p" + string(rune('A'+i)), Weight: 1}
	}
	return out
}
