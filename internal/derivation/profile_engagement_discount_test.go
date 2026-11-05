package derivation

import "testing"

func TestProfileTrendingEngagement_ReactionFarmUsesCheapFloor(t *testing.T) {
	reactions := weightedPeople(20)
	got := profileTrendingEngagement(nil, reactions, nil, nil)
	want := roundProfileEngagement(reactionSignal(20, 0))
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

func TestProfileTrendingEngagement_MediaRepliesUnlockLikes(t *testing.T) {
	replies := []weightedReply{
		{Pubkey: "a", Content: "https://cdn.example/one.jpg", Weight: 1},
		{Pubkey: "b", Content: "https://cdn.example/two.jpg", Weight: 1},
	}
	reactions := weightedPeople(3)
	got := profileTrendingEngagement(replies, reactions, nil, nil)
	if got != 5 {
		t.Fatalf("media replies plus likes inside the budget = %v, want 5", got)
	}
}

func TestProfileTrendingEngagement_GreetingPileDoesNotUnlockLikes(t *testing.T) {
	replies := make([]weightedReply, 8)
	for i := range replies {
		replies[i] = weightedReply{Pubkey: string(rune('a' + i)), Content: "GM", Weight: 1}
	}
	reactions := weightedPeople(10)
	got := profileTrendingEngagement(replies, reactions, nil, nil)
	want := roundProfileEngagement(greetingPileVote(8, 1) + reactionSignal(10, 0))
	if got != want {
		t.Fatalf("greeting pile = %v, want sqrt(people) plus the cheap floor %v", got, want)
	}
	if got >= 8 {
		t.Fatalf("greeting pile counted per person: %v", got)
	}
}

func TestProfileTrendingEngagement_ZapsUnlockLikesDespiteGreetingPile(t *testing.T) {
	replies := make([]weightedReply, 8)
	for i := range replies {
		replies[i] = weightedReply{Pubkey: string(rune('a' + i)), Content: "GM", Weight: 1}
	}
	reactions := weightedPeople(10)
	zaps := []weightedReply{{Pubkey: "z", Weight: 1}}
	got := profileTrendingEngagement(replies, reactions, nil, zaps)
	want := roundProfileEngagement(greetingPileVote(8, 1) + 1 + reactionSignal(10, 1))
	if got != want {
		t.Fatalf("greeting pile plus a zap = %v, want %v", got, want)
	}
	floored := profileTrendingEngagement(replies, reactions, nil, nil)
	if got <= floored {
		t.Fatalf("zap should open likes the greeting pile does not: with=%v without=%v", got, floored)
	}
}

func TestProfileTrendingEngagement_ZapDoesNotUnlockReactionFarm(t *testing.T) {
	zaps := []weightedReply{{Pubkey: "z", Weight: 1}}
	reactions := weightedPeople(100)
	got := profileTrendingEngagement(nil, reactions, nil, zaps)
	want := roundProfileEngagement(1 + reactionSignal(100, 1))
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
	// The greeting pile is sqrt(2). The pile member's repost is omitted.
	// The other repost has no allowance, so it counts at 0.20.
	want := roundProfileEngagement(greetingPileVote(2, 1) + repostReachSignal(1, 0))
	if got != want {
		t.Fatalf("pile repost = %v, want %v", got, want)
	}
}

func TestProfileTrendingEngagement_RepostsCostTwoBeforeTheFloor(t *testing.T) {
	replies := []weightedReply{{Pubkey: "a", Content: "archival relays still matter for this conversation", Weight: 1}}
	reposts := weightedPeople(3)
	got := profileTrendingEngagement(replies, nil, reposts, nil)
	// One reply covers one repost at 2. The other two sit at the surplus floor.
	want := roundProfileEngagement(1 + repostReachSignal(3, 1))
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
	want := roundProfileEngagement(reactionSignal(12, 0))
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
	// The zapper is one person. The other greeting stays a pile vote and is
	// not a substantive reply. The zap opens four reaction points, so four
	// of the five likes count in full and the last stays at the floor.
	want := roundProfileEngagement(1 + 1 + reactionSignal(5, 1))
	if got != want {
		t.Fatalf("zapper in a greeting pile = %v, want %v", got, want)
	}
}

func TestProfileRisingEngagement_RepostAndLikeAreOnePerson(t *testing.T) {
	person := []weightedReply{{Pubkey: "a", Weight: 1}}
	got := profileRisingEngagement(nil, person, person, nil)
	want := roundProfileEngagement(repostReachSignal(1, 0))
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
	// Two copied sentences stay two pile votes. The zapper steps out and
	// counts once. The zap opens the four likes. The copied sentences do
	// not become substantive replies.
	want := roundProfileEngagement(2 + 1 + reactionSignal(4, 1))
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
